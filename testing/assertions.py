"""assertions — count check + SCD2 invariant rules for the testing framework.

Each rule returns (name, passed: bool, detail: str). Run them after the pipeline
has processed a scenario's landing data. SCD2 rules assume the standard DLT SCD2
columns __START_AT / __END_AT (rename via the `start_col` / `end_col` args if your
target uses different names).
"""


# ---- count check (mock: we know the expected number) -----------------------
def assert_active_count(spark, target_fqn, expected, is_deleted_col="is_deleted"):
    """Active (non-soft-deleted) row count matches the expected value."""
    df = spark.table(target_fqn)
    if is_deleted_col in df.columns:
        df = df.filter(f"NOT {is_deleted_col}")
    actual = df.count()
    return ("active_count", actual == expected,
            f"expected={expected} actual={actual}")


# ---- SCD2 invariants -------------------------------------------------------
def scd2_rules(spark, target_fqn, keys, start_col="__START_AT", end_col="__END_AT"):
    """Return a list of (name, passed, detail) for the core SCD2 invariants."""
    from pyspark.sql import functions as F

    df = spark.table(target_fqn)
    key_cols = [F.col(k) for k in keys]
    results = []

    # 1) exactly one open (current) version per key: __END_AT IS NULL
    open_per_key = (df.filter(F.col(end_col).isNull())
                      .groupBy(*key_cols).count())
    bad_open = open_per_key.filter("count <> 1").count()
    results.append(("scd2_one_current_per_key", bad_open == 0,
                    f"keys with != 1 open version: {bad_open}"))

    # 2) no overlapping version windows per key:
    #    ordered by start, each version's start >= previous end
    from pyspark.sql import Window
    w = Window.partitionBy(*key_cols).orderBy(F.col(start_col))
    with_prev_end = df.withColumn("_prev_end", F.lag(F.col(end_col)).over(w))
    overlaps = with_prev_end.filter(
        F.col("_prev_end").isNotNull() & (F.col(start_col) < F.col("_prev_end"))
    ).count()
    results.append(("scd2_no_overlap", overlaps == 0,
                    f"overlapping windows: {overlaps}"))

    # 3) contiguous history: each closed version's end == next version's start
    with_next_start = df.withColumn("_next_start", F.lead(F.col(start_col)).over(w))
    gaps = with_next_start.filter(
        F.col(end_col).isNotNull() & F.col("_next_start").isNotNull()
        & (F.col(end_col) != F.col("_next_start"))
    ).count()
    results.append(("scd2_contiguous_history", gaps == 0,
                    f"gaps between versions: {gaps}"))

    # 4) ordering: start < end for every closed version (sequence/date sanity)
    bad_order = df.filter(
        F.col(end_col).isNotNull() & (F.col(start_col) >= F.col(end_col))
    ).count()
    results.append(("scd2_start_before_end", bad_order == 0,
                    f"versions with start >= end: {bad_order}"))

    return results


def summarize(results):
    """Pretty one-line-per-rule summary; returns (all_passed, text)."""
    lines = []
    all_ok = True
    for name, passed, detail in results:
        all_ok = all_ok and passed
        lines.append(f"  [{'PASS' if passed else 'FAIL'}] {name} — {detail}")
    return all_ok, "\n".join(lines)
