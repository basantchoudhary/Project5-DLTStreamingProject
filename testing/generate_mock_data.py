"""generate_mock_data — write synthetic Qlik-style CDC parquet to ADLS (or DBFS).

Produces the two landing folders the pipeline expects — <table>__full and
<table>__ct — from a compact scenario spec, so tests know the exact expected
result. The sequence column is central (SCD2 versioning depends on it), so every
change carries an explicit, monotonic sequence.

A scenario is a list of change dicts:
    {"op": "I"|"U"|"D", "seq": <int>, "<pk>": ..., <other cols>...}

`op="I"` rows also seed the __full snapshot (latest-by-key) unless you pass an
explicit `full` list.
"""

OP_COL = "header__change_oper"
SEQ_COL = "header__change_seq"


def _latest_by_key(changes, keys):
    """Reduce a change list to the latest non-deleted row per key (for __full seed)."""
    latest = {}
    for c in sorted(changes, key=lambda r: r["seq"]):
        k = tuple(c[k] for k in keys)
        if c["op"] == "D":
            latest.pop(k, None)
        else:
            latest[k] = c
    return list(latest.values())


def _to_rows(changes, columns, with_header):
    """Turn scenario dicts into uniform rows (adds the Qlik op/seq header cols)."""
    rows = []
    for c in changes:
        row = {col: c.get(col) for col in columns}
        if with_header:
            row[OP_COL] = c["op"]
            row[SEQ_COL] = c["seq"]
        rows.append(row)
    return rows


def write_scenario(spark, landing_root, table, keys, changes,
                   business_columns, full=None, full_suffix="__full", ct_suffix="__ct"):
    """Write a scenario's __full and __ct parquet under landing_root/<table>...

    Args:
        landing_root:     base path (abfss://... or dbfs:/...).
        table:            landing table base name (e.g. ORDERS).
        keys:             primary key column names (list).
        changes:          list of change dicts for the ct feed.
        business_columns: the non-header columns to materialize.
        full:             explicit full-snapshot change list; default = latest-by-key(changes).
    Returns dict with the two paths and the expected active-row count.
    """
    root = landing_root.rstrip("/")
    full_path = f"{root}/{table}{full_suffix}"
    ct_path = f"{root}/{table}{ct_suffix}"

    full_changes = full if full is not None else _latest_by_key(changes, keys)

    # __full: snapshot rows, no header op column (full-load rows are upserts)
    full_rows = _to_rows(full_changes, business_columns, with_header=False)
    if full_rows:
        spark.createDataFrame(full_rows).write.mode("overwrite").parquet(full_path)

    # __ct: change rows with header op/seq columns
    ct_rows = _to_rows(changes, business_columns, with_header=True)
    if ct_rows:
        spark.createDataFrame(ct_rows).write.mode("overwrite").parquet(ct_path)

    expected_active = len(_latest_by_key(changes, keys))
    return {
        "full_path": full_path,
        "ct_path": ct_path,
        "expected_active_count": expected_active,
        "total_changes": len(changes),
    }
