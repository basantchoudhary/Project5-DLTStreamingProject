"""processor — standardizes raw Qlik records into a common CDC shape.

Adds two framework columns the writer relies on:
  _op   'I' | 'U' | 'D'   (full-load snapshot rows -> 'U' upsert; ct rows mapped
                           from the Qlik operation column)
  _seq  ordering value    (from sequence_by; falls back to ingest time)

Everything else on the row is passed through untouched so apply_changes can
merge the business columns.
"""


def standardize_cdc(df, op_col, seq_col, delete_code,
                    delete_mode="hard", is_deleted_col="is_deleted"):
    """Return df with normalized _op / _seq (and, in soft mode, is_deleted) columns.

    Args:
        df:            streaming DataFrame from the raw table (full + ct unioned).
        op_col:        Qlik change-operation column (may be absent on full-load rows).
        seq_col:       ordering column (may be absent on full-load rows).
        delete_code:   value of op_col that means "delete" (e.g. 'D').
        delete_mode:   'hard' (physical delete downstream) or 'soft' (is_deleted flag).
        is_deleted_col: name of the soft-delete flag column.
    """
    from pyspark.sql import functions as F

    cols = set(df.columns)

    # --- _op: map the Qlik operation; full-load rows (no op col) are upserts ----
    if op_col in cols:
        op = F.upper(F.trim(F.col(op_col).cast("string")))
        _op = (
            F.when(op == F.lit(str(delete_code).upper()), F.lit("D"))
            .when(op.isin("I", "INSERT"), F.lit("I"))
            .when(op.isin("U", "UPDATE"), F.lit("U"))
            .otherwise(F.lit("U"))  # unknown/blank -> treat as upsert
        )
    else:
        _op = F.lit("U")
    df = df.withColumn("_op", _op)

    # --- soft delete: turn a delete into an upsert that flags the row -----------
    # is_deleted = true when the event is a delete; then remap _op 'D' -> 'U' so
    # apply_changes UPSERTS the flagged row instead of physically removing it.
    if delete_mode == "soft":
        df = df.withColumn(is_deleted_col, F.col("_op") == F.lit("D"))
        df = df.withColumn("_op", F.when(F.col("_op") == F.lit("D"), F.lit("U"))
                                   .otherwise(F.col("_op")))

    # --- _seq: ordering for apply_changes; fall back to ingest time -------------
    if seq_col in cols:
        df = df.withColumn("_seq", F.col(seq_col))
    else:
        df = df.withColumn("_seq", F.col("_ingest_ts"))

    return df
