"""logger — append rows to ingest_meta.control.pipeline_log.

Called by the controllers at graph-build time (driver-side), so this is a low
-frequency structured log of *which datasets a pipeline update registered and
how far each got* — not a per-record log. Never raises: a logging failure must
not break the pipeline graph.
"""

import datetime

DEFAULT_LOG_TABLE = "ingest_meta.control.pipeline_log"

# Column order must match metadata/03_create_log_table.sql.
_COLUMNS = [
    "run_id",
    "source_id",
    "table_group_no",
    "dataset_id",
    "target_table",
    "phase",
    "status",
    "message",
    "event_ts",
]


def log_event(
    spark,
    run_id,
    source_id,
    table_group_no,
    phase,
    status="INFO",
    dataset_id=None,
    target_table=None,
    message=None,
    log_table=DEFAULT_LOG_TABLE,
):
    """Append one row to the pipeline_log. Swallows its own errors."""
    try:
        row = (
            run_id,
            source_id,
            int(table_group_no) if table_group_no is not None else None,
            dataset_id,
            target_table,
            phase,
            status,
            message,
            datetime.datetime.now(),
        )
        (
            spark.createDataFrame([row], schema=_COLUMNS)
            .write.format("delta")
            .mode("append")
            .saveAsTable(log_table)
        )
    except Exception as e:  # logging must never break the pipeline
        print(f"[logger] WARN could not write log row ({phase}/{status}): {e}")
