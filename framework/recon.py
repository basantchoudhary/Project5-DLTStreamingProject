"""recon — reconciliation report: live Oracle count vs UC target count.

Loops the datasets from meta_loader, gets a live COUNT(*) from Oracle (JDBC) and
a COUNT from the UC target, diffs them, and writes one row per dataset to
ops.recon_report. Counts are a point-in-time check — a streaming target can lag by
seconds, so a *persistent* non-zero diff is the real signal.

Run as a normal (non-DLT) job/notebook on a schedule.
"""

import datetime

from framework.meta_loader import (
    load_dataset_metadata,
    load_platform_config,
    load_source_metadata,
)

RECON_TABLE = "ingest_meta.control.recon_report"


def _oracle_count(spark, jdbc_url, jdbc_props, source_schema, source_table):
    """Live COUNT(*) from Oracle via JDBC (push the count down to the source)."""
    query = f"(SELECT COUNT(*) AS CNT FROM {source_schema}.{source_table}) t"
    df = spark.read.jdbc(url=jdbc_url, table=query, properties=jdbc_props)
    return int(df.collect()[0]["CNT"])


def _uc_active_count(spark, target_fqn, is_deleted_col):
    """COUNT of ACTIVE rows in the UC target (exclude soft-deleted if present)."""
    df = spark.table(target_fqn)
    if is_deleted_col in df.columns:
        df = df.filter(f"NOT {is_deleted_col}")
    return int(df.count())


def run_recon(spark, source, table_group_no="ALL", dataset_list="ALL", *,
              jdbc_url, jdbc_props, meta_catalog="ingest_meta", meta_schema="control"):
    """Reconcile Oracle vs UC for a source (optionally one group) and write a report.

    Args:
        jdbc_url:   Oracle JDBC url (e.g. jdbc:oracle:thin:@//host:1521/svc).
        jdbc_props: dict with user/password/driver for the JDBC read.
        table_group_no: a specific group, or "ALL" to reconcile every group.
    """
    platform = load_platform_config(spark, meta_catalog=meta_catalog, meta_schema=meta_schema)
    is_deleted_col = platform.get("is_deleted_col", "is_deleted")
    target_catalog = platform.get("target_catalog", "bronze")
    scd1_schema = platform.get("scd1_schema", "bronze_scd1")
    scd2_schema = platform.get("scd2_schema", "bronze_scd2")

    groups = ([table_group_no] if table_group_no != "ALL"
              else _distinct_groups(spark, source, meta_catalog, meta_schema))

    rows = []
    ts = datetime.datetime.now()
    for g in groups:
        datasets = load_dataset_metadata(spark, source, g, dataset_list,
                                         meta_catalog=meta_catalog, meta_schema=meta_schema)
        for dataset_id, d in datasets.items():
            scd_type = str(d.get("scd_type", "1")).lower()
            # reconcile against the "latest state" view: SCD1 table, else SCD2 current
            if scd_type in ("1", "both"):
                target = f"{target_catalog}.{scd1_schema}.{d['target_table']}"
            else:
                target = f"{target_catalog}.{scd2_schema}.{d['target_table']}"
            try:
                src = _oracle_count(spark, jdbc_url, jdbc_props,
                                    d["source_schema"], d["source_table"])
                tgt = _uc_active_count(spark, target, is_deleted_col)
                diff = src - tgt
                status = "OK" if diff == 0 else "MISMATCH"
                msg = None
            except Exception as e:
                src = tgt = diff = None
                status = "ERROR"
                msg = str(e)
            rows.append((dataset_id, source, int(g), d["source_schema"], d["source_table"],
                         target, src, tgt, diff, status, msg, ts))

    if rows:
        cols = ["dataset_id", "source_id", "table_group_no", "source_schema",
                "source_table", "target_table", "source_count", "target_count",
                "diff", "status", "message", "event_ts"]
        (spark.createDataFrame(rows, schema=cols)
             .write.format("delta").mode("append").saveAsTable(RECON_TABLE))
    return rows


def _distinct_groups(spark, source, meta_catalog, meta_schema):
    from pyspark.sql import functions as F
    df = (spark.table(f"{meta_catalog}.{meta_schema}.dataset_master")
          .filter((F.col("source_id") == source) & (F.col("enabled") == True))  # noqa: E712
          .select("table_group_no").distinct())
    return sorted(int(r["table_group_no"]) for r in df.collect())
