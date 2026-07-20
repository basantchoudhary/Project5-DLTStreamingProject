"""latency_report — record-level end-to-end latency (Oracle commit → UC commit).

For each record we know two commit timestamps:
  * oracle_commit_ts : from the Qlik CDC header (source change/commit time),
                       carried through as a column on the target.
  * uc_commit_ts     : from Delta Change Data Feed (_commit_timestamp) on the
                       target table — i.e. when Delta actually recorded the row.

latency = uc_commit_ts - oracle_commit_ts, per record. Percentile it (p50/p95)
and compare to the SLA. Requires delta.enableChangeDataFeed=true on the targets.

Run as a normal (non-DLT) job/notebook.
"""

import datetime

from framework.meta_loader import load_dataset_metadata, load_platform_config

LATENCY_TABLE = "ingest_meta.control.latency_report"


def run_latency_report(spark, source, table_group_no, dataset_list="ALL", *,
                       from_version=0, oracle_commit_col="header__change_ts",
                       meta_catalog="ingest_meta", meta_schema="control"):
    """Compute per-record latency for a group's targets from Delta CDF and summarize.

    Args:
        from_version:      Delta version to read CDF from (0 = full history; use the
                           last processed version for incremental runs).
        oracle_commit_col: column on the target holding the Oracle commit timestamp.
    """
    from pyspark.sql import functions as F

    platform = load_platform_config(spark, meta_catalog=meta_catalog, meta_schema=meta_schema)
    target_catalog = platform.get("target_catalog", "bronze")
    scd1_schema = platform.get("scd1_schema", "bronze_scd1")
    scd2_schema = platform.get("scd2_schema", "bronze_scd2")

    datasets = load_dataset_metadata(spark, source, table_group_no, dataset_list,
                                     meta_catalog=meta_catalog, meta_schema=meta_schema)
    ts = datetime.datetime.now()
    summary = []
    for dataset_id, d in datasets.items():
        scd_type = str(d.get("scd_type", "1")).lower()
        schema = scd1_schema if scd_type in ("1", "both") else scd2_schema
        target = f"{target_catalog}.{schema}.{d['target_table']}"
        try:
            # read the target's Change Data Feed: _commit_timestamp = UC commit time
            cdf = (spark.read.format("delta")
                   .option("readChangeFeed", "true")
                   .option("startingVersion", from_version)
                   .table(target))
            if oracle_commit_col not in cdf.columns:
                summary.append((dataset_id, target, None, None, None, 0,
                                "ERROR", f"missing {oracle_commit_col}", ts))
                continue
            lat = (cdf
                   .withColumn("latency_sec",
                               F.col("_commit_timestamp").cast("double")
                               - F.col(oracle_commit_col).cast("double"))
                   .filter("latency_sec IS NOT NULL"))
            stats = lat.agg(
                F.count("*").alias("n"),
                F.expr("percentile_approx(latency_sec, 0.5)").alias("p50"),
                F.expr("percentile_approx(latency_sec, 0.95)").alias("p95"),
                F.max("latency_sec").alias("max"),
            ).collect()[0]
            summary.append((dataset_id, target, float(stats["p50"] or 0),
                            float(stats["p95"] or 0), float(stats["max"] or 0),
                            int(stats["n"] or 0), "OK", None, ts))
        except Exception as e:
            summary.append((dataset_id, target, None, None, None, 0, "ERROR", str(e), ts))

    if summary:
        cols = ["dataset_id", "target_table", "p50_sec", "p95_sec", "max_sec",
                "record_count", "status", "message", "event_ts"]
        (spark.createDataFrame(summary, schema=cols)
             .write.format("delta").mode("append").saveAsTable(LATENCY_TABLE))
    return summary
