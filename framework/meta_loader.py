"""
meta_loader — reads the 5 metadata tables (UC Delta schema) into plain Python dicts.

The framework never hard-codes a table name, path, or key list; it asks the
metadata database. Everything downstream (Controller -> SingleTableController ->
Processor -> Writer) is built from what these functions return.

Core call (the one the pipeline uses):

    from framework.meta_loader import load_dataset_metadata

    datasets = load_dataset_metadata(
        spark,
        source="ORA_SALES",
        table_group_no=1,
        dataset_list="ALL",          # or ["ORA_SALES.ORDERS", ...]
    )
    # -> {
    #      "ORA_SALES.ORDERS": {
    #          "dataset_id": "ORA_SALES.ORDERS", "source_id": "ORA_SALES",
    #          "source_schema": "SALES", "source_table": "ORDERS",
    #          "target_table": "orders", "table_group_no": 1,
    #          "primary_keys": "order_id", "sequence_by": "header__change_seq",
    #          "scd_type": "both", "load_type": "incr",
    #          "full_path": "...", "incr_path": "...", "cluster_by": "",
    #      },
    #      "ORA_SALES.CUSTOMERS": { ... },
    #    }

Each dataset's dict = its dataset_master columns overlaid with all of its
dataset_config key/values. Master columns are the fixed dimensions (source,
table_group_no, target_table); config key/values are the per-table "how to load"
settings — merged into one flat dict per dataset so callers read one place.
"""

from collections import defaultdict

# Bootstrap defaults — the metadata schema location. Override per call if needed.
DEFAULT_META_CATALOG = "ingest_meta"
DEFAULT_META_SCHEMA = "control"


def _fq(meta_catalog, meta_schema):
    """Fully-qualified metadata schema prefix, e.g. `ingest_meta.control`."""
    return f"{meta_catalog}.{meta_schema}"


def load_dataset_metadata(
    spark,
    source,
    table_group_no,
    dataset_list="ALL",
    *,
    meta_catalog=DEFAULT_META_CATALOG,
    meta_schema=DEFAULT_META_SCHEMA,
):
    """Return {dataset_id: {config_key: config_value, ...}} for one source + group.

    Args:
        spark:          active SparkSession.
        source:         source_id to load (e.g. "ORA_SALES").
        table_group_no: which DLT pipeline group owns the tables (e.g. 1).
        dataset_list:   "ALL" (default) or a list/str of dataset_ids to restrict to.
        meta_catalog / meta_schema: where the metadata tables live.

    Only enabled datasets are returned. Empty dict if nothing matches.
    """
    from pyspark.sql import functions as F

    fq = _fq(meta_catalog, meta_schema)

    # --- dataset_master: filter by source + group + enabled (+ optional list) ---
    master = spark.table(f"{fq}.dataset_master").filter(
        (F.col("source_id") == source)
        & (F.col("table_group_no") == table_group_no)
        & (F.col("enabled") == True)  # noqa: E712 (Spark Column, not Python bool)
    )

    if dataset_list != "ALL":
        wanted = (
            list(dataset_list)
            if isinstance(dataset_list, (list, tuple, set))
            else [dataset_list]
        )
        master = master.filter(F.col("dataset_id").isin(wanted))

    master_rows = master.collect()
    if not master_rows:
        return {}

    dataset_ids = [r["dataset_id"] for r in master_rows]

    # --- dataset_config: pull every key/value for those datasets in one shot ----
    cfg_rows = (
        spark.table(f"{fq}.dataset_config")
        .filter(F.col("dataset_id").isin(dataset_ids))
        .collect()
    )
    kv_by_dataset = defaultdict(dict)
    for r in cfg_rows:
        kv_by_dataset[r["dataset_id"]][r["config_key"]] = r["config_value"]

    # --- merge master columns + config key/values into one flat dict per dataset -
    result = {}
    for r in master_rows:
        did = r["dataset_id"]
        merged = {
            "dataset_id": did,
            "source_id": r["source_id"],
            "source_schema": r["source_schema"],
            "source_table": r["source_table"],
            "target_table": r["target_table"],
            "table_group_no": r["table_group_no"],
        }
        merged.update(kv_by_dataset.get(did, {}))  # config overlays master
        result[did] = merged

    return result


def load_platform_config(
    spark,
    *,
    meta_catalog=DEFAULT_META_CATALOG,
    meta_schema=DEFAULT_META_SCHEMA,
):
    """Return platform_config as a flat {config_key: config_value} dict."""
    fq = _fq(meta_catalog, meta_schema)
    rows = spark.table(f"{fq}.platform_config").collect()
    return {r["config_key"]: r["config_value"] for r in rows}


def load_source_metadata(
    spark,
    source,
    *,
    meta_catalog=DEFAULT_META_CATALOG,
    meta_schema=DEFAULT_META_SCHEMA,
):
    """Return one source's master columns merged with its source_config key/values.

    {source_id, source_name, source_type, replication_tool, landing_root,
     enabled, <config_key>: <config_value>, ...}  or {} if not found.
    """
    from pyspark.sql import functions as F

    fq = _fq(meta_catalog, meta_schema)

    master_rows = (
        spark.table(f"{fq}.source_master")
        .filter(F.col("source_id") == source)
        .collect()
    )
    if not master_rows:
        return {}
    m = master_rows[0]

    cfg_rows = (
        spark.table(f"{fq}.source_config")
        .filter(F.col("source_id") == source)
        .collect()
    )

    merged = {
        "source_id": m["source_id"],
        "source_name": m["source_name"],
        "source_type": m["source_type"],
        "replication_tool": m["replication_tool"],
        "landing_root": m["landing_root"],
        "enabled": m["enabled"],
    }
    for r in cfg_rows:
        merged[r["config_key"]] = r["config_value"]
    return merged
