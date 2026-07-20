"""derived_config — buildDerivedConfigForThisTable.

Takes the raw metadata dicts (one dataset's merged master+config, plus the
source dict and the platform dict) and computes everything the reader / processor
/ writer need but that isn't stored literally in metadata:

  * ADLS full path  and  ct (change-table) path      <- derived from landing_root
  * Auto Loader schema/checkpoint locations per table
  * standardized CDC columns (op / seq / delete code) with per-table override
  * the SCD targets to write  (scd1 / scd2 / both) as fully-qualified names
  * whether to include the full-load seed flow (load_type)

Landing layout (Qlik Replicate):  <landing_root>/<table><full_suffix>
                                   <landing_root>/<table><ct_suffix>
   e.g.  .../ora_sales/ORDERS__full   and   .../ora_sales/ORDERS__ct
"""


def _get(d, key, default=None):
    """dict.get that treats blank strings as absent."""
    v = d.get(key) if d else None
    if v is None or (isinstance(v, str) and v.strip() == ""):
        return default
    return v


def build_derived_config(dataset, source, platform):
    """Return an enriched dict for one table. Inputs are plain dicts from meta_loader.

    Args:
        dataset:  one value from load_dataset_metadata() (master cols + config kv).
        source:   load_source_metadata() result for this dataset's source.
        platform: load_platform_config() result (global knobs).
    """
    dataset_id = dataset["dataset_id"]
    source_table = dataset["source_table"]
    target_table = dataset["target_table"]

    # --- landing paths (derived unless explicitly overridden in dataset_config) --
    landing_root = _get(source, "landing_root", "").rstrip("/")
    landing_base = _get(dataset, "landing_table_name", source_table)  # Qlik uses source table name
    full_suffix = _get(source, "full_suffix", "__full")
    ct_suffix = _get(source, "ct_suffix", "__ct")

    full_path = _get(dataset, "full_path", f"{landing_root}/{landing_base}{full_suffix}")
    ct_path = _get(dataset, "ct_path", f"{landing_root}/{landing_base}{ct_suffix}")

    file_format = _get(source, "file_format", "parquet")

    # --- Auto Loader state locations (one per table, split full vs ct) ----------
    checkpoint_root = _get(platform, "checkpoint_root", "").rstrip("/")
    schema_root = _get(platform, "schema_root", "").rstrip("/")
    schema_full = f"{schema_root}/{target_table}/full"
    schema_ct = f"{schema_root}/{target_table}/ct"

    # --- CDC columns: platform default, per-dataset override --------------------
    op_col = _get(dataset, "op_col", _get(platform, "qlik_op_col", "header__change_oper"))
    seq_col = _get(dataset, "sequence_by", _get(platform, "qlik_seq_col", "header__change_seq"))
    delete_code = _get(dataset, "delete_code", _get(platform, "qlik_delete_code", "D"))

    # --- SCD targets ------------------------------------------------------------
    scd_type = str(_get(dataset, "scd_type", "1")).lower()
    target_catalog = _get(platform, "target_catalog", "bronze")
    scd1_schema = _get(platform, "scd1_schema", "bronze_scd1")
    scd2_schema = _get(platform, "scd2_schema", "bronze_scd2")

    scd_targets = []  # list of (scd_int, fully_qualified_target_name)
    if scd_type in ("1", "both"):
        scd_targets.append((1, f"{target_catalog}.{scd1_schema}.{target_table}"))
    if scd_type in ("2", "both"):
        scd_targets.append((2, f"{target_catalog}.{scd2_schema}.{target_table}"))

    # --- keys / clustering / load lifecycle ------------------------------------
    primary_keys = [k.strip() for k in _get(dataset, "primary_keys", "").split(",") if k.strip()]
    cluster_by_raw = _get(dataset, "cluster_by", "")
    cluster_by = [c.strip() for c in cluster_by_raw.split(",") if c.strip()]  # empty => AUTO
    cluster_auto = str(_get(platform, "liquid_cluster_auto", "true")).lower() == "true"

    load_type = str(_get(dataset, "load_type", "incr")).lower()
    include_full_flow = load_type in ("full", "both")  # seed flow; ct flow is always on

    return {
        "dataset_id": dataset_id,
        "source_table": source_table,
        "target_table": target_table,
        "raw_table": f"{target_table}_raw",
        "processed_view": f"{target_table}_processed",
        # paths
        "full_path": full_path,
        "ct_path": ct_path,
        "file_format": file_format,
        "schema_full": schema_full,
        "schema_ct": schema_ct,
        # cdc columns
        "op_col": op_col,
        "seq_col": seq_col,
        "delete_code": delete_code,
        # targets
        "scd_targets": scd_targets,
        "primary_keys": primary_keys,
        "cluster_by": cluster_by,
        "cluster_auto": cluster_auto,
        # lifecycle
        "load_type": load_type,
        "include_full_flow": include_full_flow,
    }
