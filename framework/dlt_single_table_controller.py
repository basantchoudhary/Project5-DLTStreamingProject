"""DLTSingleTableController — build the DLT graph for ONE table.

Given one dataset's metadata, it:
  1. buildDerivedConfigForThisTable  (paths, cdc cols, scd targets, merge mode)
  2. branches on merge_mode:
       - 'cdc'    (default): <table>_raw ← full/ct append_flow → processed view
                   (standardize _op/_seq) → apply_changes into SCD1/SCD2.
       - 'append' (keyless / true duplicates the business wants kept): full/ct
                   append_flow straight into the final append target — no dedup,
                   no apply_changes.
  3. logs each phase to pipeline_log.

All DLT objects are defined *inside* build_single_table so each table's closures
capture that call's own `derived` (no late-binding across the controller loop).
"""

from framework.derived_config import build_derived_config
from framework.reader import build_autoloader_stream
from framework.processor import standardize_cdc
from framework.writer import write_scd_targets
from framework.logger import log_event


def build_single_table(spark, dataset, source, platform, *, run_id, source_id, table_group_no):
    """Wire the DLT graph for one table (CDC merge, or plain append)."""
    dataset_id = dataset["dataset_id"]
    target_table = dataset["target_table"]

    # -- 1. derived config -----------------------------------------------------
    derived = build_derived_config(dataset, source, platform)
    log_event(spark, run_id, source_id, table_group_no, "DERIVE", "OK",
              dataset_id=dataset_id, target_table=target_table,
              message=f"mode={derived['merge_mode']} full={derived['full_path']} "
                      f"ct={derived['ct_path']} scd={[s for s, _ in derived['scd_targets']]}")

    ctx = dict(spark=spark, derived=derived, run_id=run_id,
               source_id=source_id, table_group_no=table_group_no,
               dataset_id=dataset_id, target_table=target_table)

    if derived["merge_mode"] == "append":
        _build_append(**ctx)
    else:
        _build_cdc(**ctx)

    log_event(spark, run_id, source_id, table_group_no, "DONE", "OK",
              dataset_id=dataset_id, target_table=target_table)
    return derived


def _cluster_kwargs(derived, name):
    """Streaming-table create kwargs with liquid clustering (explicit keys or AUTO)."""
    kw = {"name": name}
    if derived["cluster_by"]:
        kw["cluster_by"] = derived["cluster_by"]
    elif derived["cluster_auto"]:
        kw["cluster_by_auto"] = True
    return kw


def _full_ct_flows(spark, derived, target_name):
    """Attach the optional full (seed) flow and the ct flow, both appending target_name."""
    import dlt

    if derived["include_full_flow"]:
        @dlt.append_flow(target=target_name, name=f"{target_name.split('.')[-1]}_full_flow")
        def _full_flow():
            return build_autoloader_stream(
                spark, derived["full_path"], derived["schema_full"],
                file_format=derived["file_format"], source_kind="full",
            )

    @dlt.append_flow(target=target_name, name=f"{target_name.split('.')[-1]}_ct_flow")
    def _ct_flow():
        return build_autoloader_stream(
            spark, derived["ct_path"], derived["schema_ct"],
            file_format=derived["file_format"], source_kind="ct",
        )


def _build_cdc(spark, derived, run_id, source_id, table_group_no, dataset_id, target_table):
    """CDC path: raw table + flows -> standardized view -> apply_changes -> SCD1/2."""
    import dlt

    raw_table = derived["raw_table"]
    processed_view = derived["processed_view"]

    # raw streaming table both flows append into
    dlt.create_streaming_table(name=raw_table)
    _full_ct_flows(spark, derived, raw_table)
    log_event(spark, run_id, source_id, table_group_no, "READER", "OK",
              dataset_id=dataset_id, target_table=target_table,
              message=f"append_flows: {'full+ct' if derived['include_full_flow'] else 'ct'}")

    # processor view: standardize the CDC shape
    @dlt.view(name=processed_view)
    def _processed():
        raw = dlt.read_stream(raw_table)
        return standardize_cdc(
            raw, derived["op_col"], derived["seq_col"], derived["delete_code"],
            delete_mode=derived["delete_mode"], is_deleted_col=derived["is_deleted_col"],
        )

    # writer: apply_changes into SCD1/SCD2
    write_scd_targets(derived, processed_view)
    log_event(spark, run_id, source_id, table_group_no, "WRITER", "OK",
              dataset_id=dataset_id, target_table=target_table,
              message=f"targets={[t for _, t in derived['scd_targets']]}")


def _build_append(spark, derived, run_id, source_id, table_group_no, dataset_id, target_table):
    """Append path: full/ct append_flow straight into the final table. No dedup."""
    import dlt

    append_target = derived["append_target"]

    # the final append-only streaming table; every row is kept (duplicates included)
    dlt.create_streaming_table(**_cluster_kwargs(derived, append_target))
    _full_ct_flows(spark, derived, append_target)

    log_event(spark, run_id, source_id, table_group_no, "WRITER", "OK",
              dataset_id=dataset_id, target_table=target_table,
              message=f"append-only target={append_target} (no dedup)")
