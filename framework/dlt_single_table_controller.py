"""DLTSingleTableController — build the DLT graph for ONE table.

Given one dataset's metadata, it:
  1. buildDerivedConfigForThisTable  (paths, cdc cols, scd targets)
  2. creates the <table>_raw streaming table
  3. reader + append_flow: full-load flow (optional) and ct flow both APPEND
     into <table>_raw via Auto Loader
  4. processor: a view over <table>_raw that standardizes _op / _seq
  5. writer: apply_changes from the processed view into SCD1/SCD2 targets
  6. logs each phase to pipeline_log

All DLT objects are defined *inside* build_single_table so each table's closures
capture that call's own `derived` (no late-binding across the controller loop).
"""

from framework.derived_config import build_derived_config
from framework.reader import build_autoloader_stream
from framework.processor import standardize_cdc
from framework.writer import write_scd_targets
from framework.logger import log_event


def build_single_table(spark, dataset, source, platform, *, run_id, source_id, table_group_no):
    """Wire the reader -> append_flow -> processor -> writer graph for one table."""
    import dlt

    dataset_id = dataset["dataset_id"]
    target_table = dataset["target_table"]

    # -- 1. derived config -----------------------------------------------------
    derived = build_derived_config(dataset, source, platform)
    log_event(spark, run_id, source_id, table_group_no, "DERIVE", "OK",
              dataset_id=dataset_id, target_table=target_table,
              message=f"full={derived['full_path']} ct={derived['ct_path']} "
                      f"scd={[s for s, _ in derived['scd_targets']]}")

    raw_table = derived["raw_table"]
    processed_view = derived["processed_view"]

    # -- 2. the raw streaming table both flows append into ---------------------
    dlt.create_streaming_table(name=raw_table)

    # -- 3. reader + append_flow (full seed optional, ct always) ---------------
    if derived["include_full_flow"]:
        @dlt.append_flow(target=raw_table, name=f"{raw_table}_full_flow")
        def _full_flow():
            return build_autoloader_stream(
                spark, derived["full_path"], derived["schema_full"],
                file_format=derived["file_format"], source_kind="full",
            )

    @dlt.append_flow(target=raw_table, name=f"{raw_table}_ct_flow")
    def _ct_flow():
        return build_autoloader_stream(
            spark, derived["ct_path"], derived["schema_ct"],
            file_format=derived["file_format"], source_kind="ct",
        )

    log_event(spark, run_id, source_id, table_group_no, "READER", "OK",
              dataset_id=dataset_id, target_table=target_table,
              message=f"append_flows: {'full+ct' if derived['include_full_flow'] else 'ct'}")

    # -- 4. processor view: standardize the CDC shape --------------------------
    @dlt.view(name=processed_view)
    def _processed():
        raw = dlt.read_stream(raw_table)
        return standardize_cdc(raw, derived["op_col"], derived["seq_col"], derived["delete_code"])

    # -- 5. writer: apply_changes into SCD1/SCD2 -------------------------------
    write_scd_targets(derived, processed_view)
    log_event(spark, run_id, source_id, table_group_no, "WRITER", "OK",
              dataset_id=dataset_id, target_table=target_table,
              message=f"targets={[t for _, t in derived['scd_targets']]}")

    log_event(spark, run_id, source_id, table_group_no, "DONE", "OK",
              dataset_id=dataset_id, target_table=target_table)
    return derived
