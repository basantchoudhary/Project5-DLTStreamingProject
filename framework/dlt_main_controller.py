"""DLTMainController — pipeline entry point for one (source, table_group_no).

The DLT pipeline invokes run() with the parameters it was configured with. This
controller:
  1. receives source / table_group_no / dataset_list from the pipeline
  2. loads platform + source metadata, and the group's datasets via meta_loader
  3. loops the datasets, logging a REGISTERED row per table
  4. calls DLTSingleTableController.build_single_table for each

Because ~10 tables share one pipeline (table_group_no), a failure while building
one table's graph is logged and re-raised only after being recorded, so the log
shows exactly which table broke the update.
"""

import uuid

from framework.meta_loader import (
    load_dataset_metadata,
    load_platform_config,
    load_source_metadata,
)
from framework.dlt_single_table_controller import build_single_table
from framework.logger import log_event


def run(spark, source, table_group_no, dataset_list="ALL", *,
        meta_catalog="ingest_meta", meta_schema="control"):
    """Build the DLT graph for every dataset in (source, table_group_no)."""
    run_id = str(uuid.uuid4())
    table_group_no = int(table_group_no)

    log_event(spark, run_id, source, table_group_no, "GROUP_START", "INFO",
              message=f"source={source} group={table_group_no} dataset_list={dataset_list}")

    # -- 2. metadata ----------------------------------------------------------
    platform = load_platform_config(spark, meta_catalog=meta_catalog, meta_schema=meta_schema)
    src = load_source_metadata(spark, source, meta_catalog=meta_catalog, meta_schema=meta_schema)
    datasets = load_dataset_metadata(
        spark, source, table_group_no, dataset_list,
        meta_catalog=meta_catalog, meta_schema=meta_schema,
    )

    if not datasets:
        log_event(spark, run_id, source, table_group_no, "GROUP_START", "INFO",
                  message="no enabled datasets matched — nothing to build")
        print(f"[main] no datasets for source={source} group={table_group_no}")
        return run_id

    print(f"[main] run={run_id} building {len(datasets)} table(s) for "
          f"source={source} group={table_group_no}")

    # -- 3/4. loop, log, delegate --------------------------------------------
    for dataset_id, dataset in datasets.items():
        log_event(spark, run_id, source, table_group_no, "REGISTERED", "OK",
                  dataset_id=dataset_id, target_table=dataset.get("target_table"),
                  message=f"scd_type={dataset.get('scd_type')} load_type={dataset.get('load_type')}")
        try:
            build_single_table(
                spark, dataset, src, platform,
                run_id=run_id, source_id=source, table_group_no=table_group_no,
            )
        except Exception as e:
            log_event(spark, run_id, source, table_group_no, "ERROR", "FAILED",
                      dataset_id=dataset_id, target_table=dataset.get("target_table"),
                      message=str(e))
            raise

    return run_id
