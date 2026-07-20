-- =============================================================================
-- Run log for the ingestion framework. One row per (run, dataset, phase).
-- DLTMainController writes a REGISTERED row as it loops the group's datasets;
-- DLTSingleTableController writes phase rows (derive / reader / writer / done).
-- =============================================================================

CREATE TABLE IF NOT EXISTS ingest_meta.control.pipeline_log (
  run_id         STRING    COMMENT 'one id per pipeline update (uuid)',
  source_id      STRING    COMMENT 'source being ingested',
  table_group_no INT       COMMENT 'which DLT pipeline group',
  dataset_id     STRING    COMMENT 'dataset_id (e.g. ORA_SALES.ORDERS); NULL for group-level rows',
  target_table   STRING    COMMENT 'UC target table name',
  phase          STRING    COMMENT 'GROUP_START | REGISTERED | DERIVE | READER | PROCESSOR | WRITER | DONE | ERROR',
  status         STRING    COMMENT 'OK | FAILED | INFO',
  message        STRING    COMMENT 'free text / error message',
  event_ts       TIMESTAMP COMMENT 'when this row was written'
)
USING DELTA
COMMENT 'Registration/run log written by the DLT controllers at graph-build time';
