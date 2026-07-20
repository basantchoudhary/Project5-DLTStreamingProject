-- =============================================================================
-- Observability tables: reconciliation and record-level latency reports.
-- Written by framework/recon.py and framework/latency_report.py.
-- =============================================================================

-- Recon: live Oracle count vs UC target count, one row per dataset per run.
CREATE TABLE IF NOT EXISTS ingest_meta.control.recon_report (
  dataset_id     STRING,
  source_id      STRING,
  table_group_no INT,
  source_schema  STRING,
  source_table   STRING,
  target_table   STRING,
  source_count   BIGINT   COMMENT 'live COUNT(*) from Oracle (JDBC)',
  target_count   BIGINT   COMMENT 'active row count in UC (excl. soft-deleted)',
  diff           BIGINT   COMMENT 'source_count - target_count (0 = reconciled)',
  status         STRING   COMMENT 'OK | MISMATCH | ERROR',
  message        STRING,
  event_ts       TIMESTAMP
)
USING DELTA
COMMENT 'Reconciliation: Oracle vs UC row counts';

-- Latency: record-level end-to-end latency summary (from Delta CDF), per dataset.
CREATE TABLE IF NOT EXISTS ingest_meta.control.latency_report (
  dataset_id   STRING,
  target_table STRING,
  p50_sec      DOUBLE COMMENT 'median oracle_commit -> uc_commit latency (s)',
  p95_sec      DOUBLE,
  max_sec      DOUBLE,
  record_count BIGINT,
  status       STRING COMMENT 'OK | ERROR',
  message      STRING,
  event_ts     TIMESTAMP
)
USING DELTA
COMMENT 'Record-level end-to-end latency (Oracle commit -> UC commit via CDF)';
