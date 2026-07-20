-- =============================================================================
-- Metadata schema for the metadata-driven DLT streaming framework.
--
-- Five tables drive everything (see docs/02-metadata-model.md):
--   source_master     WHERE data comes from            (fixed columns)
--   source_config     per-source overflow              (key / value)
--   dataset_master    WHICH tables to ingest           (fixed columns)
--   dataset_config    HOW to load each table           (key / value)  <- the heart
--   platform_config   global knobs                     (key / value)
--
-- The metadata location itself is bootstrapped (meta_catalog / meta_schema are
-- passed to meta_loader); everything else the framework reads from these tables.
-- Idempotent: safe to re-run.
-- =============================================================================

CREATE CATALOG IF NOT EXISTS ingest_meta;
CREATE SCHEMA  IF NOT EXISTS ingest_meta.control;

-- 1 -- source_master ---------------------------------------------------------
CREATE TABLE IF NOT EXISTS ingest_meta.control.source_master (
  source_id        STRING  NOT NULL COMMENT 'PK — logical source id, e.g. ORA_SALES',
  source_name      STRING           COMMENT 'human-readable name',
  source_type      STRING           COMMENT 'source technology (oracle, mysql, ...)',
  replication_tool STRING           COMMENT 'how it lands (qlik_replicate, ...)',
  landing_root     STRING           COMMENT 'ADLS root for this source',
  enabled          BOOLEAN          COMMENT 'toggle the whole source on/off'
)
USING DELTA
COMMENT 'Source systems feeding the ingestion framework';

-- 2 -- source_config (key/value) ---------------------------------------------
CREATE TABLE IF NOT EXISTS ingest_meta.control.source_config (
  source_id    STRING NOT NULL COMMENT 'FK -> source_master.source_id',
  config_key   STRING NOT NULL COMMENT 'e.g. qlik_op_col / file_format / cdc_subpath',
  config_value STRING          COMMENT 'the value for config_key'
)
USING DELTA
COMMENT 'Per-source key/value overflow (Qlik column names, subfolder conventions, ...)';

-- 3 -- dataset_master --------------------------------------------------------
CREATE TABLE IF NOT EXISTS ingest_meta.control.dataset_master (
  dataset_id     STRING  NOT NULL COMMENT 'PK — e.g. ORA_SALES.ORDERS',
  source_id      STRING  NOT NULL COMMENT 'FK -> source_master.source_id',
  source_schema  STRING           COMMENT 'source schema (e.g. SALES)',
  source_table   STRING           COMMENT 'source table (e.g. ORDERS)',
  target_table   STRING           COMMENT 'UC target table name (e.g. orders)',
  table_group_no INT              COMMENT 'which DLT pipeline (1..N) owns this table — filter dimension',
  enabled        BOOLEAN          COMMENT 'toggle this table on/off'
)
USING DELTA
COMMENT 'Tables to ingest; the loader filters by source_id + table_group_no + enabled';

-- 4 -- dataset_config (key/value) — the heart of the framework ----------------
CREATE TABLE IF NOT EXISTS ingest_meta.control.dataset_config (
  dataset_id   STRING NOT NULL COMMENT 'FK -> dataset_master.dataset_id',
  config_key   STRING NOT NULL COMMENT 'primary_keys | sequence_by | scd_type | load_type | full_path | incr_path | cluster_by | ...',
  config_value STRING          COMMENT 'the value for config_key'
)
USING DELTA
COMMENT 'HOW to load each table — one key/value row per setting';

-- 5 -- platform_config (key/value) -------------------------------------------
CREATE TABLE IF NOT EXISTS ingest_meta.control.platform_config (
  config_key   STRING NOT NULL COMMENT 'meta_catalog | target_catalog | scd1_schema | checkpoint_root | qlik_op_col | ...',
  config_value STRING          COMMENT 'the value for config_key'
)
USING DELTA
COMMENT 'Global knobs shared by every pipeline';
