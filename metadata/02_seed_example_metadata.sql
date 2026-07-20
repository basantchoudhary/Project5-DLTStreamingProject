-- =============================================================================
-- Example metadata so meta_loader has something to read.
-- One source (ORA_SALES), two datasets in table_group_no = 1 (ORDERS, CUSTOMERS).
-- Onboarding a new table later = two INSERTs (dataset_master + dataset_config).
-- Idempotent-ish for a demo: truncate then insert.
-- =============================================================================

-- ---- platform_config (global) ----------------------------------------------
DELETE FROM ingest_meta.control.platform_config;
INSERT INTO ingest_meta.control.platform_config (config_key, config_value) VALUES
  ('meta_catalog',        'ingest_meta'),
  ('meta_schema',         'control'),
  ('target_catalog',      'bronze'),
  ('scd1_schema',         'bronze_scd1'),
  ('scd2_schema',         'bronze_scd2'),
  ('append_schema',       'bronze_append'),   -- target schema for merge_mode='append' tables
  ('checkpoint_root',     'abfss://ingest@acct.dfs.core.windows.net/_checkpoints'),
  ('schema_root',         'abfss://ingest@acct.dfs.core.windows.net/_schemas'),
  ('qlik_op_col',         'header__change_oper'),
  ('qlik_seq_col',        'header__change_seq'),
  ('qlik_delete_code',    'D'),
  ('liquid_cluster_auto', 'true'),
  ('delete_mode',         'hard'),      -- default delete handling; 'soft' = is_deleted flag
  ('is_deleted_col',      'is_deleted');

-- ---- source_master + source_config -----------------------------------------
DELETE FROM ingest_meta.control.source_master WHERE source_id = 'ORA_SALES';
INSERT INTO ingest_meta.control.source_master
  (source_id, source_name, source_type, replication_tool, landing_root, enabled) VALUES
  ('ORA_SALES', 'Oracle Sales DB', 'oracle', 'qlik_replicate',
   'abfss://land@acct.dfs.core.windows.net/ora_sales', true);

DELETE FROM ingest_meta.control.source_config WHERE source_id = 'ORA_SALES';
INSERT INTO ingest_meta.control.source_config (source_id, config_key, config_value) VALUES
  ('ORA_SALES', 'file_format',  'parquet'),
  ('ORA_SALES', 'full_suffix',  '__full'),   -- landing layout: <table>__full
  ('ORA_SALES', 'ct_suffix',    '__ct');     -- landing layout: <table>__ct

-- ---- dataset_master --------------------------------------------------------
DELETE FROM ingest_meta.control.dataset_master WHERE source_id = 'ORA_SALES';
INSERT INTO ingest_meta.control.dataset_master
  (dataset_id, source_id, source_schema, source_table, target_table, table_group_no, enabled) VALUES
  ('ORA_SALES.ORDERS',    'ORA_SALES', 'SALES', 'ORDERS',    'orders',    1, true),
  ('ORA_SALES.CUSTOMERS', 'ORA_SALES', 'SALES', 'CUSTOMERS', 'customers', 1, true);

-- ---- dataset_config (HOW to load each) -------------------------------------
DELETE FROM ingest_meta.control.dataset_config
  WHERE dataset_id IN ('ORA_SALES.ORDERS', 'ORA_SALES.CUSTOMERS');
-- paths are DERIVED by build_derived_config (landing_root + <table>__full/__ct),
-- so only the "how to load" knobs live here.
INSERT INTO ingest_meta.control.dataset_config (dataset_id, config_key, config_value) VALUES
  -- ORDERS: SCD1+SCD2, ongoing CDC only
  ('ORA_SALES.ORDERS', 'primary_keys', 'order_id'),
  ('ORA_SALES.ORDERS', 'sequence_by',  'header__change_seq'),
  ('ORA_SALES.ORDERS', 'scd_type',     'both'),
  ('ORA_SALES.ORDERS', 'load_type',    'incr'),
  ('ORA_SALES.ORDERS', 'cluster_by',   ''),
  -- CUSTOMERS: SCD2, seed with full then ongoing CDC
  ('ORA_SALES.CUSTOMERS', 'primary_keys', 'customer_id'),
  ('ORA_SALES.CUSTOMERS', 'sequence_by',  'header__change_seq'),
  ('ORA_SALES.CUSTOMERS', 'scd_type',     '2'),
  ('ORA_SALES.CUSTOMERS', 'load_type',    'full'),
  ('ORA_SALES.CUSTOMERS', 'cluster_by',   'customer_id');
