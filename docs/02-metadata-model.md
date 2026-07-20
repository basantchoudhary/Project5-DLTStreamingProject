# Metadata model — the 5 tables that drive everything

All metadata lives in a **UC Delta schema** (e.g. `ingest_meta.control`). The
`meta_loader` reads these into a `Metadata` object; the framework never hard-codes a
table name or path.

```
   source_master ─┬─ source_config          (WHERE data comes from)
                  │
   dataset_master ┴─ dataset_config          (WHICH tables + HOW to load each)
                                             (table_group_no, primary_keys, scd_type,
                                              load_type, paths, cluster keys)
   platform_config                           (global knobs: catalog, checkpoint root,
                                              Qlik header column names, liquid-auto flag)
```

---

## 1 · `source_master` — the source systems

| column | example | meaning |
|---|---|---|
| `source_id` | `ORA_SALES` | PK, logical source id |
| `source_name` | `Oracle Sales DB` | human name |
| `source_type` | `oracle` | source technology |
| `replication_tool` | `qlik_replicate` | how it lands |
| `landing_root` | `abfss://land@acct.dfs.core.windows.net/ora_sales` | ADLS root |
| `enabled` | `true` | toggle the whole source |

## 2 · `source_config` — per-source key/values

| column | example |
|---|---|
| `source_id` | `ORA_SALES` |
| `config_key` | `qlik_op_col` / `file_format` / `cdc_subpath` |
| `config_value` | `header__change_oper` / `parquet` / `cdc` |

> Overflow for anything source-specific (Qlik column names, subfolder conventions).

## 3 · `dataset_master` — the tables to ingest

| column | example | meaning |
|---|---|---|
| `dataset_id` | `ORA_SALES.ORDERS` | PK |
| `source_id` | `ORA_SALES` | FK → source_master |
| `source_schema` | `SALES` | Oracle schema |
| `source_table` | `ORDERS` | Oracle table |
| `enabled` | `true` | toggle this table |

## 4 · `dataset_config` — HOW to load each table  ⭐

The heart of the framework — one row per dataset.

| column | example | meaning |
|---|---|---|
| `dataset_id` | `ORA_SALES.ORDERS` | FK → dataset_master |
| `target_table` | `orders` | UC table name |
| **`table_group_no`** | `1` | which of the 10 DLT pipelines owns it |
| **`primary_keys`** | `order_id` | CSV; the `apply_changes` keys |
| `sequence_by` | `header__change_seq` | CDC ordering column |
| `scd_type` | `1` / `2` / `both` | which target schema(s) to write |
| `load_type` | `full` / `incr` | which Auto Loader path |
| `full_path` | `.../ORDERS/full` | full-load landing path |
| `incr_path` | `.../ORDERS/cdc` | CDC landing path |
| `cluster_by` | `` (blank = AUTO) | liquid clustering keys; blank ⇒ CLUSTER BY AUTO |

## 5 · `platform_config` — global knobs

| config_key | example | meaning |
|---|---|---|
| `meta_catalog` / `meta_schema` | `ingest_meta` / `control` | where metadata lives |
| `target_catalog` | `bronze` | UC catalog for outputs |
| `scd1_schema` / `scd2_schema` | `bronze_scd1` / `bronze_scd2` | output schemas |
| `checkpoint_root` | `abfss://.../_checkpoints` | Auto Loader/DLT state |
| `schema_root` | `abfss://.../_schemas` | Auto Loader schema inference |
| `qlik_op_col` | `header__change_oper` | default CDC operation column |
| `qlik_delete_code` | `D` | value meaning "delete" |
| `liquid_cluster_auto` | `true` | default CLUSTER BY AUTO on |

---

## 6 · How the loader turns rows into a `Metadata` object

```
   spark tables ──meta_loader.load_metadata()──►  Metadata(
       datasets = { "ORA_SALES.ORDERS": Dataset(table_group_no=1, primary_keys=[order_id],
                                                 scd_type='both', load_type='incr', ...), ... },
       config   = { "target_catalog":"bronze", "qlik_op_col":"header__change_oper", ... },
       sources  = { "ORA_SALES": {master, config} },
   )

   meta.datasets_for_group(1)  → the ~20 Dataset objects that pipeline 1 will build.
```

---

## 7 · Onboarding a new table = 2 inserts

```
   INSERT INTO dataset_master (dataset_id, source_id, source_schema, source_table, enabled)
     VALUES ('ORA_SALES.RETURNS','ORA_SALES','SALES','RETURNS', true);

   INSERT INTO dataset_config (dataset_id, target_table, table_group_no, primary_keys,
                               sequence_by, scd_type, load_type, full_path, incr_path)
     VALUES ('ORA_SALES.RETURNS','returns', 3, 'return_id', 'header__change_seq',
             'both','incr', '.../RETURNS/full', '.../RETURNS/cdc');
   -- next run of pipeline group 3 picks it up. No code change.
```

*Next: [`../metadata/01_create_metadata_schema.sql`](../metadata/01_create_metadata_schema.sql).*
