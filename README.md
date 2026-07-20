# Project 5 · Metadata-Driven DLT Streaming (Oracle CDC → Lakehouse)

A **metadata-driven** streaming ingestion framework: Oracle change data, captured
by **Qlik Replicate** from the redo logs, landed as Parquet in **ADLS Gen2**,
ingested by **Auto Loader**, and merged into **Unity Catalog SCD1/SCD2 streaming
tables** via **DLT `apply_changes`** — onboarding a table is a config row, not code.

> Part of a 7-project data-engineering training portfolio. Companion:
> [Project 1 · batch medallion](../ecomsimpledataplatformusingdatabricks/).

---

## Architecture (one line per hop)

```
   SomeApp → Oracle (redo logs) → Qlik Replicate (Azure VM, snappy parquet)
        → ADLS  source/<table>__full + source/<table>__ct
        → 20 DLT pipelines (~10 tables each)  → bronze_scd1.<t> / bronze_scd2.<t>
```

Diagram-first design pages (open in a browser): **[design/](design/index.html)** —
[high-level flow](design/high-level-flow.html) ·
[low-level design](design/low-level-design.html) ·
[metadata tables](design/metadata-tables-design.html).

---

## Repository map

| Path | What's in it |
|---|---|
| [`metadata/`](metadata/) | The 5 control tables + log/ops tables (DDL + seed). |
| [`framework/`](framework/) | The engine — see the component table below. |
| [`pipelines/dlt_entry.py`](pipelines/dlt_entry.py) | The single notebook all 20 pipelines point at. |
| [`deploy/create_dlt_pipelines.py`](deploy/create_dlt_pipelines.py) | Provision the 20 pipelines (Databricks SDK, metadata-driven). |
| [`design/`](design/index.html) | HTML design pages (flow / LLD / metadata). |
| [`docs/`](docs/) | Architecture, metadata model, Silver/Gold, real-time to Power BI. |
| [`ComponentWiseConceptToLearn/`](ComponentWiseConceptToLearn/) | 19 concept explainers (redo log → testing). |
| [`Project Challenges/`](Project%20Challenges/) | 12 real-world war stories (problem → cause → fix). |

## Framework components

| Component | File | Role |
|---|---|---|
| meta_loader | `framework/meta_loader.py` | Reads the 5 metadata tables → dicts. |
| derived_config | `framework/derived_config.py` | `buildDerivedConfigForThisTable` — paths, cdc cols, scd targets, delete mode. |
| reader | `framework/reader.py` | Auto Loader (`cloudFiles`) read streams (full + ct). |
| processor | `framework/processor.py` | Standardize Qlik op → `_op` (I/U/D), `_seq`; soft-delete flag. |
| writer | `framework/writer.py` | `create_streaming_table` + `apply_changes` (SCD1/2, hard/soft delete). |
| SingleTableController | `framework/dlt_single_table_controller.py` | Per-table graph: raw + full/ct append_flow → processor → writer. |
| MainController | `framework/dlt_main_controller.py` | meta_loader → loop group's datasets → log → delegate. |
| create_dlt_pipelines | `deploy/create_dlt_pipelines.py` | Stamp out the 20 pipelines. |
| recon | `framework/recon.py` | Live Oracle vs UC counts → `recon_report`. |
| latency_report | `framework/latency_report.py` | Record-level e2e latency via Delta CDF → `latency_report`. |

---

## The flow, in code

```
dlt_entry.py               reads pipeline.source / table_group_no / dataset_list
      ↓
DLTMainController.run()     meta_loader → loop datasets → log → SingleTableController
      ↓
build_single_table()       derive config → <table>_raw streaming table
                           → full (seed) + ct append_flow (Auto Loader)
                           → processed view (standardize _op / _seq)
                           → apply_changes → bronze_scd1.<t> / bronze_scd2.<t>
```

## Metadata = the control plane

Five tables (see [metadata design](design/metadata-tables-design.html)):
`source_master`, `source_config` (k/v), `dataset_master` (holds
`table_group_no`), `dataset_config` (k/v — `primary_keys`, `scd_type`,
`load_type`, `cluster_by`, `delete_mode`), `platform_config` (k/v — global knobs).

**Onboard a table = 2 inserts** (`dataset_master` + `dataset_config`); the next run
of that pipeline group picks it up. No code change.

## Key capabilities

- **Log-based CDC → SCD1/SCD2** via `apply_changes` (no hand-rolled MERGE).
- **full + ct via `append_flow`** into one `_raw` table; `load_type` toggles the seed.
- **Idempotent** by primary key + sequence; safe re-runs, safe full/incr overlap.
- **Hard or soft delete** (`delete_mode` — soft flags `is_deleted` instead of removing).
- **Liquid clustering AUTO** (not aggressive auto-compaction — see the challenge).
- **20-way fan-out** (`table_group_no`) for parallelism + blast-radius isolation.
- **Observability**: recon (counts) + record-level latency (Delta CDF/time-travel).

---

## Run order (on Databricks)

1. `metadata/01_create_metadata_schema.sql` · `03_create_log_table.sql` · `04_create_ops_tables.sql`
2. `metadata/02_seed_example_metadata.sql` (example source + 2 datasets)
3. `deploy/create_dlt_pipelines.py --source ORA_SALES --groups 1-20 --entry <ws path> --dry-run`
4. Run the pipeline(s); inspect `ingest_meta.control.pipeline_log`.
5. (ops) `framework/recon.py` and `framework/latency_report.py` on a schedule.

**Learning path:** start with [ComponentWiseConceptToLearn](ComponentWiseConceptToLearn/)
(what is a redo log, CDC, SCD1/2, streaming, Auto Loader, apply_changes…), then the
[design pages](design/index.html), then the [Project Challenges](Project%20Challenges/).
