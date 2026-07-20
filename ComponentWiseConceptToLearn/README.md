# Component-Wise Concepts to Learn

The concepts a data engineer should understand to build (or defend) this
framework, grouped by where they show up. Each page is a short, example-first
explainer — definition, why it matters *here*, and where in the code it appears.

## Foundations (the "what is…" set)

| # | Concept | One line |
|---|---|---|
| 01 | [Oracle Redo Log](01-oracle-redo-log.md) | The ordered record of every DB change — our CDC source. |
| 02 | [Change Data Capture (CDC)](02-change-data-capture.md) | Capturing inserts/updates/deletes as a stream — with an example. |
| 03 | [SCD Type 1 vs Type 2](03-scd1-vs-scd2.md) | Latest-state vs full history dimensions. |
| 04 | [Spark Structured Streaming](04-spark-structured-streaming.md) | The incremental micro-batch engine under everything. |
| 05 | [Auto Loader](05-auto-loader.md) | Incremental, checkpointed file ingestion from cloud storage. |
| 06 | [Event-driven file ingestion](06-event-driven-file-ingestion.md) | ADLS Gen2 events → Event Grid → Databricks → DLT reads the file. |
| 09 | [Trigger & processing time](09-trigger-and-processing-time.md) | `processingTime="30 seconds"` — how often a micro-batch runs. |
| 12 | [Schema evolution](12-schema-evolution.md) | New source columns flow through without breaking. |
| 15 | [Why DLT? (streaming vs DLT)](15-why-dlt-vs-spark-streaming.md) | What DLT adds, pros/cons, what we'd rebuild without it. |
| 16 | [Technology alternatives](16-technology-alternatives.md) | Alternatives to Qlik, ADLS, DLT, Databricks — and why these. |

## Framework mechanics

| # | Concept | Shows up in |
|---|---|---|
| 07 | [apply_changes](07-dlt-apply-changes.md) | `writer.py` — the CDC/SCD merge |
| 08 | [append_flow](08-append-flow.md) | `dlt_single_table_controller.py` — full + ct into one table |
| 10 | [Delete handling (soft delete)](10-delete-handling-soft-delete.md) | `processor.py` / `writer.py` — D → is_deleted |
| 11 | [Idempotency via primary key](11-idempotency-primary-key.md) | `writer.py` — re-runs don't duplicate |
| 13 | [Late arrival handling](13-late-arrival-handling.md) | `sequence_by`, watermarks |
| 14 | [Record drop / invalid & reprocess](14-record-drop-and-invalid-records.md) | expectations, quarantine, replay |
| 17 | [Reload strategies (ADLS vs Oracle)](17-reload-strategies.md) | reprocess from the closest trustworthy copy |
| 18 | [Observability: recon & latency](18-observability-recon-and-latency.md) | `recon.py`, `latency_report.py` (CDF/time-travel) |
| 19 | [Testing framework](19-testing-framework.md) | mock → DLT → assert; SCD2 rules |

## Component → concept map

| Component | Concepts to know |
|---|---|
| `meta_loader` | metadata-driven design; UC Delta tables; key/value (EAV) config |
| `reader` (Auto Loader) | 04 streaming · 05 Auto Loader · 06 event-driven · 12 schema evolution |
| `processor` | 02 CDC · 10 delete handling · 13 late arrival · 14 invalid records |
| `writer` | 07 apply_changes · 03 SCD1/2 · 11 idempotency |
| controllers | 04 streaming · 08 append_flow · 15 why DLT · DLT graph construction |
| `create_dlt_pipelines` | 09 triggers · Databricks SDK · pipeline config |
| `recon` / `latency_report` | 18 observability · CDF / time travel |
| `testing` | 19 testing framework · SCD2 invariants |
