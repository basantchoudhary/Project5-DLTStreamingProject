# Project 5 · DLT Streaming Project (Delta Live Tables)

> **Training project — breadth over depth.** Design a **streaming** medallion pipeline
> with **Delta Live Tables** — declarative tables + built-in quality expectations —
> the contrast to Project 1's imperative batch notebooks.

Part of a 7-project data-engineering training portfolio. Concepts:
[Project 1 · theory/databricks-jobs-and-orchestration](../ecomsimpledataplatformusingdatabricks/notebooks/common/theory/databricks-jobs-and-orchestration.md).

---

## Problem statement

Events (e.g. e-commerce clickstream / orders) arrive continuously. Instead of
hand-writing batch loads, **declare** the pipeline in DLT: DLT infers the DAG, handles
incremental/streaming, checkpointing, retries, and data-quality expectations.

---

## Target architecture

```
   SOURCE (files/Kafka/EventHub)
        │  Auto Loader (schema infer + evolution, exactly-once)
        ▼
   ┌─────────────────────────── DLT PIPELINE (declarative) ───────────────────────────┐
   │  STREAMING TABLE  bronze_events      raw append, continuous                        │
   │        │  @dlt.expect (quality rules; drop/quarantine/fail)                        │
   │  STREAMING TABLE  silver_events      cleaned, deduped (watermark on event time)    │
   │        │                                                                            │
   │  MATERIALIZED VIEW  gold_sales_by_min   incremental aggregate, auto-refreshed       │
   └──────────────────────────────────────────────────────────────────────────────────┘
        DLT manages: DAG · checkpoints · retries · autoscaling · lineage · data quality
```

---

## Batch (Project 1) vs Streaming/DLT (this project)

```
   PROJECT 1 (imperative batch)            PROJECT 5 (declarative streaming)
   ─────────────────────────────           ──────────────────────────────────
   you write MERGE + watermark logic       you DECLARE tables; DLT does incremental
   you wire the DAG (driver / bundle)      DLT infers the DAG from dependencies
   DQ helpers you built                    @dlt.expect built-in expectations
   run on schedule                         continuous (or triggered) streaming
   full control                            less boilerplate, opinionated
```

---

## Design decisions

| Decision | Rationale |
|---|---|
| **Auto Loader source** | Exactly-once file ingestion, schema evolution, no manual bookkeeping. |
| **Streaming tables for B/S** | Continuous incremental without hand-rolled watermarks. |
| **`@dlt.expect` for DQ** | Quality is declarative + tracked in the DLT event log. |
| **Materialized view for Gold** | Incremental aggregate refresh, not full recompute. |
| **Event-time watermarks** | Correct dedup/windowing on late/out-of-order events. |
| **Streaming vs triggered** | Continuous for low latency; triggered for cost control. |

---

## Cross-cutting concerns
Exactly-once & checkpointing · late/out-of-order data (event-time watermarks) ·
backpressure & autoscaling · expectation actions (warn / drop / fail) · the DLT
**event log** as observability · cost of always-on vs triggered.

## Tech stack
Delta Live Tables · Auto Loader · Structured Streaming · streaming tables &
materialized views · `@dlt.expect` · (optional) Kafka / Event Hubs source.

## Planned (light) components
- `dlt/bronze_events` — Auto Loader → streaming bronze.
- `dlt/silver_events` — dedup + expectations (event-time watermark).
- `dlt/gold_sales_by_min` — materialized view aggregate.
- `pipeline.json` / bundle — DLT pipeline config (continuous vs triggered).
- `docs/streaming-vs-batch.md` — when to use DLT vs Project 1's batch approach.

**Status:** scaffold — declare the three DLT tables + expectations lightly.
