# Real-time to Power BI (end-to-end streaming)

The pipeline is streaming from Oracle to Gold. To make it **end-to-end streaming**,
the last hop — Gold → Power BI — must also push, not poll. Here are the options,
strongest first.

## The last-mile problem

```
   Oracle ─stream─► ADLS ─stream─► DLT (bronze→silver→gold) ─?─► Power BI
```

A normal Power BI **import** dataset refreshes on a schedule (minutes/hours) —
that breaks the streaming story at the very end. You need a **push/near-real-time**
path.

## Options

| Option | How it works | Latency | Use when |
|---|---|---|---|
| **DirectQuery on Gold (Databricks SQL)** | Power BI queries the Databricks SQL warehouse live against Gold; no import. | seconds–low tens of s | Simplest end-to-end; Gold is query-fast (clustering/materialized). |
| **Power BI push / streaming dataset (REST/API)** | A job (or a Structured Streaming `foreachBatch`) pushes new Gold rows to a Power BI streaming dataset via the REST API; tiles update live. | ~seconds | Live dashboards/tiles that must tick without user refresh. |
| **Event Hubs → Power BI (Stream Analytics)** | Gold changes (or Silver events) flow to Azure Event Hubs; Azure Stream Analytics outputs to a Power BI streaming dataset. | seconds | Already event-centric; want managed streaming to BI. |
| **Automatic page refresh (DirectQuery)** | Power BI report auto-refreshes the DirectQuery every N seconds. | N seconds | Lightweight near-real-time without a push pipeline. |

## Recommended shape here

1. **Gold in Databricks SQL, DirectQuery** for most reports — no data movement,
   always current, governed by Unity Catalog. Pair with **automatic page refresh**
   for dashboards.
2. For a few **live tiles** that must update continuously, add a small **push**
   path: a streaming job reads Gold's **Change Data Feed** (the same CDF used for
   [latency](../ComponentWiseConceptToLearn/18-observability-recon-and-latency.md))
   and pushes deltas to a **Power BI streaming dataset** via the REST API.

```
   gold.mart_* ──CDF/stream──► foreachBatch ──REST push──► Power BI streaming dataset ──► live tile
```

## The end-to-end latency budget (with BI)

```
   Oracle→Qlik (~10s) + Qlik→ADLS (~15s) + ADLS→UC (~30s) + Gold→PBI (~5–10s)
   ≈ ~1 min to a live tile   (see Project Challenges/end-to-end-latency.md)
```

## Trade-offs

- **DirectQuery** = freshest + no duplication, but load lands on the SQL warehouse
  (size it; keep Gold lean).
- **Push/streaming datasets** = true live tiles, but limited modeling (best for
  metrics/tiles, not rich models) and you operate the push job.
- Match the mechanism to the **SLA and cost** — most reports don't need push;
  reserve it for the handful of tiles that truly must tick.

**Related:** [triggers & processing time](../ComponentWiseConceptToLearn/09-trigger-and-processing-time.md),
[event-driven ingestion](../ComponentWiseConceptToLearn/06-event-driven-file-ingestion.md),
[end-to-end latency](../Project%20Challenges/end-to-end-latency.md).
