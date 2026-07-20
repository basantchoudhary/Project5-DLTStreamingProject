# Trigger & processing time

**What it is.** A **trigger** decides *when* a streaming query runs its next
micro-batch. **Processing time** trigger = "run a micro-batch every N seconds".

```python
.trigger(processingTime="30 seconds")   # a batch at most every 30s
```

## The trigger types

| Trigger | Behaviour | Use when |
|---|---|---|
| `processingTime="30 seconds"` | Wake every 30s, process what's new, sleep. | Steady low-latency streaming with bounded cost. |
| **Continuous** | Always on, minimal gap. | Sub-second/second latency needs. |
| `AvailableNow` | Process everything available **now**, then **stop**. | Triggered/scheduled runs — great for cost. |
| `Once` (legacy) | One batch then stop. | Superseded by `AvailableNow`. |

## The core trade-off

```
   short interval  ──►  lower latency,  more runs, more small files, more $$
   long interval   ──►  higher latency, fewer runs, bigger files, less $$
```

Pick the **loosest** interval that meets the SLA:

- Business wants **< 1 min** → short processing-time (e.g. 20–30s) or continuous,
  plus event-driven ingest (see [latency](../Project%20Challenges/end-to-end-latency.md)).
- Business is fine with **hourly** → `AvailableNow` on a schedule; pay only while
  it runs (see [high cost](../Project%20Challenges/high-cost.md)).

## In DLT terms

A DLT pipeline in **triggered** mode runs each streaming table with an
`AvailableNow`-style pass and stops (cheap, scheduled). In **continuous** mode it
keeps streaming (low latency, always-on compute). This is the single biggest
cost-vs-latency lever in the project.

**In this project.** `create_dlt_pipelines.py` exposes `--continuous`
(default = triggered). Triggered + serverless is the cost-friendly default; flip
to continuous (or short trigger) only for the tables that truly need sub-minute
freshness.

**Related:** [Spark streaming](04-spark-structured-streaming.md),
[latency](../Project%20Challenges/end-to-end-latency.md), [cost](../Project%20Challenges/high-cost.md).
