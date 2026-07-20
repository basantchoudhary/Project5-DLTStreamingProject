# Spark Structured Streaming

**What it is.** Structured Streaming is Spark's engine for processing an
**unbounded** table that grows as new data arrives. You write (almost) the same
DataFrame code as for a batch, and the engine runs it **incrementally** as a
series of small batches — each run processes only the *new* input since the last.

DLT is a **declarative wrapper on top of Structured Streaming** — it generates
and manages these streams for you. To reason about DLT you must understand the
engine underneath.

## Core concepts (know these)

| Concept | What it means |
|---|---|
| **Unbounded / input table** | The source is treated as a table that keeps appending (files landing, Kafka, etc.). |
| **Micro-batch** | The engine wakes on a **trigger**, reads the new offsets, runs the query, writes output, commits. |
| **Trigger** | *When* to run a micro-batch: `processingTime="30 seconds"`, `AvailableNow` (drain then stop), continuous. See [09](09-trigger-and-processing-time.md). |
| **Checkpoint** | Durable record of *what's been processed* (source offsets + state). Restart resumes exactly where it left off — the basis of **exactly-once**. |
| **Offsets / progress** | Per-source position (which files / Kafka offsets are done). |
| **State store** | Keyed state kept across batches for aggregations, dedup, joins, and `apply_changes`. |
| **Output mode** | append / update / complete — how results are emitted. |
| **Watermark** | A bound on lateness for event-time ops so state can be dropped safely (see [late arrival](13-late-arrival-handling.md)). |
| **Stateful ops** | Dedup, windowed aggregates, stream-stream joins — need state + watermark. |

## Mental model

```
   new input ─► [trigger fires] ─► read new offsets ─► run query (+ state)
                                        │
                                        ▼
                        write output ─► commit offsets to checkpoint
                        (restart = resume from last commit = exactly once)
```

**In this project.** Auto Loader (`readStream.format("cloudFiles")`) is a
Structured Streaming source; each DLT table/flow is a streaming query; the
checkpoints DLT manages give exactly-once file ingestion; `apply_changes` is a
stateful operator that maintains the SCD merge state.

**Related:** [Why DLT (vs raw streaming)](15-why-dlt.md), [Auto Loader](05-auto-loader.md),
[triggers](09-trigger-and-processing-time.md).
