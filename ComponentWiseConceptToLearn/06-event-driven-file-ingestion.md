# Event-driven file ingestion (ADLS Gen2 → Event Grid → DLT)

**What "event-driven" means.** Instead of *polling* ("has a new file appeared?"
by listing the folder over and over), the storage system **pushes a notification
the moment a file lands**, and the consumer reacts to that event. Lower latency,
far less wasted listing at scale.

## The chain (managed file events)

```
   Qlik writes ORDERS__ct/part-000.parquet to ADLS Gen2
        │
        ▼  storage emits a "BlobCreated" event
   Azure Event Grid  (managed event routing)
        │
        ▼  Databricks-managed subscription / queue
   Databricks (internal event service) routes the event
        │
        ▼
   The running DLT / Auto Loader stream receives "new file X"
        │
        ▼  reads only file X, then records it in the checkpoint (exactly-once)
   apply_changes merges it into the SCD1/SCD2 table
```

- **ADLS Gen2** is the landing store (hierarchical namespace on Azure Blob).
- **Event Grid** is Azure's managed event router; a *BlobCreated* event fires per new file.
- With **managed file events** on a Unity Catalog **external location**, Databricks
  provisions and manages the Event Grid subscription + queue for you — you don't
  hand-wire the plumbing.
- The **checkpoint** ensures a file that was notified is processed **once**; a
  duplicate notification or a restart doesn't reprocess it.

## Why it matters here

- **Latency:** hop (3) source→UC drops from "next listing interval" to "seconds
  after the file lands" — key to the [< 1 min SLA](../Project%20Challenges/end-to-end-latency.md).
- **Cost/scale:** no repeated `LIST` over folders with millions of objects
  (see [small files](../Project%20Challenges/small-files-from-cdc.md), [high cost](../Project%20Challenges/high-cost.md)).

## Listing vs notification

| | Directory listing | File notification (event-driven) |
|---|---|---|
| Discovery | poll + LIST each trigger | pushed per new file |
| Latency | ≥ trigger interval | seconds |
| Cost at scale | LIST-heavy | per-event |
| Setup | none | managed via external location |

**In this project.** Turning on file-notification mode for the `__ct` Auto Loader
streams is the main lever for sub-minute latency without paying to list huge
landing folders.

**Related:** [Auto Loader](05-auto-loader.md), [triggers](09-trigger-and-processing-time.md).
