# Why DLT? (Structured Streaming vs DLT)

DLT (Delta Live Tables / Lakeflow Declarative Pipelines) is a **declarative
framework built on top of Structured Streaming**. You declare *what* tables
exist and *how* they're derived; DLT generates and operates the streams.

## What Structured Streaming gives you (raw)

The engine: micro-batches, checkpoints, state, exactly-once, triggers,
watermarks (see [04](04-spark-structured-streaming.md)). But **you** hand-write:
the read streams, the MERGE/upsert for CDC, checkpoint paths, table creation,
the dependency wiring between tables, retries, and observability.

## What DLT adds on top

| Capability | Raw Structured Streaming | DLT |
|---|---|---|
| CDC / SCD merge | hand-write `MERGE` per table | `apply_changes` (SCD1/2) declaratively |
| Dependencies / DAG | you orchestrate order | DLT infers the graph from `read`/`read_stream` |
| Checkpoints & table DDL | you manage paths + `CREATE TABLE` | managed for you |
| Data quality | custom code | `@dlt.expect*` expectations + metrics |
| Incremental + full refresh | you build it | built-in per table |
| Observability | you build dashboards | event log, lineage, data quality UI |
| Auto-optimize / compaction | manual `OPTIMIZE` | automatic on streaming tables |
| Multi-source into one table | manual union | `append_flow` |
| Retries / recovery | you code it | pipeline-managed |

## Pros of DLT

- **Less boilerplate** — declare tables, not loops and MERGEs. Perfect for a
  **metadata-driven** framework generating 200 tables.
- **Built-in CDC/SCD** via `apply_changes` — the single biggest win here.
- **Managed operations** — checkpoints, retries, auto-compaction, lineage, DQ.
- **Dependency graph** inferred and visualized; incremental by default.

## Cons / trade-offs

- **Less low-level control** — you work within DLT's model (e.g. table-scoped
  patterns, specific APIs) rather than arbitrary streaming code.
- **Platform lock-in** — it's Databricks-specific; not portable to open Spark.
- **Cost model** — DLT compute has its own SKU; always-on continuous pipelines
  add up (see [high cost](../Project%20Challenges/high-cost.md)).
- **Debuggability** — a generated graph is a level removed from the raw stream.

## If DLT weren't available, what would we be rebuilding?

- Per-table **CDC MERGE** logic for SCD1 **and** SCD2 (with ordering, deletes).
- **Checkpoint** and schema-location management for every stream.
- A **dependency/orchestration** layer to run tables in the right order.
- **Data-quality** expectations + metrics + quarantine plumbing.
- **Auto-compaction**, retries, and an **observability**/lineage layer.

In short: DLT is doing most of the undifferentiated heavy lifting this framework
would otherwise hand-roll. We keep the *metadata-driven* brains; DLT is the
execution substrate.

**Related:** [Spark streaming](04-spark-structured-streaming.md),
[apply_changes](07-dlt-apply-changes.md), [append_flow](08-append-flow.md).
