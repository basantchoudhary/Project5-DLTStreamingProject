# Auto Loader

**What it is.** Auto Loader (`cloudFiles`) is Databricks' way to **incrementally
ingest files from cloud storage** as they arrive — a Structured Streaming source
that tracks which files it has already processed, so each run picks up only the
new ones.

```python
spark.readStream.format("cloudFiles")
     .option("cloudFiles.format", "parquet")
     .option("cloudFiles.schemaLocation", ".../_schemas/orders/ct")
     .load(".../ORDERS__ct")
```

## Why not just `spark.read.parquet(folder)`?

A plain read reprocesses **all** files every time. Auto Loader remembers
progress in its **checkpoint**, so:

- **Exactly-once**: a file is ingested once, even across restarts.
- **Incremental**: cost ∝ new files, not total files.
- **Schema inference + evolution**: infers the schema, stores it in
  `schemaLocation`, and can add new columns automatically (see [12](12-schema-evolution.md)).

## Two file-discovery modes

| Mode | How it finds new files | When |
|---|---|---|
| **Directory listing** (default) | Lists the folder each trigger. | Simple; fine at low/medium file counts. |
| **File notification** (event-driven) | Cloud storage **events** tell it about new files — no listing. | Millions of files / low latency / cost. See [06](06-event-driven-file-ingestion.md). |

## Useful options

- `cloudFiles.schemaLocation` — where the inferred schema + evolution state live (per table).
- `cloudFiles.inferColumnTypes` — infer real types, not all-strings.
- `cloudFiles.schemaEvolutionMode=addNewColumns` — absorb new columns.
- `_metadata.file_path` — lineage: which file a row came from.

**In this project.** `reader.py` builds one Auto Loader stream for `__full` and
one for `__ct`, each with its own `schemaLocation`, and tags rows with
`_ingest_source` / `_ingest_file` / `_ingest_ts`. DLT owns the checkpoints.

**Related:** [Spark streaming](04-spark-structured-streaming.md),
[event-driven ingestion](06-event-driven-file-ingestion.md), [schema evolution](12-schema-evolution.md).
