# Schema evolution in the change table

**Problem.** The source team adds a column to `ORDERS`. New `__ct` parquet files
now have an extra field. Does the pipeline break?

**Cause.** A streaming reader with a fixed schema either drops the new column or
fails when the file schema no longer matches.

**Fix — Auto Loader schema evolution.** The reader runs with
`cloudFiles.schemaEvolutionMode = addNewColumns` and a per-table
`schemaLocation`. When a new column appears Auto Loader records it, the stream
restarts once, and the column flows through. DLT streaming tables and
`apply_changes` add the column to the target.

**Levers / watch-outs.**
- New columns are **additive**. Type changes or dropped columns are *not* auto
  handled — those need intervention (backfill / new target).
- The `schemaLocation` is state: keep it stable per table (the framework derives
  it under `schema_root/<table>/…`). Don't delete it casually.
- A schema change triggers a stream restart — expect one short blip, not data loss.
