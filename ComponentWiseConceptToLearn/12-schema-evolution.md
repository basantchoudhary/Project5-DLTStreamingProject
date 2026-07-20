# Schema evolution

**What it is.** The source schema changes over time — a new column is added to
`ORDERS`. **Schema evolution** is the pipeline absorbing that change without
breaking and without a manual redeploy.

## How it works end to end

1. **Auto Loader** reads with `cloudFiles.schemaEvolutionMode = addNewColumns`
   and a per-table `cloudFiles.schemaLocation` (durable schema state).
2. A file arrives with a **new column**. Auto Loader detects it, updates the
   stored schema, and **restarts the stream once** to pick it up.
3. **DLT streaming table** adds the column; **`apply_changes`** carries it into
   the SCD1/SCD2 target.

```
   day 1: id, amount            → target: id, amount
   day 2: id, amount, currency  → Auto Loader adds col, stream restarts,
                                   target now: id, amount, currency
```

## What's handled vs not

| Change | Handled? |
|---|---|
| **Add** a column | ✅ automatic (addNewColumns) |
| **Widen** a type (int→long) sometimes | ⚠️ depends; may need rescue/backfill |
| **Rename** a column | ❌ looks like drop + add — needs intervention |
| **Drop** a column | ❌ old data keeps it; new rows null it |
| **Type change** (string→int) | ❌ not automatic; rescue data / new target |

- **Rescued data column** captures values that don't fit the current schema, so
  nothing is silently lost.
- `schemaLocation` is **state** — keep it stable per table (framework derives it
  under `schema_root/<table>/…`); deleting it forces re-inference.

## In this project

`reader.py` sets `schemaEvolutionMode=addNewColumns` + per-table `schemaLocation`
(`derived_config` → `schema_full` / `schema_ct`). Additive source changes flow
through hands-free; breaking changes are a deliberate operation.

**Related:** [Auto Loader](05-auto-loader.md),
[schema evolution in the change table](../Project%20Challenges/schema-evolution-in-ct.md).
