# Reload strategies (from ADLS vs from Oracle)

Sometimes you must **reload** a table — corruption, a logic bug, a missed window,
a schema change that needs a clean rebuild. There are two depths of reload,
depending on *where you still have good data*.

## 1 · Reload from ADLS (re-read the landing zone)

The `__full` / `__ct` parquet is **still in ADLS** — you just need Databricks to
reprocess it.

```
   keep Qlik + ADLS as-is  ──►  reset UC target + Auto Loader checkpoint  ──►  re-ingest
```

Steps:
- **Full refresh** the DLT table (drops target + Auto Loader checkpoint/schema
  state) so Auto Loader re-reads `__full` then `__ct` from the beginning.
- `apply_changes` rebuilds SCD1/SCD2 by **key + sequence** → same final state,
  **no duplicates** (idempotent — see [11](11-idempotency-primary-key.md)).
- Cheap and fast — no source involvement.

**Use when:** the landing data is good and complete; the problem was downstream
(bad transform, dropped target, checkpoint you want to reset).

## 2 · Reload from Oracle (re-extract via Qlik)

The landing data itself is **wrong or incomplete** (Qlik gap, purged files,
missed changes). You must go back to the **source**.

```
   Qlik full-load task re-snapshots Oracle  ──►  fresh __full to ADLS  ──►  re-ingest in DLT
```

Steps:
- Trigger a **Qlik full load** (re-reads the Oracle table / redo position) to
  regenerate `__full`, then resume CDC into `__ct`.
- On the Databricks side, treat it like onboarding: `load_type=full` to seed, then
  back to `incr`.
- Heavier — it loads the source and can hit **full-load problems** (long-running
  snapshots, [rollback/undo errors](../Project%20Challenges/full-load-problems.md)).

**Use when:** ADLS no longer has trustworthy data for the window you need.

## Choosing

| Symptom | Reload from |
|---|---|
| Bad transform / dropped UC table / reset checkpoint | **ADLS** (cheap) |
| Missing/gap in `__ct`, purged landing files, source drift | **Oracle** (full re-extract) |

**Guiding rule:** reload from the **closest trustworthy copy**. Only go back to
Oracle when ADLS can't answer — it's slower and stresses the source.

**Related:** [full → incr overlap](../Project%20Challenges/full-then-incr-overlap.md),
[idempotency](11-idempotency-primary-key.md), [full-load problems](../Project%20Challenges/full-load-problems.md).
