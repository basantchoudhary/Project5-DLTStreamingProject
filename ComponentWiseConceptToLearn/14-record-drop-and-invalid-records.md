# Record drop, invalid records & reprocessing

**The question.** What happens to a bad record — one with a null key, a malformed
value, a type that won't parse? Do we drop it, fail the batch, or set it aside?

## The wrong answers

- **Silently drop it** → data loss you can't see or recover.
- **Fail the whole batch** → one bad row halts 10 tables.

## The pattern: quarantine, don't drop

Use **DLT expectations** to classify each record and route the bad ones to a
**quarantine**, so nothing is lost and good data keeps flowing.

| Expectation | Behaviour on failure |
|---|---|
| `@dlt.expect` (warn) | keep the row, **count** the violation (metrics only) |
| `@dlt.expect_or_drop` | **drop** the row from the target, but it's counted (and can be captured) |
| `@dlt.expect_or_fail` | **fail** the update (use only for truly fatal invariants) |

**Better than plain drop — mark invalid + quarantine:**

```
   incoming row ─► valid?  ── yes ─►  clean stream ─► apply_changes ─► target
                     │
                     └── no ──► is_valid=false + reason ─► <table>_quarantine
```

- Tag the row (`is_valid=false`, `dq_reason="null primary_key"`) instead of
  deleting it, and write it to a `<table>_quarantine` table.
- Good records proceed; the pipeline never blocks on bad ones.

## Reprocessing invalid records

Once the root cause is fixed (source patched, mapping corrected):

1. Read the quarantine table (or the specific failed `_seq`/keys).
2. Re-emit those records into the ingestion flow (or a targeted backfill flow).
3. `apply_changes` merges them by **key + sequence**, so replay is **idempotent**
   — no duplicates, correct final state (see [idempotency](11-idempotency-primary-key.md)).

## Levers

- Decide per rule: **warn / drop-to-quarantine / fail** — most are quarantine.
- Keep a **reason** column so triage is fast.
- Track quarantine counts in metrics/`pipeline_log`; a spike is an early warning.

**Related:** [apply_changes](07-dlt-apply-changes.md),
[idempotency](11-idempotency-primary-key.md), [late arrival](13-late-arrival-handling.md).
