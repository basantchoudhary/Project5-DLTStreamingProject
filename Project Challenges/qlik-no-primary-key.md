# Qlik performance & no primary key (incl. true duplicates)

Two related pains when a source table has **no primary key**.

## A · Qlik Replicate performance with no PK

**Problem.** CDC apply is slow / heavy for a table with no PK.

**Cause.** Without a key, an UPDATE/DELETE can't be targeted by identity — the CDC
mechanism falls back to matching on **all columns** (or a full-row comparison) to
find the affected row. That's expensive per change and scales badly.

**Fixes.**
- Define a **real or logical key** at the source if at all possible (unique
  index) — the single biggest win.
- If none exists, configure Qlik with a **defined key set** (a column combo that
  is unique enough) so apply isn't full-row.
- For downstream, a keyless table often should be **append-only** (see below)
  rather than merged.

## B · No PK *and* genuine duplicate rows — business wants BOTH kept

**Problem.** The table has rows identical across **every** column, and the
business explicitly wants **both** copies preserved (they're distinct events,
e.g. two identical payments).

**Cause.** `apply_changes` needs a key to define identity; with true duplicates,
any key would collapse them — losing a row the business needs.

**Fixes.**
- **Don't merge — append.** Treat the feed as an **append-only** fact: keep every
  record, no dedup. (A `create_streaming_table` + `append_flow`, *not*
  `apply_changes`.)
- **Synthesize identity** if you must merge downstream: a **sequence / ingestion
  order + content hash** gives each duplicate a distinct key, so both survive and
  reprocessing is still idempotent.
- Make the choice **explicit in metadata** (e.g. a `merge_mode = append|cdc` knob)
  so the framework skips `apply_changes` for these tables.

**Lesson.** "No primary key" isn't one problem — it's a *performance* problem at
the source and an *identity* problem in the merge. Duplicates that are
**business-meaningful** must be appended, not deduped.

**Related:** [idempotency](../ComponentWiseConceptToLearn/11-idempotency-primary-key.md),
[changing primary key](changing-primary-key.md).
