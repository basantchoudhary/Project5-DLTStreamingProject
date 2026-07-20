# Changing primary key (mutable key)

**Problem.** The table *has* a primary key, but the source **changes the key
value** of an existing row (a business "renumber", a re-key migration). Downstream
this looks like the old row **vanished** and a brand-new row **appeared** — history
splits, SCD2 breaks, counts drift.

**Cause.** `apply_changes` uses the key as **row identity**. If `order_id` moves
from `101 → 900` for the same logical row, CDC emits it as a **delete of 101 +
insert of 900** (or an update whose key column changed). The merge can't know
`101` and `900` are the *same* row, so:
- SCD1: `101` is deleted, `900` inserted — the link is lost.
- SCD2: `101`'s history closes; `900` starts fresh — one timeline becomes two.

**Fixes / levers.**
- **Best: keys shouldn't change.** Push back on mutable primary keys — use a
  **surrogate/immutable key** as the CDC key and treat the mutable one as an
  attribute.
- If the source exposes both **old and new key** on the change (some CDC tools
  emit `before`/`after` key images), map the transition explicitly so history
  follows the row.
- If a re-key is a known **one-off**, handle it as a **controlled migration**
  (backfill/stitch the two timelines) rather than letting the stream guess.
- Document per table whether the key is **stable**; unstable keys need special
  handling, not the default `apply_changes` path.

**Lesson.** A primary key is only useful for CDC if it's **immutable**. A changing
key is worse than no key — it silently fragments identity and history. Choose the
merge key for stability, not just uniqueness.

**Related:** [idempotency](../ComponentWiseConceptToLearn/11-idempotency-primary-key.md),
[no primary key](qlik-no-primary-key.md), [SCD1/2](../ComponentWiseConceptToLearn/03-scd1-vs-scd2.md).
