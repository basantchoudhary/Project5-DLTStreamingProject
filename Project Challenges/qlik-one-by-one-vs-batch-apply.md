# Qlik one-by-one vs batch apply

**Problem.** Qlik Replicate was sometimes processing changes **one record at a
time** when it should have been applying them in **batches**. Row-by-row apply is
far slower — high latency, more load on source and target, and CDC falling behind
(which itself risks [missing log sequence](missing-log-sequence.md)).

**Background — Qlik's two apply modes.**
- **Batch optimized apply** — groups many change records and applies them in bulk
  (net changes per key). Fast, the default we want.
- **Transactional / one-by-one apply** — applies changes individually, in order.
  Correct but slow; Qlik falls back to it in certain conditions.

**Why Qlik drops to one-by-one (there are several reasons).**
- **No primary key / unique index** — without a key to identify a row, Qlik can't
  safely batch UPDATEs/DELETEs (it can't collapse "net change per key"), so it
  falls back to row-by-row full-row matching. ← **our root cause.**
- LOB columns / certain data types that can't be bulk-applied.
- Apply **conflict/error-handling** settings that force per-row so it can react to
  each failure.
- Very high conflict rates or tables mixed in a task with incompatible settings.

**RCA — it was the keyless tables.** We did a root-cause analysis and found the
one-by-one behaviour was isolated to the tables **without a primary key**. Those
tables forced Qlik into transactional apply, and because they shared a task with
well-behaved tables, they **dragged the whole task's throughput down**.

**Fix — a separate Qlik task for the no-PK tables.** We split the keyless tables
into their **own dedicated Qlik Replicate task**, so:
- Their slow, row-by-row apply is **isolated** and no longer throttles the
  batch-capable tables (which went back to fast batch apply in their own task).
- The no-PK task can be **tuned independently** (batch/apply settings, parallelism,
  and where possible a **surrogate/logical key** to let even those batch).
- Blast radius shrinks — a problem on the awkward tables doesn't stall the rest.

**Lesson.** Apply mode is a throughput cliff: **batch vs one-by-one** can be 10–100×.
The usual trigger is a **missing key**, and one bad table in a shared task poisons
the whole task's rate. Isolate the awkward tables into their own task and fix the
key where you can.

**Related:** [Qlik & no primary key](qlik-no-primary-key.md),
[changing primary key](changing-primary-key.md),
[idempotency via PK](../ComponentWiseConceptToLearn/11-idempotency-primary-key.md).
