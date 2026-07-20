# Heavy joins on streaming — cost & performance (read the plan)

**Problem.** Cost and performance blew up on the streams that did **heavy joins**.
Latency rose and compute cost climbed. Reading the **query plan** revealed the
culprits — and they were different per query.

**Diagnosis — check the query plan first.** `EXPLAIN FORMATTED` (and the Spark UI
SQL tab) shows the join **type**, scan sizes, row estimates, and shuffles. That's
where the problems were visible:

| What the plan showed | Why it's expensive |
|---|---|
| **Many-to-many join** | Join keys weren't unique on both sides → the join **exploded** row counts (near-cartesian blowup), multiplying downstream work. |
| **Broadcast not used** | A small dimension that *should* broadcast was instead going through a **SortMergeJoin** — shuffling/sorting the large stream every batch. |
| **Full scans** | Queries written so no pruning happened — filter applied **after** the join, non-sargable predicates, `SELECT *`, or no clustering on the join key → read **everything**. |

**Fixes.**
- **Kill the many-to-many.** Dedup/aggregate the smaller side to the **right
  grain** before joining, and make sure join keys are **unique** on at least one
  side. A one-to-many is fine; many-to-many is usually a modeling bug.
- **Force / enable broadcast** for small dimensions (`broadcast()` hint or tune
  `spark.sql.autoBroadcastJoinThreshold`) so the big stream isn't shuffled — a
  BroadcastHashJoin against a small side is dramatically cheaper.
- **Rewrite for pruning.** Push filters **before** the join (predicate pushdown),
  select only needed columns, keep predicates **sargable**, and cluster on the
  **join key** so the reader can skip files (ties to
  [liquid clustering](liquid-clustering-not-working.md)).
- **Bound stream-stream joins** with a **watermark** so join state doesn't grow
  unbounded (see [late arrival](../ComponentWiseConceptToLearn/13-late-arrival-handling.md)).
- **Re-check the plan** after each change: confirm BroadcastHashJoin, sane row
  estimates, and file-scan metrics dropping.

**Lesson.** On streaming, **joins are the #1 cost/perf lever**. Never guess —
**read the plan**: watch for many-to-many blowups, broadcast that didn't happen,
and full scans from queries written the wrong way. Most fixes are modeling
(grain/keys) and query hygiene (filter early, broadcast small, cluster the key),
not more compute.

**Related:** [high cost](high-cost.md),
[liquid clustering not working](liquid-clustering-not-working.md),
[streaming join miss](streaming-join-miss-reprocess.md).
