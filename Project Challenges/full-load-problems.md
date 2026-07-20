# Full-load problems (rollback / undo, ORA-01555)

**Problem.** The initial **full load** of a big Oracle table (Qlik snapshot) runs
for hours and fails partway with a **rollback-segment / undo** error — classically
`ORA-01555: snapshot too old`.

**Cause.** A long-running consistent read needs Oracle to reconstruct the table
*as of the query's start* using **undo** (rollback) data. If the table is busy
and undo is recycled before the read finishes, Oracle can no longer rebuild the
old image → the read dies. Long full loads on hot tables are exactly this risk.

**Fixes / levers.**
- **Chunk the full load** — extract by key ranges / partitions so no single read
  runs long enough to outlive undo (Qlik parallel full-load / segmented load).
- **Bigger / longer-retained undo** — increase `UNDO_RETENTION` and undo tablespace
  so consistent reads survive (a DBA lever, often temporary for the load window).
- **Run during a quieter window** — less concurrent DML = less undo churn.
- **Prefer resumable, restartable loads** — so a failure resumes a chunk rather
  than restarting the whole table.
- **Seed from a copy** — snapshot/standby/export as the full source if the primary
  is too hot.

**In the framework.** The Databricks side is unaffected — it just ingests whatever
`__full` lands. This is a **source-extraction** problem; the fix is in how Qlik/
Oracle run the snapshot. Once `__full` is complete, `load_type=full → incr` seeds
and switches as usual.

**Related:** [reload strategies](../ComponentWiseConceptToLearn/17-reload-strategies.md),
[redo-log flood](redo-log-flood-from-maintenance.md).
