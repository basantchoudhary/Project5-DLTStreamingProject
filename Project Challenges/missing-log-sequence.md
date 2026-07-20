# Missing log sequence (Oracle deleted the redo/archive logs)

**Problem.** Qlik Replicate's CDC task fails with a **"missing log sequence"** /
cannot-find-archived-log error. The change stream stops — Qlik needs a redo log
sequence that **Oracle has already deleted**, so it can't continue from where it
left off.

**Cause.** Log-based CDC reads Oracle's **archived redo logs** in sequence. If an
archived log Qlik still needs is **purged before Qlik consumes it**, the sequence
has a hole and CDC can't proceed. Common reasons:
- **Aggressive archive-log retention / RMAN delete policy** — backups delete
  archived logs quickly (e.g. "delete after backup").
- **FRA (Fast Recovery Area) space pressure** — Oracle auto-deletes older archived
  logs to make room when the recovery area fills.
- **CDC fell behind** — a Qlik task outage, a slow window, or a
  [redo flood](redo-log-flood-from-maintenance.md) meant Qlik couldn't keep up, so
  the logs it needed aged out before it read them.

**How we fixed it — work with the DBA, increase retention.**
- **Root-caused *why* logs were being deleted** with the DBA — not just "make it
  work", but which policy/space pressure removed them.
- **Increased archived-log retention to 48 hours**, so there's a generous window
  for CDC to consume every log even across an outage or a backlog. Retention must
  be **> the maximum expected CDC downtime / lag**.
- Made sure the **RMAN delete policy** doesn't purge archived logs until
  replication (not just backup) has consumed them — "delete after applied/shipped",
  not "delete after backup".
- Ensured **FRA is sized** so space pressure doesn't force early deletion.

**Prevention / monitoring.**
- **Alert on CDC lag** — if Qlik's read position falls too far behind the current
  log, you're at risk *before* the log is gone. Catch it early.
- **Retention as a contract** — the source DB's log retention is part of the
  pipeline's SLA. The ingestion team must know (and sign off on) it, just like the
  [maintenance calendar](redo-log-flood-from-maintenance.md).
- If a gap *does* happen and logs are truly gone, recovery is a **reload** — resync
  the affected table from a fresh full extract (see
  [reload strategies](../ComponentWiseConceptToLearn/17-reload-strategies.md)).

**Lesson.** Log-based CDC has a hard dependency on the source's **log retention**.
"Missing log sequence" is almost always a retention/space policy on the Oracle
side, not a Qlik bug — the fix lives with the DBA. Retention must comfortably
exceed how far behind CDC can ever fall.

**Related:** [Oracle redo log](../ComponentWiseConceptToLearn/01-oracle-redo-log.md),
[redo-log flood](redo-log-flood-from-maintenance.md),
[reload strategies](../ComponentWiseConceptToLearn/17-reload-strategies.md).
