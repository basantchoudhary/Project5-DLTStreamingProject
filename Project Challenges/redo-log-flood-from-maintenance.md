# Redo-log flood from maintenance (≈3000 TB)

**Problem.** Oracle suddenly generated a **huge** volume of redo — on the order of
**3000 TB** — because of **maintenance activity** (bulk updates, index rebuilds,
reorgs, partition moves, mass `UPDATE`s). Qlik dutifully turned it all into CDC
and flooded ADLS; the pipeline was buried under change records that weren't
"real" business change.

**Cause.** Log-based CDC captures **every** logged change — including
maintenance DML that rewrites millions of rows without changing their meaning.
Redo volume ≠ business-change volume. A maintenance job can dwarf a day of real
transactions.

**Fixes / levers.**
- **Coordinate maintenance with CDC.** Pause / suspend the Qlik task around known
  heavy maintenance, then resync — don't stream a reorg as if it were business change.
- **NOLOGGING / direct-path** for maintenance where safe, so the operation
  doesn't generate normal redo (DBA lever; mind recovery implications).
- **Filter at the source** — exclude maintenance-only operations/tables from the
  CDC task if they carry no analytical value.
- **Throttle / scale ingest** for the burst — larger triggers, temporary extra
  compute, so the flood drains without blocking other groups.
- **Backfill instead of stream** — for a mass rewrite, a targeted full reload of
  the affected table is often cheaper than streaming billions of ct rows
  (see [reload strategies](../ComponentWiseConceptToLearn/17-reload-strategies.md)).

**Lesson.** In log-based CDC, **source maintenance is a data-pipeline event**.
Ops calendars for the source DB must be shared with the ingestion team — a silent
index rebuild can cost more than a month of normal load.

**Related:** [high cost](high-cost.md), [small files](small-files-from-cdc.md).
