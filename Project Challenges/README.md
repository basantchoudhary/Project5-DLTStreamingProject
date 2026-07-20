# Project Challenges — real-world war stories

Short **problem → cause → fix** pages for the things that actually bite a
metadata-driven, log-based CDC streaming pipeline in production. Each is a
flavour, not a deep dive — enough to recognise the symptom and know the lever.

| # | Challenge | The gist |
|---|---|---|
| 01 | [Full → incr overlap & duplicates](full-then-incr-overlap.md) | Seeding with `__full` while `__ct` is already flowing — why it doesn't duplicate. |
| 02 | [apply_changes out-of-order CDC](apply-changes-ordering.md) | Late/rewound change records; `sequence_by` and why ordering is everything. |
| 03 | [Schema evolution in the change table](schema-evolution-in-ct.md) | Source adds a column; Auto Loader + DLT reaction and how to not break. |
| 04 | [Small files from CDC](small-files-from-cdc.md) | Qlik writes many tiny parquet files; ingest slows; what to tune. |
| 05 | [One table breaks the whole group](blast-radius-and-groups.md) | Why 20 groups of 10, and what happens when table 7 of group 3 fails. |
| 06 | [High cost](high-cost.md) | The cost levers — uptime, triggers, serverless, small files, group sizing. |
| 07 | [End-to-end latency (< 1 min)](end-to-end-latency.md) | The three hops source→Qlik→ADLS→UC and how to hit a sub-minute SLA. |
| 08 | [Full-load problems (rollback/ORA-01555)](full-load-problems.md) | Long Oracle snapshots dying on undo/rollback, and how to chunk them. |
| 09 | [Redo-log flood from maintenance (≈3000 TB)](redo-log-flood-from-maintenance.md) | Maintenance DML floods CDC; redo volume ≠ business change. |
| 10 | [Qlik perf & no primary key](qlik-no-primary-key.md) | Keyless CDC is slow; true duplicates the business wants kept. |
| 11 | [Changing primary key](changing-primary-key.md) | A mutable key fragments identity and SCD2 history. |
| 12 | [Auto-optimize overhead → liquid clustering AUTO](auto-optimize-overhead.md) | Auto-compaction over-fired on streaming; moved to clustering AUTO. |
| 13 | [Missing log sequence (deleted redo logs)](missing-log-sequence.md) | Oracle purged archived logs before Qlik read them; retention → 48h. |
| 14 | [Qlik one-by-one vs batch apply](qlik-one-by-one-vs-batch-apply.md) | No-PK tables forced row-by-row apply; split into a separate task. |

> These mirror the style of the ecom project's `Project Challenges/` — problem
> first, root cause second, the proportionate fix third.
