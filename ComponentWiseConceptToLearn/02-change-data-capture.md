# Change Data Capture (CDC)

**What it is.** CDC is capturing the **changes** to a dataset — inserts, updates,
deletes — as a stream of change events, instead of repeatedly copying the whole
table. You move only what changed.

**The shape of a change event.** Each event carries: the **operation** (I/U/D),
the **key**, the **new values**, and an **ordering** field (a sequence/SCN/LSN).

## Example

Source table `CUSTOMERS` over time:

```
t1  INSERT  id=7  name="Asha"   city="Pune"
t2  UPDATE  id=7  name="Asha"   city="Mumbai"     -- moved city
t3  DELETE  id=7
```

The CDC stream (what Qlik lands in `__ct`) looks like:

| header__change_oper | header__change_seq | id | name | city |
|---|---|---|---|---|
| I | 5001 | 7 | Asha | Pune |
| U | 5002 | 7 | Asha | Mumbai |
| D | 5003 | 7 | | |

A consumer that applies these in `header__change_seq` order reproduces the exact
final state of the source (here: row 7 ends up deleted).

**Full vs CDC.** A **full** extract is a snapshot ("here is every row now"); CDC
is the **delta** ("here is what changed since"). In this project each table has
both: `__full` (one-time seed) and `__ct` (ongoing CDC).

**In this project.**
- `processor.py` reads `header__change_oper` and standardizes it to `_op` (I/U/D)
  and `_seq` (from `header__change_seq`).
- `writer.py` feeds those to `apply_changes`, which turns the change stream back
  into a table (SCD1 latest-state, and/or SCD2 history).

**Related:** [Oracle redo log](01-oracle-redo-log.md) (the source),
[apply_changes](07-dlt-apply-changes.md) (the sink), [SCD1/2](03-scd1-vs-scd2.md).
