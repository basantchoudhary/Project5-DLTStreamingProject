# One table breaks the whole group

**Problem.** Table 7 of group 3 has a bad schema change and its flow errors.
What blows up — that table, that group, or all 200 tables?

**Cause.** Everything in one DLT pipeline shares a graph. A hard failure while
*building* the graph fails the whole pipeline update; a *runtime* flow failure
fails that flow (and can stall the update).

**Fix — the `table_group_no` fan-out.** 200 tables are split across **20
pipelines of ~10 tables**. A failure is contained to its group: the other 19
pipelines keep running. That's the whole point of the grouping — parallelism
**and** blast-radius isolation.

**Levers.**
- Group by **blast radius and SLA**, not randomly — keep a flaky, fast-changing
  source's tables out of the group holding your most critical tables.
- The `pipeline_log` `ERROR` row names the exact `dataset_id` that failed, so you
  fix one table, not hunt across a group.
- Smaller groups = tighter isolation but more pipelines to operate; 10/group is
  the balance here.

**Related:** [High cost](high-cost.md) — more, smaller pipelines isn't free.
