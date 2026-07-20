# Testing framework

Mock-first testing: **generate** synthetic CDC parquet → **run** the pipeline →
**assert** expected vs actual. Because the mock is generated, we know the exact
expected result — count mismatches, dropped rows, and SCD2 bugs surface at once.

See the concept: [ComponentWiseConceptToLearn/19-testing-framework](../ComponentWiseConceptToLearn/19-testing-framework.md).

## Files

| File | Role |
|---|---|
| `generate_mock_data.py` | `write_scenario()` — lands `<table>__full` + `<table>__ct` parquet from a compact scenario spec. |
| `assertions.py` | `assert_active_count()` + `scd2_rules()` (SCD2 invariants). |
| `run_scenario.py` | Glue: `land_scenario()` → (run pipeline) → `check_scenario()`; ships a built-in CUSTOMERS scenario. |

## The loop (on Databricks)

```python
from testing.run_scenario import land_scenario, check_scenario, CUSTOMERS_SCENARIO

# 1. land mock data to a test landing root
land_scenario(spark, "dbfs:/tmp/dlt_test/ora_sales")

# 2. run a DLT pipeline pointed at that landing root (metadata → test source),
#    OR drive processor+writer directly, producing the SCD1/SCD2 targets.

# 3. assert
check_scenario(spark,
               target_scd1="bronze_scd1.customers",
               target_scd2="bronze_scd2.customers")
```

## What the SCD2 rules check (the sequence column is the linchpin)

- **one current per key** — exactly one open (`__END_AT IS NULL`) version per key.
- **no overlap** — version windows `[__START_AT, __END_AT)` don't overlap.
- **contiguous history** — each closed version's end == the next version's start.
- **start < end** — every closed version is well-ordered (sequence sanity).

Add scenarios with out-of-order, duplicate, and same-timestamp sequences to stress
the ordering guarantees (see
[late arrival](../ComponentWiseConceptToLearn/13-late-arrival-handling.md)).

> Adjust `start_col` / `end_col` in `scd2_rules()` if your DLT SCD2 target uses
> different effective-dating column names.
