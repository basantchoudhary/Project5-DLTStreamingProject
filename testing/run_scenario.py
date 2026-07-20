"""run_scenario — glue: generate mock data, (run pipeline), assert results.

Databricks flow:
  1. write_scenario() lands __full/__ct parquet for a table.
  2. point a DLT pipeline (or the processor+writer path) at that landing root and
     run it — do this via the pipeline, then call check_scenario() below.
  3. check_scenario() runs the count check + SCD2 rules and prints a report.

Kept as plain functions so it works from a notebook, a job, or a pytest wrapper.
"""

from testing.generate_mock_data import write_scenario
from testing.assertions import assert_active_count, scd2_rules, summarize


# A tiny built-in scenario: one key that is inserted, updated twice, then deleted,
# plus a second key that survives. Sequence column drives SCD2 versioning.
CUSTOMERS_SCENARIO = {
    "table": "CUSTOMERS",
    "keys": ["customer_id"],
    "business_columns": ["customer_id", "name", "city"],
    "changes": [
        {"op": "I", "seq": 1, "customer_id": 7, "name": "Asha", "city": "Pune"},
        {"op": "U", "seq": 2, "customer_id": 7, "name": "Asha", "city": "Mumbai"},
        {"op": "I", "seq": 3, "customer_id": 8, "name": "Ravi", "city": "Delhi"},
        {"op": "U", "seq": 4, "customer_id": 7, "name": "Asha R", "city": "Mumbai"},
        {"op": "D", "seq": 5, "customer_id": 8},
    ],
    # after replay: customer 7 active (3 versions), customer 8 deleted
    "expected_active_hard_delete": 1,
}


def land_scenario(spark, landing_root, scenario=CUSTOMERS_SCENARIO):
    """Step 1 — write the scenario's parquet. Returns the paths + expectations."""
    info = write_scenario(
        spark, landing_root,
        table=scenario["table"], keys=scenario["keys"],
        changes=scenario["changes"], business_columns=scenario["business_columns"],
    )
    print(f"[test] landed scenario '{scenario['table']}': "
          f"{info['total_changes']} changes -> {info['full_path']} , {info['ct_path']}")
    return info


def check_scenario(spark, target_scd1, target_scd2, scenario=CUSTOMERS_SCENARIO,
                   is_deleted_col="is_deleted"):
    """Step 3 — assert count (SCD1) + SCD2 invariants (SCD2). Returns all_passed."""
    results = []
    if target_scd1:
        results.append(assert_active_count(
            spark, target_scd1, scenario["expected_active_hard_delete"], is_deleted_col))
    if target_scd2:
        results.extend(scd2_rules(spark, target_scd2, scenario["keys"]))

    all_ok, text = summarize(results)
    print(f"[test] scenario '{scenario['table']}' — {'ALL PASS' if all_ok else 'FAILURES'}:")
    print(text)
    return all_ok
