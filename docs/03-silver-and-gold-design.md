# Silver & Gold design (on top of this bronze)

This project produces **bronze** — raw CDC merged into SCD1/SCD2 streaming tables.
Here's how Silver and Gold layer on top, keeping the whole thing streaming.

```
   bronze_scd1.<t> / bronze_scd2.<t>   (CDC-merged, per source table)
            │  clean · conform · model
            ▼
   silver.dim_* / silver.fct_*         (business entities, conformed, DQ-checked)
            │  aggregate · serve
            ▼
   gold.agg_* / gold.mart_*            (KPIs, marts, BI-ready)
```

## Silver — how it's designed

**Purpose.** Turn source-shaped bronze into **business-shaped, trustworthy**
entities. Still incremental (DLT streaming tables reading bronze streams).

Design choices:
- **Conform & rename** — map Oracle column names to business names, standardize
  types, units, enums; join reference/lookup tables.
- **Model** — bronze is per-source-table; Silver is **dimensions and facts**
  (e.g. `silver.dim_customer`, `silver.fct_order`). One Silver entity may combine
  several bronze tables.
- **Keys** — promote/derive business keys; add surrogate keys where needed
  (important where the source key is weak — see
  [changing PK](../Project%20Challenges/changing-primary-key.md)).
- **SCD** — carry SCD2 history where the business needs a timeline; collapse to
  latest (SCD1) where it doesn't. Bronze already did the CDC merge; Silver picks
  the grain.
- **Deletes** — respect the soft-delete flag: Silver typically serves
  `WHERE NOT is_deleted` for "active", but keeps deleted rows for history.
- **Data quality** — DLT **expectations** here (not-null keys, ranges, referential
  checks); route violations to quarantine (see
  [record drop / invalid](../ComponentWiseConceptToLearn/14-record-drop-and-invalid-records.md)).
- **Late data** — Silver aggregations/joins add **watermarks** sized to real
  lateness (see [late arrival](../ComponentWiseConceptToLearn/13-late-arrival-handling.md)).

## Gold — how it's designed

**Purpose.** **Serve** — the shapes BI and consumers query directly. Cheap,
derived, often fully recomputed or incrementally aggregated.

Design choices:
- **Aggregates & marts** — `gold.agg_daily_sales`, `gold.mart_customer_360` —
  pre-joined/pre-aggregated star schemas so BI is fast.
- **Grain-first** — define the grain explicitly (per day × store, per customer);
  everything rolls up to it.
- **Incremental where it pays** — aggregate only changed partitions when volume
  is high; full-recompute small marts for simplicity/consistency.
- **Serving concerns** — liquid clustering on common filter columns; materialize
  what dashboards hit; keep semantics stable (renaming a Gold column breaks BI).
- **Streaming to BI** — Gold is where you push to a serving layer / Power BI for
  end-to-end streaming (see [realtime to Power BI](04-realtime-to-powerbi.md)).

## Keeping it metadata-driven

The same philosophy extends up: a Silver/Gold build spec (source bronze tables,
grain, keys, SCD choice, DQ rules) can live in metadata so new marts are config,
not new pipelines — the natural next iteration of this framework.

**Related:** [SCD1/2](../ComponentWiseConceptToLearn/03-scd1-vs-scd2.md),
[apply_changes](../ComponentWiseConceptToLearn/07-dlt-apply-changes.md).
