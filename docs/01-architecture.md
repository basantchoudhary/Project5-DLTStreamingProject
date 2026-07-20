# Architecture — metadata-driven DLT streaming ingestion

Oracle change data, replicated by **Qlik Replicate** to ADLS Parquet, ingested by
**Auto Loader**, and materialized into **UC streaming tables** as SCD1 and SCD2 via
**DLT `apply_changes`** — all driven by a metadata database so onboarding a table is a
config row, not code.

---

## 1 · End-to-end flow

```
   ┌────────┐ redo  ┌──────────────────────────┐ snappy  ┌──────────────────────────────┐
   │ ORACLE │ logs  │ Qlik Replicate (Azure VM)│ parquet │ ADLS Gen2 (landing)          │
   │ (OLTP) │──────►│ copy redolog → extract → │────────►│  <source>/<table>__full  ▪▪▪ │
   └────────┘       │ zip parquet (snappy)     │         │  <source>/<table>__ct    ▪▪▪ │
     ▲ SomeApp      └──────────────────────────┘         └───────────────┬──────────────┘
                                                                │ Auto Loader (cloudFiles)
                                                                │ __full seed flow + __ct flow
                        ┌───────────────────────────────────────▼───────────────────────┐
                        │  DLT PIPELINE  (one per table_group_no; ~20 tables each)        │
                        │                                                                 │
                        │   Controller ──loop──► SingleTableController (per table)        │
                        │                          ├─ SingleTableProcessor  (parse CDC)   │
                        │                          └─ SingleTableWriter (dlt.apply_changes)│
                        └───────────────────────────────┬───────────────────────────────┘
                                        ┌───────────────┴───────────────┐
                                        ▼                               ▼
                          UC  bronze_scd1.<table>            UC  bronze_scd2.<table>
                          (SCD1 streaming table,             (SCD2 streaming table,
                           liquid clustering AUTO)            liquid clustering AUTO)
```

---

## 2 · Why this shape

```
   METADATA-DRIVEN   onboard a table = insert a config row (no new code).
   DLT apply_changes  CDC/SCD handled natively (Type 1 & Type 2) — no hand-rolled MERGE.
   Auto Loader        exactly-once file ingestion, schema evolution, checkpointed.
   table_group_no     200 tables / 20 pipelines → ~10 tables each = parallelism +
                      blast-radius isolation (one group's failure ≠ all 200).
   streaming tables   continuous/triggered incremental; liquid clustering AUTO = no
                      manual partition/Z-order tuning.
```

---

## 3 · Components (each a small, single-responsibility unit)

```
   meta_loader              reads the 5 metadata tables → Metadata object:
                              • datasets: dict[dataset_id → Dataset]
                              • config:   dict[key → value]  (platform)
                              • sources:  dict[source_id → master+config]

   Controller               given a table_group_no, loops the group's datasets,
                            calling SingleTableController for each.

   SingleTableController    for one table: picks full vs incr path, wires the
                            processor → writer as DLT graph nodes.

   SingleTableProcessor     builds the Auto Loader read stream; standardizes the
                            Qlik CDC operation into `_op` (I/U/D) + `_seq` (ordering).

   SingleTableWriter        creates the SCD1/SCD2 streaming target(s) with liquid
                            clustering AUTO and calls dlt.apply_changes.

   create_dlt_pipelines     provisions the 10 DLT pipelines, each parameterized with
                            its table_group_no, all pointing at the same entry notebook.
```

---

## 4 · Full load vs incremental (per table)

```
   dataset_config.load_type = 'full'   → Auto Loader reads  <table>/full/   (seed/reload)
   dataset_config.load_type = 'incr'   → Auto Loader reads  <table>/cdc/     (ongoing CDC)

   Typical lifecycle:  onboard with 'full' (seed the target) → switch to 'incr'.
   apply_changes de-dups by keys + sequence_by, so a full-then-incr overlap is safe.
```

---

## 5 · SCD1 vs SCD2 targets

```
   scd_type = '1'    → bronze_scd1.<table>   (latest row per key; deletes remove it)
   scd_type = '2'    → bronze_scd2.<table>   (version history; deletes close the row)
   scd_type = 'both' → write to BOTH schemas from the same source stream

   Both are STREAMING TABLES created via dlt.create_streaming_table(..., cluster_by_auto=True).
   apply_changes(stored_as_scd_type=1|2) does the CDC merge; apply_as_deletes handles 'D'.
```

---

## 6 · Pipeline fan-out (table_group_no)

```
   200 tables ──assign table_group_no 1..20 in dataset_master──►

     Pipeline g=1  ─ processes ~10 tables (group 1)   ┐
     Pipeline g=2  ─ processes ~10 tables (group 2)   │  20 pipelines run independently
     ...                                              │  (isolation + parallelism)
     Pipeline g=20 ─ processes ~10 tables (group 20)  ┘

   Same entry notebook (pipelines/dlt_entry.py); each pipeline just gets a different
   `pipeline.source` / `pipeline.table_group_no` / `pipeline.dataset_list`.
```

*Next: [`02-metadata-model.md`](02-metadata-model.md).*
