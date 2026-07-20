# Liquid clustering AUTO not working → MERGE full scan

**Problem.** For some tables, liquid clustering **AUTO** wasn't actually
clustering the data. With no useful clustering, `apply_changes` / MERGE couldn't
**skip files** and fell back to a **full table scan** on every micro-batch —
slow merges, high compute, latency creeping up.

**Cause.** `CLUSTER BY AUTO` is not instant or guaranteed per commit:
- Clustering runs **asynchronously** (predictive optimization / background
  OPTIMIZE), so on high-churn streaming tables the data can stay **poorly
  clustered** for a while — or AUTO may not have chosen the **merge/join key** as
  a clustering key at all.
- MERGE prunes files using min/max stats on the **join key**. If the table isn't
  clustered on that key, there's nothing to skip → it reads **everything**.
- In some cases clustering simply **wasn't running** on the table (optimization
  not firing), so it never improved.

**How we caught it — `DESCRIBE HISTORY`.** The table's history is the truth about
what maintenance actually ran:

```sql
DESCRIBE HISTORY bronze_scd1.orders;
-- look for OPTIMIZE / CLUSTERING operations and their metrics.
-- No clustering entries (or no recent ones) = AUTO isn't doing its job.
-- MERGE rows with huge numTargetFilesScanned / full-scan metrics = no pruning.
```

Reading `operation` + `operationMetrics` in the history showed clustering wasn't
happening (or hadn't clustered on the key MERGE needed), which explained the
full scans.

**Fix.**
- **Verify with `DESCRIBE HISTORY`** that clustering/OPTIMIZE actually runs and
  that MERGE is pruning (file-scan metrics drop) — don't assume AUTO worked.
- **Cluster on the merge key.** Where AUTO didn't pick it, set **explicit
  clustering keys** (`CLUSTER BY <primary/merge key>`) so MERGE can skip files.
  In the framework this is `dataset_config.cluster_by` (blank ⇒ AUTO) — for these
  tables we set it explicitly to the key `apply_changes` merges on.
- **Make sure optimization is enabled/running** (predictive optimization or a
  scheduled `OPTIMIZE`) so clustering keeps up with the write rate.

**Lesson.** `CLUSTER BY AUTO` is a default, not a guarantee — on hot streaming
tables it can lag or miss the key that matters. **MERGE only prunes if the table
is clustered on the join key.** Verify with `DESCRIBE HISTORY`, and pin explicit
clustering keys on the merge key when AUTO isn't cutting it.

**Related:** [auto-optimize overhead](auto-optimize-overhead.md),
[apply_changes](../ComponentWiseConceptToLearn/07-dlt-apply-changes.md),
[high cost](high-cost.md).
