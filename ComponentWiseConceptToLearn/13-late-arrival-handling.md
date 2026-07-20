# Late arrival handling

**What "late" means.** A change event reaches the pipeline **after** events that
are logically *newer*. Causes: Qlik retries, network delays, parallel `__ct`
files landing out of order, a backfill replaying old data.

## Why it's usually fine here

`apply_changes` does **not** trust arrival order — it orders by **`sequence_by`**
(`_seq`, the Qlik change sequence / SCN). For each primary key it keeps the row
with the **highest sequence**. So:

```
   arrival order:  U(seq=5002)  then late  U(seq=5001)
   result:         seq=5002 wins; the late, lower-seq row is ignored
```

A late event that is genuinely **older** is correctly discarded; a late event
that is genuinely **newer** is correctly applied. Correct sequencing is what
makes lateness a non-event.

## Where you must think about watermarks

Plain `apply_changes` (upsert by key) doesn't need a watermark. But **stateful
event-time** operations — windowed aggregates, stream-stream joins, dedup over
event time (typically in Silver/Gold) — must bound how long to wait for late
data using a **watermark**:

```python
df.withWatermark("event_ts", "15 minutes")   # accept up to 15 min late
```

- Data later than the watermark is **dropped** from that stateful op (a
  deliberate trade: bounded state vs. accepting stragglers).
- Set it from the business tolerance for lateness, not arbitrarily.

## Levers

- Guarantee a **monotonic `sequence_by`** on every ct record — the whole late
  -arrival guarantee rests on it (see [ordering](../Project%20Challenges/apply-changes-ordering.md)).
- For downstream aggregations, size the **watermark** to real-world lateness.
- For a big historical **backfill**, replay in sequence order so old data can't
  overwrite new (keys + seq protect you, but ordered replay is cleaner).

**Related:** [apply_changes](07-dlt-apply-changes.md),
[Spark streaming (watermark)](04-spark-structured-streaming.md),
[ordering challenge](../Project%20Challenges/apply-changes-ordering.md).
