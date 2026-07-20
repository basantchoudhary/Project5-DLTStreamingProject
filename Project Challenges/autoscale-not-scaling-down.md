# Autoscale scaled up but wouldn't scale down

**Problem.** Under load the cluster **autoscaled up** — good — but afterwards it
**wouldn't scale back down**. It sat at high worker counts long after the burst
passed, so we kept **paying for capacity we no longer used**. Scale-up worked;
scale-down didn't.

**Cause.** Scaling *down* a streaming cluster is genuinely harder than scaling up,
and several things block it:
- **Streaming holds executors.** A long-running streaming query keeps tasks,
  **shuffle data, and state** on executors. Spark won't remove an executor that
  still holds needed shuffle/state, so downscale stalls.
- **Continuous mode never idles.** In continuous streaming the workers stay busy
  enough that the autoscaler never sees them idle long enough to release.
- **Cached/persisted data** pins executors (cached RDDs/DataFrames block removal).
- **`min_workers` set too high** — it literally can't go below the floor you set.
- **Skew** — a few long tasks keep some executors busy while the rest idle,
  preventing a clean downscale.

**Fixes / levers.**
- **Triggered instead of continuous** (`AvailableNow` / DLT triggered): the cluster
  processes what's available and can **release / terminate between runs** — the
  single biggest scale-down lever. (See
  [trigger & processing time](../ComponentWiseConceptToLearn/09-trigger-and-processing-time.md).)
- **DLT enhanced autoscaling** — designed to scale streaming workloads **down**
  more aggressively than classic autoscaling; prefer it for pipelines.
- **Right-size `min_workers`** — lower the floor so the cluster *can* shrink; set a
  sensible `max` for the burst.
- **Drop unnecessary caching** that pins executors.
- **Fix skew** so tasks finish and executors go idle enough to be reclaimed.
- **Serverless** sidesteps much of this — no long-lived cluster to fail to shrink.

**How we noticed.** The [cost observability dashboard](high-cost.md) showed DBUs
staying high after load dropped — the tell-tale of a cluster that scaled up and
never came back down.

**Lesson.** **Scale-up is easy; scale-down is the hard part** of streaming
autoscaling, because state/shuffle pin executors. Don't assume "autoscaling on"
means "cost follows load" — verify it actually shrinks, and lean on **triggered
mode + enhanced autoscaling + a low min-workers floor** to make down-scaling real.

**Related:** [high cost](high-cost.md),
[trigger & processing time](../ComponentWiseConceptToLearn/09-trigger-and-processing-time.md),
[heavy joins](streaming-join-performance-cost.md).
