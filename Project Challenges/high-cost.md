# High cost

**Problem.** The DLT bill is higher than expected — 20 always-on pipelines, lots
of small-file churn, continuous compute.

**Cause.** Streaming cost is dominated by **compute uptime** and **wasted work**
(re-listing files, tiny writes, over-frequent triggers, idle clusters).

## Cost levers (cheapest wins first)

| Lever | What it does |
|---|---|
| **Triggered, not continuous** | Run each pipeline on a schedule / `AvailableNow` instead of 24×7. For "every few minutes" SLAs this cuts idle compute massively. |
| **Serverless DLT** | No idle cluster to pay for between micro-batches; scales to the work. |
| **Right-size the trigger interval** | Bigger micro-batches = fewer runs, fewer/larger files, less per-batch overhead (trade latency). |
| **File-notification ingest** | Event-driven Auto Loader avoids expensive directory **listing** at scale — you pay per event, not per re-scan. |
| **Group sizing** | 20 pipelines is isolation, but each has fixed overhead. Don't over-split; ~10 tables/group balances isolation vs pipeline count. |
| **Auto-compaction + liquid clustering** | Fewer small files ⇒ cheaper reads downstream and less write amplification. |
| **SCD2 only where needed** | History isn't free — `scd_type` per table so you don't store versions for tables that only need latest state. |
| **Prune the graph** | Drop the `__full` seed flow after onboarding (`load_type=incr`) so you're not scanning the snapshot forever. |

**Rule of thumb.** For CDC ingestion the biggest single lever is almost always
**uptime**: triggered + serverless beats always-on continuous for anything looser
than sub-minute latency. Match the trigger to the SLA, not to "as fast as possible".

## How we watch cost — cost observability dashboard

You can't tune what you can't see. We ran a **cost observability dashboard** to
attribute spend and catch regressions:

- Built on the **system tables** — `system.billing.usage` (DBU consumption) joined
  to `system.billing.list_prices`, sliced by pipeline / job / cluster / tag.
- **Tag** each pipeline (source, table_group_no) so cost is attributable **per
  group**, not just a lump sum — you can see which of the 20 pipelines is
  expensive.
- Watch **trend and outliers**: a pipeline whose DBUs jump overnight = a
  regression (a [redo flood](redo-log-flood-from-maintenance.md), a
  [full-scan MERGE](liquid-clustering-not-working.md), an
  [autoscale that won't come down](autoscale-not-scaling-down.md)).
- Pair with the **DLT event log** and Spark UI for the *why* once the dashboard
  shows the *where*.

The dashboard turns cost from a monthly surprise into a signal you act on.

**Related:** [Small files](small-files-from-cdc.md),
[End-to-end latency](end-to-end-latency.md),
[Blast radius / groups](blast-radius-and-groups.md),
[Autoscale won't scale down](autoscale-not-scaling-down.md),
[Heavy joins](streaming-join-performance-cost.md).
