# Auto-optimize overhead (→ liquid clustering AUTO)

**Problem.** We had **auto-optimize** (optimizeWrite + auto-compaction) enabled on
the streaming tables. It was doing **too much optimize** — compaction ran
constantly on every small micro-batch, burning compute and adding write
amplification, especially under the high-frequency CDC load.

**Cause.** Auto-compaction reacts to *every* commit. With streaming CDC producing
frequent small commits, it kept re-compacting recently written files — a lot of
work for marginal gain, and it competes with ingestion for compute.

**What we changed — liquid clustering AUTO.** We rely on **liquid clustering with
`AUTO`** on the streaming targets instead of aggressive auto-compaction:

- Clustering keys are chosen/maintained automatically (`CLUSTER BY AUTO`) — no
  manual `ZORDER`, no partition design.
- File organization is handled as part of clustering rather than a separate
  compaction storm on every commit.
- Layout stays query-efficient **without** constant re-optimize churn.

**Levers / lessons.**
- **Don't blindly enable auto-compaction on high-frequency streaming tables** —
  the commit rate makes it over-fire. Measure before turning it on.
- Prefer **liquid clustering (AUTO)** for CDC/streaming targets: good layout,
  far less optimize overhead, no manual tuning.
- If you *do* need periodic compaction, run it **scheduled/off-peak**, not
  reactively on every micro-batch.

**In the framework.** `platform_config.liquid_cluster_auto = true` and
`dataset_config.cluster_by` (blank ⇒ AUTO) drive this — the writer creates
targets with `cluster_by_auto=True` (or explicit keys) rather than leaning on
auto-compaction.

**Related:** [high cost](high-cost.md), [small files](small-files-from-cdc.md).
