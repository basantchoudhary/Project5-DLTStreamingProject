# End-to-end latency (business wants < 1 min)

**Requirement.** Business wants a change in the source visible in UC **within
1 minute**. Where does the minute go, and what do you tune?

## The latency budget — three hops

```
   Oracle commit ──►(1)──► Qlik Replicate ──►(2)──► ADLS __ct ──►(3)──► UC table
```

| Hop | What adds latency | Lever |
|---|---|---|
| **(1) Source → Qlik** | Redo-log read + Qlik's change-processing/batch interval. | Qlik latency/commit-rate settings; log-reader mode; keep the CDC task warm. |
| **(2) Qlik → ADLS** | How often Qlik **flushes** parquet to the landing zone. | Smaller flush interval / file size ⇒ lower latency (but more small files — see [small files](small-files-from-cdc.md)). |
| **(3) ADLS → UC** | File discovery + DLT **trigger interval** + micro-batch processing. | **Event-driven** ingest (no listing delay) + a short **processing-time trigger** (e.g. 20–30s). |

## Getting under a minute

- Make hop (3) **event-driven**: file-notification Auto Loader reacts to a new
  `__ct` file in seconds instead of waiting for the next directory listing
  (see [event-driven ingestion](../ComponentWiseConceptToLearn/06-event-driven-file-ingestion.md)).
- Run the pipeline **continuous** *or* with a **short processing-time trigger**
  (e.g. `processingTime="30 seconds"`) so hop (3) is bounded
  (see [trigger & processing time](../ComponentWiseConceptToLearn/09-trigger-and-processing-time.md)).
- Push Qlik's flush interval down so hop (2) contributes seconds, not minutes.
- Budget it: e.g. (1) ~10s + (2) ~15s + (3) ~30s ≈ **~55s** — under the SLA, with
  headroom to watch.

## The tension

Sub-minute latency **fights cost**: continuous/short triggers and frequent Qlik
flushes mean more compute uptime and more small files. If the SLA is really
"a few minutes", loosen the triggers and save a lot (see [high cost](high-cost.md)).
The right answer is the *loosest* setting that still meets the number the
business actually needs.
