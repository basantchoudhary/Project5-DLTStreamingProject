# Small files from CDC

**Problem.** Qlik writes a *lot* of tiny parquet files to `__ct` (one per
micro-batch of changes). Ingestion gets slow and listing-heavy; the target
accumulates small files too.

**Cause.** High-frequency CDC + frequent flushes = many small objects. Both the
read side (file discovery) and the write side (many small target files) suffer.

**Fix — attack both sides.**
- **Read side:** prefer **file-notification / event-driven** ingestion over
  directory listing so Auto Loader isn't re-listing millions of files
  (see [event-driven ingestion](../ComponentWiseConceptToLearn/06-event-driven-file-ingestion.md)).
- **Write side:** let DLT **auto-optimize / auto-compact** streaming tables, and
  use **liquid clustering** so files stay well-sized without manual OPTIMIZE.
- Right-size the **trigger interval** — larger micro-batches = fewer, bigger
  files (trade latency for file size; see
  [trigger & processing time](../ComponentWiseConceptToLearn/09-trigger-and-processing-time.md)).

**Rule of thumb.** Small files are a *throughput and cost* problem long before
they're a correctness problem — fix them before they compound.

**Related:** [High cost](high-cost.md).
