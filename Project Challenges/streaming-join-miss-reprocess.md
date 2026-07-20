# Streaming join miss → drop (reprocess invalid next micro-batch)

**Problem.** One source needed records **filtered / enriched by looking up other
tables** before writing (a join on a related entity). Under streaming, if the
matching record wasn't present **in that micro-batch**, the join found no match
and the record was **silently dropped** — even though the referenced record
arrived moments later in a following batch.

**Cause.** A streaming micro-batch only sees what has **landed so far**. A lookup /
join against another table is evaluated against that instant's data. When the two
entities arrive **out of order across streams** (the child before its parent /
reference), the join legitimately returns "no match" — and a naive
filter-on-no-match **deletes a record that is actually valid**, just early.

```
   micro-batch N   : record A refers to B  → lookup B → NOT FOUND → A dropped ✗
   micro-batch N+1 : B arrives             → (too late, A already gone)
```

**Fix — mark invalid, reprocess next micro-batch.** Instead of dropping on a
failed lookup, we **quarantined the record as INVALID** (reason: unresolved
reference / lookup miss) and **reprocessed the invalid records on the next
micro-batch**:

```
   lookup fails → is_valid=false, reason="ref B not found" → invalid table
        │
        ▼ next micro-batch
   re-read invalid records → retry the lookup
        ├─ B now present → resolve, write through, clear from invalid ✓
        └─ still missing  → keep invalid, try again next batch (bounded)
```

- Nothing is lost — a record that was merely **early** gets resolved once its
  reference lands (see [record drop / invalid & reprocess](../ComponentWiseConceptToLearn/14-record-drop-and-invalid-records.md)).
- Reprocessing is **idempotent** by key + sequence, so retrying can't duplicate
  (see [idempotency](../ComponentWiseConceptToLearn/11-idempotency-primary-key.md)).

**Levers / watch-outs.**
- **Bound the retries** — a truly orphaned record (reference will *never* arrive)
  must age out / alert after N attempts, not loop forever.
- **Monitor the invalid backlog** — a growing quarantine means references are
  systematically missing, not just late.
- **Alternative:** a proper **stream-stream join with a watermark** can wait for
  the other side within a bounded window — but it carries state + latency cost;
  the invalid-and-reprocess pattern is simpler and self-healing for this case
  (see [late arrival](../ComponentWiseConceptToLearn/13-late-arrival-handling.md)).

**Lesson.** In streaming, "no match on a lookup" often means **"not yet"**, not
**"invalid"**. Never drop on a failed cross-entity join — **quarantine and retry**
so early-arriving records heal once their reference lands.

**Related:** [record drop / invalid & reprocess](../ComponentWiseConceptToLearn/14-record-drop-and-invalid-records.md),
[late arrival](../ComponentWiseConceptToLearn/13-late-arrival-handling.md),
[idempotency](../ComponentWiseConceptToLearn/11-idempotency-primary-key.md).
