# apply_changes out-of-order CDC

**Problem.** A row shows the *wrong* final value, or a delete "comes back". The
change records arrived out of order (retries, parallel files, clock skew).

**Cause.** Streaming files don't guarantee arrival order. If ingestion applied
changes in arrival order, a stale UPDATE could overwrite a newer one.

**Fix — `sequence_by`.** `apply_changes` never trusts arrival order; it orders
by the `sequence_by` column (`_seq`, from Qlik's `header__change_seq`). For each
key it keeps the highest sequence. Out-of-order arrival is therefore harmless —
a late, lower-sequence record is ignored.

**Levers.**
- Ensure every ct record carries a **monotonic** change sequence. If it's weak
  or missing, ordering breaks and this guarantee is lost.
- SCD2 uses the same sequence to order the *history* — wrong sequence = wrong
  effective-dated versions, not just wrong latest.

**Related:** [full-then-incr-overlap](full-then-incr-overlap.md),
[apply_changes concept](../ComponentWiseConceptToLearn/07-dlt-apply-changes.md).
