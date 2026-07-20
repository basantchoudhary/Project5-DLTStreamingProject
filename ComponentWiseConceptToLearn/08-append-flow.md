# append_flow

**What it is.** `@dlt.append_flow` lets **multiple streaming sources append into
one target streaming table**. Each flow is an independent stream with its own
checkpoint, but they all write to the same table.

```python
dlt.create_streaming_table("orders_raw")

@dlt.append_flow(target="orders_raw", name="orders_raw_full_flow")
def full():   return read_autoloader(".../ORDERS__full")

@dlt.append_flow(target="orders_raw", name="orders_raw_ct_flow")
def ct():     return read_autoloader(".../ORDERS__ct")
```

## Why we need it here

A table has **two** landing sources — `__full` (seed) and `__ct` (ongoing CDC) —
that must become **one** logical change stream before `apply_changes`. Options:

- ❌ **Union two streams** into one query — brittle for streaming, and you can't
  independently start/stop or checkpoint the two sources.
- ✅ **Two append_flows into one `_raw` table** — each source streams at its own
  pace with its own checkpoint; the target is a single stream the processor and
  writer read once.

## Benefits

- **Independent flows:** drop the full flow after seeding (`load_type=incr`)
  *without* touching the ct flow or losing state.
- **Separate checkpoints:** a backfill flow and the live flow don't interfere.
- **One downstream:** processor + `apply_changes` see a single `orders_raw` stream.

```
   __full ─(append_flow)─┐
                          ├─►  orders_raw  ─► processed view ─► apply_changes
   __ct   ─(append_flow)─┘
```

**In this project.** `dlt_single_table_controller.py` creates `<table>_raw` and
attaches the full (optional) + ct append_flows; `include_full_flow` comes from
`load_type`.

**Related:** [apply_changes](07-dlt-apply-changes.md),
[full → incr overlap](../Project%20Challenges/full-then-incr-overlap.md).
