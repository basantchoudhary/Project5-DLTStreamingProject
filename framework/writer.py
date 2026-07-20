"""writer — creates the SCD1/SCD2 streaming targets and runs dlt.apply_changes.

One processed source view feeds one or both SCD targets. apply_changes handles
the CDC merge natively (Type 1 = latest row per key; Type 2 = version history);
deletes are driven by _op = 'D'. The framework columns (_op/_seq/_ingest_*) are
excluded from the stored target.
"""

# Framework-internal columns that should not land in the target tables.
_FRAMEWORK_COLS = ["_op", "_seq", "_ingest_source", "_ingest_file", "_ingest_ts"]


def write_scd_targets(derived, source_view):
    """Create each SCD target streaming table and apply_changes into it.

    Args:
        derived:     build_derived_config() output for this table.
        source_view: name of the processed streaming view/table to read from.
    """
    import dlt
    from pyspark.sql import functions as F

    keys = derived["primary_keys"]
    op_col = derived["op_col"]
    seq_col = derived["seq_col"]

    # columns to drop from the target = framework cols + raw Qlik cdc header cols
    except_cols = list(dict.fromkeys(_FRAMEWORK_COLS + [op_col, seq_col]))

    for scd_int, target_name in derived["scd_targets"]:
        # --- create the streaming target with liquid clustering -----------------
        create_kwargs = {"name": target_name}
        if derived["cluster_by"]:
            create_kwargs["cluster_by"] = derived["cluster_by"]
        elif derived["cluster_auto"]:
            create_kwargs["cluster_by_auto"] = True
        dlt.create_streaming_table(**create_kwargs)

        # --- apply the CDC merge ------------------------------------------------
        dlt.apply_changes(
            target=target_name,
            source=source_view,
            keys=keys,
            sequence_by=F.col("_seq"),
            apply_as_deletes=F.expr("_op = 'D'"),
            except_column_list=except_cols,
            stored_as_scd_type=scd_int,
        )
