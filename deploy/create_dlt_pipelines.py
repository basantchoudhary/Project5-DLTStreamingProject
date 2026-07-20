"""create_dlt_pipelines — provision the N DLT pipelines (one per table_group_no).

All pipelines share ONE entry notebook (pipelines/dlt_entry.py); they differ only
by the configuration values that entry reads:
    pipeline.source          e.g. ORA_SALES
    pipeline.table_group_no  1 .. 20
    pipeline.dataset_list    ALL (default) or a comma-separated dataset_id list

Create-or-update by pipeline name, so re-running is idempotent (edits in place).

Groups can be given explicitly (--groups 1-20) or discovered from metadata
(--from-metadata, reads distinct table_group_no from dataset_master via a SQL
warehouse) — the metadata-driven path means adding group 21 later needs no code
change here.

Requires: pip install databricks-sdk   (auth via env / CLI profile / DEFAULT).

Examples:
    python deploy/create_dlt_pipelines.py --source ORA_SALES --groups 1-20 \
        --entry /Repos/basant/Project5-DLTStreamingProject/pipelines/dlt_entry \
        --catalog bronze --dry-run

    python deploy/create_dlt_pipelines.py --source ORA_SALES --from-metadata \
        --warehouse-id 0123456789abcdef --entry /Repos/.../pipelines/dlt_entry
"""

import argparse
import sys


# ---- group parsing ---------------------------------------------------------
def parse_groups(spec):
    """'1-20' or '1,3,5' or '1-5,9' -> sorted list of ints."""
    groups = set()
    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            lo, hi = part.split("-", 1)
            groups.update(range(int(lo), int(hi) + 1))
        else:
            groups.add(int(part))
    return sorted(groups)


def discover_groups_from_metadata(w, warehouse_id, source, meta_catalog, meta_schema):
    """Return distinct enabled table_group_no for `source` from dataset_master."""
    from databricks.sdk.service.sql import StatementState

    sql = (
        f"SELECT DISTINCT table_group_no "
        f"FROM {meta_catalog}.{meta_schema}.dataset_master "
        f"WHERE source_id = '{source}' AND enabled = true "
        f"ORDER BY table_group_no"
    )
    resp = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id, statement=sql, wait_timeout="30s"
    )
    if resp.status and resp.status.state != StatementState.SUCCEEDED:
        raise RuntimeError(f"metadata query failed: {resp.status}")
    rows = (resp.result.data_array if resp.result else None) or []
    return sorted(int(r[0]) for r in rows)


# ---- pipeline spec ---------------------------------------------------------
def pipeline_name(source, group):
    return f"ingest_{source.lower()}_g{group:02d}"


def build_pipeline_kwargs(source, group, entry_notebook, catalog, target_schema,
                          dataset_list, root_path, continuous, serverless):
    """kwargs shared by pipelines.create() and pipelines.update()."""
    from databricks.sdk.service.pipelines import (
        NotebookLibrary,
        PipelineLibrary,
    )

    kwargs = dict(
        name=pipeline_name(source, group),
        libraries=[PipelineLibrary(notebook=NotebookLibrary(path=entry_notebook))],
        configuration={
            "pipeline.source": source,
            "pipeline.table_group_no": str(group),
            "pipeline.dataset_list": dataset_list,
        },
        catalog=catalog,          # UC catalog; tables use 3-part names to hit scd1/scd2 schemas
        target=target_schema,     # default publish schema (tables override with full names)
        serverless=serverless,
        continuous=continuous,    # False = triggered (run on demand / schedule)
        development=False,
        photon=True,
        channel="CURRENT",
    )
    if root_path:                 # so `from framework import ...` resolves at repo root
        kwargs["root_path"] = root_path
    return kwargs


def create_or_update(w, kwargs, dry_run):
    """Create the pipeline, or update it if one with the same name exists."""
    name = kwargs["name"]
    existing = {p.name: p.pipeline_id for p in w.pipelines.list_pipelines()}

    if dry_run:
        action = "UPDATE" if name in existing else "CREATE"
        cfg = kwargs["configuration"]
        print(f"  [dry-run] {action} {name}  "
              f"(source={cfg['pipeline.source']} group={cfg['pipeline.table_group_no']})")
        return None

    if name in existing:
        pid = existing[name]
        w.pipelines.update(pipeline_id=pid, **kwargs)
        print(f"  updated {name}  ({pid})")
        return pid

    created = w.pipelines.create(**kwargs)
    print(f"  created {name}  ({created.pipeline_id})")
    return created.pipeline_id


# ---- main ------------------------------------------------------------------
def main(argv=None):
    ap = argparse.ArgumentParser(description="Provision the DLT ingestion pipelines.")
    ap.add_argument("--source", required=True, help="source_id, e.g. ORA_SALES")
    ap.add_argument("--groups", help="explicit groups, e.g. '1-20' or '1,3,5'")
    ap.add_argument("--from-metadata", action="store_true",
                    help="discover groups from dataset_master (needs --warehouse-id)")
    ap.add_argument("--warehouse-id", help="SQL warehouse id for --from-metadata")
    ap.add_argument("--entry", required=True,
                    help="workspace path of pipelines/dlt_entry (no .py)")
    ap.add_argument("--root-path", help="repo root so `framework` is importable")
    ap.add_argument("--catalog", default="bronze", help="UC target catalog")
    ap.add_argument("--target-schema", default="bronze_scd1", help="default publish schema")
    ap.add_argument("--dataset-list", default="ALL")
    ap.add_argument("--meta-catalog", default="ingest_meta")
    ap.add_argument("--meta-schema", default="control")
    ap.add_argument("--continuous", action="store_true", help="continuous (default: triggered)")
    ap.add_argument("--classic-compute", action="store_true", help="disable serverless")
    ap.add_argument("--dry-run", action="store_true", help="print the plan, change nothing")
    args = ap.parse_args(argv)

    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()

    # resolve the group list
    if args.from_metadata:
        if not args.warehouse_id:
            ap.error("--from-metadata requires --warehouse-id")
        groups = discover_groups_from_metadata(
            w, args.warehouse_id, args.source, args.meta_catalog, args.meta_schema)
    elif args.groups:
        groups = parse_groups(args.groups)
    else:
        ap.error("provide --groups or --from-metadata")

    if not groups:
        print(f"no groups found for source={args.source}; nothing to do")
        return 0

    print(f"provisioning {len(groups)} pipeline(s) for source={args.source}: {groups}")
    for group in groups:
        kwargs = build_pipeline_kwargs(
            source=args.source, group=group, entry_notebook=args.entry,
            catalog=args.catalog, target_schema=args.target_schema,
            dataset_list=args.dataset_list, root_path=args.root_path,
            continuous=args.continuous, serverless=not args.classic_compute,
        )
        create_or_update(w, kwargs, args.dry_run)

    print("done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
