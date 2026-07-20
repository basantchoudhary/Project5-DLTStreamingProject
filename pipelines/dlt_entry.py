# Databricks notebook source
# =============================================================================
# DLT ENTRY — the single notebook every one of the 20 pipelines points at.
#
# Each pipeline supplies its own parameters via the DLT pipeline configuration
# (Spark conf):
#     pipeline.source          e.g. ORA_SALES
#     pipeline.table_group_no  1 .. 20
#     pipeline.dataset_list    ALL  (or a comma-separated list of dataset_ids)
#
# The notebook just reads those and hands off to DLTMainController.run(), which
# queries meta_loader and builds the graph for that group's ~10 tables.
# =============================================================================

from pyspark.sql import SparkSession

from framework.dlt_main_controller import run

spark = SparkSession.getActiveSession()


def _conf(key, default=None):
    try:
        return spark.conf.get(key)
    except Exception:
        return default


source = _conf("pipeline.source")
table_group_no = _conf("pipeline.table_group_no")
dataset_list_raw = _conf("pipeline.dataset_list", "ALL")

# "ALL" stays a string; anything else becomes a list of dataset_ids
if dataset_list_raw and dataset_list_raw.strip().upper() != "ALL":
    dataset_list = [d.strip() for d in dataset_list_raw.split(",") if d.strip()]
else:
    dataset_list = "ALL"

# Building the graph = defining DLT tables/flows for every dataset in the group.
run(spark, source=source, table_group_no=table_group_no, dataset_list=dataset_list)
