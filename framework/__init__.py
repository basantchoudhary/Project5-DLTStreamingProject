"""Metadata-driven DLT streaming ingestion framework."""

from framework.meta_loader import (
    load_dataset_metadata,
    load_platform_config,
    load_source_metadata,
)
from framework.derived_config import build_derived_config
from framework.reader import build_autoloader_stream
from framework.processor import standardize_cdc
from framework.writer import write_scd_targets
from framework.logger import log_event
from framework.dlt_single_table_controller import build_single_table
from framework.dlt_main_controller import run

__all__ = [
    "load_dataset_metadata",
    "load_platform_config",
    "load_source_metadata",
    "build_derived_config",
    "build_autoloader_stream",
    "standardize_cdc",
    "write_scd_targets",
    "log_event",
    "build_single_table",
    "run",
]
