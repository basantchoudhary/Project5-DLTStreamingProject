"""Metadata-driven DLT streaming ingestion framework."""

from framework.meta_loader import (
    load_dataset_metadata,
    load_platform_config,
    load_source_metadata,
)

__all__ = [
    "load_dataset_metadata",
    "load_platform_config",
    "load_source_metadata",
]
