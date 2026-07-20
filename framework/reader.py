"""reader — readerforthistable. Builds an Auto Loader (cloudFiles) read stream.

Used for both the full-load path and the ct (change-table) path; the caller
passes the right path + schema location. Adds ingest lineage columns so the
processor/writer can see where each record came from.
"""


def build_autoloader_stream(spark, path, schema_location, file_format="parquet",
                            source_kind="ct", extra_options=None):
    """Return a streaming DataFrame reading `path` via Auto Loader.

    Args:
        spark:           SparkSession.
        path:            ADLS folder (e.g. .../ORDERS__ct).
        schema_location: cloudFiles.schemaLocation for inference/evolution state.
        file_format:     cloudFiles.format (parquet for Qlik snappy parquet).
        source_kind:     'full' or 'ct' — recorded in _ingest_source for lineage.
        extra_options:   optional dict of extra cloudFiles/reader options.
    """
    from pyspark.sql import functions as F

    reader = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", file_format)
        .option("cloudFiles.schemaLocation", schema_location)
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    )
    for k, v in (extra_options or {}).items():
        reader = reader.option(k, v)

    return (
        reader.load(path)
        .withColumn("_ingest_source", F.lit(source_kind))
        .withColumn("_ingest_file", F.col("_metadata.file_path"))
        .withColumn("_ingest_ts", F.current_timestamp())
    )
