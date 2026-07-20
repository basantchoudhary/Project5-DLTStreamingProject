# Technology alternatives (why these choices)

Every box in the architecture had alternatives. Knowing them (and the trade-offs)
is how you defend the design in a review.

## Alternatives to Qlik Replicate (log-based CDC)

| Option | Notes |
|---|---|
| **Qlik Replicate** (chosen) | Mature log-based CDC; broad source support; GUI-driven; commercial license. |
| **Oracle GoldenGate** | Oracle's own CDC; deep Oracle integration; powerful but heavy/licensed. |
| **Debezium** (+ Kafka) | Open-source log-based CDC; very popular; you operate Kafka Connect. |
| **AWS DMS / Azure DMS** | Managed migration/replication; cheaper; less rich transformation. |
| **Databricks Lakeflow Connect** | Native Databricks ingestion connectors (incl. some DB CDC) — fewer moving parts if it covers your source. |
| **Oracle LogMiner (DIY)** | Read redo logs yourself — max control, max effort; rarely worth it. |

**Why Qlik here:** log-based (low source impact), handles Oracle redo well,
lands clean parquet, operationally proven for many tables.

## Alternatives to ADLS Gen2 (landing store)

| Option | Notes |
|---|---|
| **ADLS Gen2** (chosen) | Azure-native; hierarchical namespace; Event Grid file events; UC external locations. |
| **AWS S3** | Equivalent on AWS; S3 event notifications for Auto Loader. |
| **Google Cloud Storage** | Equivalent on GCP; Pub/Sub notifications. |
| **Managed Volumes / UC-managed storage** | Governance-first, but landing raw CDC usually wants a plain object store. |

**Why ADLS here:** the platform is Azure; ADLS + Event Grid gives clean
**event-driven** ingestion (see [06](06-event-driven-file-ingestion.md)), and UC
external locations manage the file-event plumbing.

## Alternatives to DLT (processing)

Raw Structured Streaming, Spark + custom MERGE, or **Apache Flink** for
event-at-a-time streaming. See [Why DLT](15-why-dlt-vs-spark-streaming.md) for the
full trade-off — short version: DLT removes the CDC/SCD/ops boilerplate at the
cost of Databricks lock-in.

## Alternatives to Databricks/UC (lakehouse)

Snowflake (+ Streams/Tasks for CDC), Azure Synapse, BigQuery. The medallion +
Delta + UC governance story is why Databricks fits this log-CDC-to-lakehouse
shape well.

**Takeaway.** None of these are "wrong" — the choices optimize for **Azure +
Oracle + many tables + lakehouse governance**. Change any of those constraints and
a different box might win.
