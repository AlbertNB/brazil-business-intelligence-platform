# Databricks Asset Bundle — Brazil Business Intelligence Platform

This folder contains the [Databricks Asset Bundle (DAB)](https://docs.databricks.com/dev-tools/bundles/index.html) configuration for deploying and running ingestion jobs on Databricks.

## Structure

```
databricks.yml   # Bundle root — declares variables and includes jobs/targets
ingestion/       # PySpark ingestion scripts
jobs/            # Job definitions
targets/         # Per-environment configuration (workspace host + variable values)
```

## Prerequisites

Install the Databricks CLI:

```bash
brew install databricks/tap/databricks  # macOS
```

Or via pip:

```bash
pip install databricks-cli
```

Authenticate against the target workspace:

```bash
databricks auth login --host https://<workspace-url>
```

## Variables

Variables are declared in `databricks.yml` and resolved per target:

| Variable | Description |
|---|---|
| `LANDING_ROOT` | S3 path where raw source files are dropped |
| `AUTOLOADER_ROOT` | S3 path for Auto Loader schema and checkpoint metadata |
| `BRONZE_ROOT` | S3 path where Delta Bronze tables are written |

## Deploy

```bash
databricks bundle deploy -t prod
```

## Run

```bash
databricks bundle run -t prod <job_name>
```

## How It Works

The `bronze_ingestion.py` script uses Databricks Auto Loader (`cloudFiles`) to incrementally ingest JSON files from the landing zone into Delta Bronze tables under Unity Catalog.

For each stream (subfolder) discovered under `{LANDING_ROOT}/{source}/`, it:
1. Reads new JSON files using `trigger(availableNow=True)`
2. Adds `_ingestion_ts` and `_source_file` metadata columns
3. Writes to `{BRONZE_ROOT}/{source}/{table_name}/` in Delta format
4. Registers the table in Unity Catalog as `{catalog}.{schema}.{source}__{stream}`
