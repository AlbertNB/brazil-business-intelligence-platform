# Lakehouse Layer Architecture

This project follows a multi-layer lakehouse architecture designed to provide clear separation of responsibilities across data ingestion, processing, normalization, and analytical consumption.

---

# Landing

The Landing layer is the external arrival area for raw source data before ingestion into the lakehouse.

This layer is implemented using Amazon S3 and exists outside the managed lakehouse tables.

## Purpose

- Receive raw extracted files from source systems
- Preserve source delivery structure
- Decouple extraction from ingestion
- Serve as the ingestion source for Auto Loader pipelines

## Characteristics

- Raw, immutable source files
- No transformations applied
- Organized using source-driven partitioning conventions
- Supports batch snapshots and API extractions

---

# Bronze

The Bronze layer is the first managed layer inside the lakehouse and is populated through Databricks Auto Loader ingestion pipelines.

Bronze preserves the raw source structure while materializing ingestion metadata required for traceability and debugging.

## Purpose

- Ingest Landing data into Delta tables
- Preserve raw source fidelity
- Enable schema evolution and rescued data handling
- Provide traceable and reproducible ingestion

## Characteristics

- Minimal transformations
- Source-oriented structure
- Raw operational representation
- Auto Loader managed ingestion

## Common Operations

- File ingestion
- Schema inference/evolution
- Raw payload preservation
- Ingestion metadata generation

---

# Silver

The Silver layer contains cleaned, normalized, typed, and domain-aligned datasets derived from Bronze.

This layer prepares operational datasets for analytical modeling and downstream consumption.

## Purpose

- Normalize raw source structures
- Apply typing and data quality rules
- Split operational datasets into logical domain models
- Prepare curated datasets for Gold consumption

## Characteristics

- Cleaned and typed data
- Domain-aligned structures
- Incremental or full-refresh processing
- Operationally aligned models

## Common Operations

- Type casting
- Data validation
- Deduplication
- Normalization
- Incremental merge processing
- Full-refresh transformations
- Dataset decomposition

---

# Gold

The Gold layer contains business-oriented analytical models optimized for reporting, BI, and downstream consumption.

This layer follows a star schema modeling approach using dimensions and facts.

## Purpose

- Provide analytical-ready datasets
- Support dashboards and BI workloads
- Expose business-friendly models
- Centralize metrics and dimensional modeling

## Characteristics

- Business-oriented structure
- Star schema modeling
- Dimensions and facts
- Optimized for analytical consumption
- Stable semantic layer

## Common Operations

- Dimensional modeling
- Business rule application
- Metric calculation
- Analytical aggregations