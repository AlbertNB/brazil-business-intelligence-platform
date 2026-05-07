# Naming Conventions

## Table Naming by Layer

Use the following naming conventions for models and physical tables across all layers.

### Bronze

- Pattern:
	`source__stream`

- Language:
	Keep stream names exactly as defined in the Landing layer.

- Rules:
	- Preserve source-oriented naming
	- Bronze stream names must match the corresponding Landing stream names
	- Use snake_case

- Examples:
	- `ibge__municipios`
	- `rfb__estabelecimentos`

---

### Silver

- Pattern:
	`source__stream`

- Language:
	Use English names.

- Rules:
	- A single Bronze stream can be split into multiple Silver models
	- Use standardized and descriptive naming
	- Keep models aligned with the operational domain structure
	- Use snake_case

- Example mapping:
	- `bronze.rfb__estabelecimentos` -> `silver.rfb__establishments_core`
	- `bronze.rfb__estabelecimentos` -> `silver.rfb__establishment_addresses`
	- `bronze.rfb__estabelecimentos` -> `silver.rfb__establishment_contacts`
	- `bronze.rfb__estabelecimentos` -> `silver.rfb__establishment_activities`

---

### Gold

The Gold layer follows a star schema modeling approach.

- Dimension pattern:
	`dim_<entity>`

- Fact pattern:
	`fact_<business_process>`

- Language:
	Use English business-oriented names.

- Rules:
	- Use descriptive and business-oriented names
	- Avoid generic or ambiguous terms
	- Design models for analytical consumption
	- Use snake_case

- Examples:
	- `dim_location`
	- `dim_company`
	- `dim_economic_activity`
	- `fact_location_demographics`
	- `fact_company_registration`

## Surrogate and Natural Key Naming

Use a consistent naming pattern for all dimensions and facts:

- Surrogate key: `<entity>_sk`
- Natural key: `<entity>_nk`

`<entity>` represents the business domain grain, not necessarily the model name.

Examples:

- `location_sk` and `location_nk`
- `economic_activity_sk` and `economic_activity_nk`
- `dim_location_geolocation` uses `location_sk` because it references the location entity

### Rules

- Dimensions should expose both keys whenever applicable.
- Surrogate keys must be deterministic and generated with `generate_sk` from natural key column(s).
- Models that reference a dimension should use the referenced entity surrogate key name (for example, `location_sk` when referencing `dim_location`).

## Bronze Metadata Fields

Bronze metadata is produced by the Auto Loader ingestion flow in `databricks/ingestion/autoloader/bronze_ingestion.py` and by landing partition conventions.

**Partition Naming Origin**: The temporal field names (`_extraction_ts`, `_reference_month`, `_reference_date`) are derived from the landing directory partition structure. For example, IBGE extractors partition landing data as `_extraction_ts={TIMESTAMP}`. These partition names are passed through by Auto Loader and become Bronze columns automatically.

| Field | Type | Description |
|---|---|---|
| `_extraction_ts` or `_reference_month` or `_reference_date` | `timestamp` or `string (yyyy-MM)` or `date` | Temporal reference field: use `_extraction_ts` for API calls, `_reference_month` for monthly snapshots, or `_reference_date` for date-based snapshots. |
| `_rescued_data` | `string` | Auto Loader rescued data column for malformed/unexpected fields. |
| `_ingestion_ts` | `timestamp` | Timestamp when the record was materialized in Bronze (set during Bronze ingestion write). |
| `_source_file` | `string` | Original landing file path from `_metadata.file_path`. |
| `_raw` | `string` | Raw JSON representation of the Bronze row generated during ingestion. |

## Silver Metadata Fields

All silver models must expose and document the following metadata fields:

| Field | Type | Description |
|---|---|---|
| `_extraction_ts` or `_reference_month` or `_reference_date` | `timestamp` or `string (yyyy-MM)` or `date` | Temporal reference field inherited from Bronze: use `_extraction_ts` for API calls, `_reference_month` for monthly snapshots, or `_reference_date` for date-based snapshots. |
| `_ingestion_ts` | `timestamp` | Timestamp when the record was materialized in Bronze. |
| `_load_ts` | `timestamp` | Timestamp when the record was materialized in Silver. |

## Gold Metadata Fields

All gold models must expose the following metadata field:

| Field | Type | Description |
|---|---|---|
| `_extraction_ts` or `_reference_month` or `_reference_date` | `timestamp` or `string (yyyy-MM)` or `date` | Optional temporal reference field inherited from upstream layers. Add it only when the Gold model is incremental and requires a cursor for incremental processing. |
| `_updated_at` | `timestamp` | Timestamp when the record was last updated in Gold. |
