{{ config(
    materialized = "table",
) }}

with base as (

    select
        -- natural keys (IBGE)
        cast(`UF-id` as bigint)     as state_id,
        cast(`regiao-id` as bigint) as region_id,

        -- descriptive attributes
        trim(cast(`UF-nome` as string))   as state_name,
        trim(cast(`UF-sigla` as string))  as state_abbreviation,

        trim(cast(`regiao-nome` as string))  as region_name,
        trim(cast(`regiao-sigla` as string)) as region_abbreviation,

        -- metadata
        cast(_extraction_ts as timestamp)   as _extraction_ts,
        cast(_ingestion_ts as timestamp) as _ingestion_ts,
        current_timestamp()              as _load_ts

    from {{ source('bronze', 'ibge__estados') }}
    where cast(`UF-id` as bigint) is not null
),

{{ latest_dedup(
    source_cte = "base",
    partition_by = ["state_id"]
) }}

select
    state_id,
    state_name,
    state_abbreviation,

    region_id,
    region_name,
    region_abbreviation,

    _extraction_ts,
    _ingestion_ts,
    _load_ts

from dedup
