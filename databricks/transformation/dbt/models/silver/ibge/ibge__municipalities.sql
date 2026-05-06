{{ config(
    materialized = "table",
) }}

with base as (

    select
        -- natural keys (IBGE)
        cast(`municipio-id` as bigint)            as municipality_id,
        cast(`UF-id` as bigint)                   as state_id,
        cast(`regiao-id` as bigint)               as region_id,
        cast(`mesorregiao-id` as bigint)          as mesoregion_id,
        cast(`microrregiao-id` as bigint)         as microregion_id,
        cast(`regiao-imediata-id` as bigint)      as immediate_region_id,
        cast(`regiao-intermediaria-id` as bigint) as intermediate_region_id,

        -- descriptive attributes
        trim(cast(`municipio-nome` as string))    as municipality_name,
        trim(cast(`UF-nome` as string))           as state_name,
        trim(cast(`UF-sigla` as string))          as state_abbreviation,

        trim(cast(`regiao-nome` as string))       as region_name,
        trim(cast(`regiao-sigla` as string))      as region_abbreviation,

        trim(cast(`mesorregiao-nome` as string))  as mesoregion_name,
        trim(cast(`microrregiao-nome` as string)) as microregion_name,

        trim(cast(`regiao-imediata-nome` as string))      as immediate_region_name,
        trim(cast(`regiao-intermediaria-nome` as string)) as intermediate_region_name,

        -- metadata
        cast(_extraction_ts as timestamp)            as _extraction_ts,
        cast(_ingestion_ts as timestamp)          as _ingestion_ts,
        current_timestamp()                       as _load_ts

    from {{ source('bronze', 'ibge__municipios') }}
    where cast(`municipio-id` as bigint) is not null
),

{{ latest_dedup(
    source_cte = "base",
    partition_by = ["municipality_id"]
) }}

select
    municipality_id,
    municipality_name,

    state_id,
    state_name,
    state_abbreviation,

    region_id,
    region_name,
    region_abbreviation,

    mesoregion_id,
    mesoregion_name,

    microregion_id,
    microregion_name,

    immediate_region_id,
    immediate_region_name,

    intermediate_region_id,
    intermediate_region_name,

    _ingestion_ts,
    _load_ts

from dedup
