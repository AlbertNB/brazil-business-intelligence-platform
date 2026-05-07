{{ config(
    materialized = 'table'
) }}

with base as (

    select
        trim(cast(_c0 as string))           as municipality_code,
        trim(cast(_c1 as string))           as municipality_name,
        trim(cast(_reference_month as string)) as _reference_month,
        _ingestion_ts,
        current_timestamp() as _load_ts

    from {{ source('bronze', 'rfb__municipios') }}
    where _c0 is not null

),

{{ latest_dedup(
    source_cte = 'base',
    partition_by = ['municipality_code'],
    extraction_column = '_reference_month'
) }}

select
    municipality_code,
    municipality_name,
    _reference_month,
    _ingestion_ts,
    _load_ts

from dedup
