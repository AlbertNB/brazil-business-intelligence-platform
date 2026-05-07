{{ config(
    materialized = 'table'
) }}

with base as (

    select
        trim(cast(_c0 as string))           as legal_nature_code,
        trim(cast(_c1 as string))           as legal_nature_description,
        trim(cast(_reference_month as string)) as _reference_month,
        _ingestion_ts

    from {{ source('bronze', 'rfb__naturezas') }}
    where _c0 is not null

),

{{ latest_dedup(
    source_cte = 'base',
    partition_by = ['legal_nature_code'],
    extraction_column = '_reference_month'
) }}

select
    legal_nature_code,
    legal_nature_description,
    _reference_month,
    _ingestion_ts,
    current_timestamp() as _load_ts

from dedup
