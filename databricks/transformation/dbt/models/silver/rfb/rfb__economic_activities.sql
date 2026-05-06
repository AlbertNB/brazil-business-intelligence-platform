{{ config(
    materialized = 'table'
) }}

with base as (

    select
        trim(cast(_c0 as string))              as economic_activity_code,
        trim(cast(_c1 as string))              as economic_activity_description,
        trim(cast(_reference_month as string)) as _reference_month,
        _ingestion_ts

    from {{ source('bronze', 'rfb__cnaes') }}
    where _c0 is not null

),

{{ latest_dedup(
    source_cte = 'base',
    partition_by = ['economic_activity_code'],
    extraction_column = '_reference_month'
) }}

select
    economic_activity_code,
    economic_activity_description,
    _reference_month,
    _ingestion_ts

from dedup
