{{ config(
    materialized = 'table'
) }}

with source as (

    select
        trim(cast(_c5 as string)) as registration_status_id,
        case trim(cast(_c5 as string))
            when '01' then 'NULL_REGISTRATION'
            when '02' then 'ACTIVE'
            when '03' then 'SUSPENDED'
            when '04' then 'UNFIT'
            when '08' then 'CLOSED'
        end as registration_status_description,
        _reference_month,
        _ingestion_ts
    from {{ source('bronze', 'rfb__estabelecimentos') }}
    where _c5 is not null

),

{{ latest_dedup(
    source_cte = 'source',
    partition_by = ['registration_status_id'],
    extraction_column = '_reference_month'
) }}

select
    registration_status_id,
    registration_status_description,
    _reference_month,
    _ingestion_ts,
    current_timestamp() as _load_ts

from dedup