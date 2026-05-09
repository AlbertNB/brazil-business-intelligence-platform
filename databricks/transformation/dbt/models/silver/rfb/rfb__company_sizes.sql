{{ config(
    materialized = 'table'
) }}

with source as (

    select
        trim(cast(_c5 as string)) as company_size_id,
        case trim(cast(_c5 as string))
            when '00' then 'NOT_INFORMED'
            when '01' then 'MICRO_COMPANY'
            when '03' then 'SMALL_COMPANY'
            when '05' then 'OTHER'
        end as company_size_description,
        _reference_month,
        _ingestion_ts
    from {{ source('bronze', 'rfb__empresas') }}
    where _c5 is not null

),

{{ latest_dedup(
    source_cte = 'source',
    partition_by = ['company_size_id'],
    extraction_column = '_reference_month'
) }}

select
    company_size_id,
    company_size_description,
    _reference_month,
    _ingestion_ts,
    current_timestamp() as _load_ts

from dedup