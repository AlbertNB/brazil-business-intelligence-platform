{{ config(
    materialized = 'table'
) }}

with source as (

    select
        {{ rfb_company_size_id('_c5') }}              as company_size_id,
        {{ rfb_company_size_description('_c5') }}     as company_size_description,
        _reference_month,
        _ingestion_ts
    from {{ source('bronze', 'rfb__empresas') }}

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