{{ config(
    materialized = 'table'
) }}

with base as (

    select
        trim(cast(_c0 as string))           as qualification_code,
        trim(cast(_c1 as string))           as qualification_description,
        trim(cast(reference_month as string)) as reference_month,
        _ingestion_ts

    from {{ source('bronze', 'rfb__qualificacoes') }}
    where _c0 is not null

),

{{ latest_dedup(
    source_cte = 'base',
    partition_by = ['qualification_code'],
    extraction_column = 'reference_month'
) }}

select
    qualification_code,
    qualification_description,
    reference_month,
    _ingestion_ts

from dedup
