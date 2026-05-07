{{ config(
    materialized = 'incremental',
    unique_key = ['cnpj_id']
) }}

with source as (

    select
        concat(
            trim(cast(_c0 as string)),
            trim(cast(_c1 as string)),
            trim(cast(_c2 as string))
        )                                                                                   as cnpj_id,
        trim(cast(_c8  as string))                                                          as foreign_city_name,
        nullif(trim(cast(_c9 as string)), '')                                               as country_code,
        trim(cast(_c13 as string))                                                          as address_type,
        trim(cast(_c14 as string))                                                          as street_name,
        trim(cast(_c15 as string))                                                          as street_number,
        trim(cast(_c16 as string))                                                          as address_complement,
        trim(cast(_c17 as string))                                                          as neighborhood,
        trim(cast(_c18 as string))                                                          as postal_code,
        trim(cast(_c19 as string))                                                          as state,
        trim(cast(_c20 as string))                                                          as municipality_code,
        trim(cast(_reference_month as string))                                               as _reference_month,
        _ingestion_ts

    from {{ source('bronze', 'rfb__estabelecimentos') }}
    where _c0 is not null
            and {{ incremental_statement('_reference_month') }}

),

{{ latest_dedup(
    source_cte = 'source',
    partition_by = ['cnpj_id'],
    extraction_column = '_reference_month'
) }}

select
    cnpj_id,
    foreign_city_name,
    country_code,
    address_type,
    street_name,
    street_number,
    address_complement,
    neighborhood,
    postal_code,
    state,
    municipality_code,
    _reference_month,
    _ingestion_ts,
    current_timestamp() as _load_ts

from dedup
