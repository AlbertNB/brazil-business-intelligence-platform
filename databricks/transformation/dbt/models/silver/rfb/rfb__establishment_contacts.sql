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
        trim(cast(_c21 as string))                                                          as phone_area_code_1,
        trim(cast(_c22 as string))                                                          as phone_number_1,
        trim(cast(_c23 as string))                                                          as phone_area_code_2,
        trim(cast(_c24 as string))                                                          as phone_number_2,
        trim(cast(_c25 as string))                                                          as fax_area_code,
        trim(cast(_c26 as string))                                                          as fax_number,
        trim(cast(_c27 as string))                                                          as email,
        trim(cast(_reference_month as string))                                               as _reference_month,
        _ingestion_ts,
        current_timestamp()                                                                  as _load_ts

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
    phone_area_code_1,
    phone_number_1,
    phone_area_code_2,
    phone_number_2,
    fax_area_code,
    fax_number,
    email,
    _reference_month,
    _ingestion_ts,
    _load_ts

from dedup
