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
        trim(cast(_c0  as string))                                                          as company_root_id,
        trim(cast(_c1  as string))                                                          as establishment_order,
        trim(cast(_c2  as string))                                                          as establishment_check_digit,
        trim(cast(_c3  as string))                                                          as establishment_type_id,
        case trim(cast(_c3 as string))
            when '1' then 'HEADQUARTERS'
            when '2' then 'BRANCH'
        end                                                                                 as establishment_type_description,
        trim(cast(_c4  as string))                                                          as trade_name,
        trim(cast(_c5  as string))                                                          as registration_status_id,
        case trim(cast(_c5 as string))
            when '01' then 'NULL_REGISTRATION'
            when '02' then 'ACTIVE'
            when '03' then 'SUSPENDED'
            when '04' then 'UNFIT'
            when '08' then 'CLOSED'
        end                                                                                 as registration_status_description,
        {{ rfb_date('_c6') }}                                                               as registration_status_date,
        trim(cast(_c7  as string))                                                          as registration_status_reason_code,
        {{ rfb_date('_c10') }}                                                              as activity_start_date,
        trim(cast(_c28 as string))                                                          as special_status,
        {{ rfb_date('_c29') }}                                                              as special_status_date,
        trim(cast(_reference_month as string))                                               as _reference_month,
        _ingestion_ts

    from {{ source('bronze', 'rfb__estabelecimentos') }}
    where _c0 is not null
      and {{ incremental_statement() }}

),

{{ latest_dedup(
    source_cte = 'source',
    partition_by = ['cnpj_id'],
    extraction_column = '_reference_month'
) }}

select
    cnpj_id,
    company_root_id,
    establishment_order,
    establishment_check_digit,
    establishment_type_id,
    establishment_type_description,
    trade_name,
    registration_status_id,
    registration_status_description,
    registration_status_date,
    registration_status_reason_code,
    activity_start_date,
    special_status,
    special_status_date,
    _reference_month,
    _ingestion_ts

from dedup
