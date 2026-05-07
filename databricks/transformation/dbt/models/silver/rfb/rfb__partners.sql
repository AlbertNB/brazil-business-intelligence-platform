{{ config(
    materialized = 'incremental',
    unique_key = ['company_root_id', 'partner_name', 'partner_document_id', '_reference_month']
) }}

with source as (

    select
        trim(cast(_c0  as string))                                      as company_root_id,
        trim(cast(_c1  as string))                                      as partner_identifier_id,
        case trim(cast(_c1 as string))
            when '1' then 'LEGAL_ENTITY'
            when '2' then 'INDIVIDUAL'
            when '3' then 'FOREIGNER'
        end                                                             as partner_identifier_description,
        trim(cast(_c2  as string))                                      as partner_name,
        trim(cast(_c3  as string))                                      as partner_document_id,
        trim(cast(_c4  as string))                                      as partner_qualification_code,
        {{ rfb_date('_c5') }}                                                as partnership_start_date,
        trim(cast(_c6  as string))                                      as country_code,
        {{ rfb_document_id('_c7') }}                                    as legal_representative_document_id,
        trim(cast(_c8  as string))                                      as legal_representative_name,
        trim(cast(_c9  as string))                                      as legal_representative_qualification_code,
        trim(cast(_c10 as string))                                      as age_group_id,
        case trim(cast(_c10 as string))
            when '0' then 'NOT_APPLICABLE'
            when '1' then '0_TO_12'
            when '2' then '13_TO_20'
            when '3' then '21_TO_30'
            when '4' then '31_TO_40'
            when '5' then '41_TO_50'
            when '6' then '51_TO_60'
            when '7' then '61_TO_70'
            when '8' then '71_TO_80'
            when '9' then 'OVER_80'
        end                                                             as age_group_description,
        trim(cast(_reference_month as string))                           as _reference_month,
        _ingestion_ts

    from {{ source('bronze', 'rfb__socios') }}
    where _c0 is not null
            and {{ incremental_statement('_reference_month') }}

)

select
    company_root_id,
    partner_identifier_id,
    partner_identifier_description,
    partner_name,
    partner_document_id,
    partner_qualification_code,
    partnership_start_date,
    country_code,
    legal_representative_document_id,
    legal_representative_name,
    legal_representative_qualification_code,
    age_group_id,
    age_group_description,
    _reference_month,
    _ingestion_ts,
    current_timestamp() as _load_ts

from source
