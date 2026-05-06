{{ config(
    materialized = 'incremental',
    unique_key = ['company_root_id']
) }}

with source as (

    select
        trim(cast(_c0 as string))                                                   as company_root_id,
        trim(cast(_c1 as string))                                                   as legal_name,
        trim(cast(_c2 as string))                                                   as legal_nature_code,
        trim(cast(_c3 as string))                                                   as responsible_person_qualification,
        try_cast(nullif(replace(trim(cast(_c4 as string)), ',', '.'), '') as decimal(18, 2)) as share_capital,
        trim(cast(_c5 as string))                                                   as company_size_id,
        case trim(cast(_c5 as string))
            when '00' then 'NOT_INFORMED'
            when '01' then 'MICRO_COMPANY'
            when '03' then 'SMALL_COMPANY'
            when '05' then 'OTHER'
        end                                                                         as company_size_description,
        trim(cast(_c6 as string))                                                   as federative_entity_responsible,
        trim(cast(_reference_month as string))                                       as _reference_month,
        _ingestion_ts

    from {{ source('bronze', 'rfb__empresas') }}
    where _c0 is not null
      and {{ incremental_statement() }}

),

{{ latest_dedup(
    source_cte = 'source',
    partition_by = ['company_root_id'],
    extraction_column = '_reference_month'
) }}

select
    company_root_id,
    legal_name,
    legal_nature_code,
    responsible_person_qualification,
    share_capital,
    company_size_id,
    company_size_description,
    federative_entity_responsible,
    _reference_month,
    _ingestion_ts

from dedup
