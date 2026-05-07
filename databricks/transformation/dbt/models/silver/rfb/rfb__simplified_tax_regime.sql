{{ config(
    materialized = 'incremental',
    unique_key = ['company_root_id', '_reference_month']
) }}

with source as (

    select
        trim(cast(_c0 as string))                                       as company_root_id,
        {{ rfb_flag('_c1') }}                                           as is_simples_nacional,
        {{ rfb_date('_c2') }}                                               as simples_nacional_option_date,
        {{ rfb_date('_c3') }}                                               as simples_nacional_exclusion_date,
        {{ rfb_flag('_c4') }}                                           as is_mei,
        {{ rfb_date('_c5') }}                                               as mei_option_date,
        {{ rfb_date('_c6') }}                                               as mei_exclusion_date,
        trim(cast(_reference_month as string))                           as _reference_month,
        _ingestion_ts

    from {{ source('bronze', 'rfb__simples') }}
    where _c0 is not null
            and {{ incremental_statement('_reference_month') }}

),

select
    company_root_id,
    is_simples_nacional,
    simples_nacional_option_date,
    simples_nacional_exclusion_date,
    is_mei,
    mei_option_date,
    mei_exclusion_date,
    _reference_month,
    _ingestion_ts,
    current_timestamp() as _load_ts

from dedup
