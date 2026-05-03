{{ config(
    materialized = 'incremental',
    unique_key = ['company_root_id']
) }}

with source as (

    select
        trim(cast(_c0 as string))                                       as company_root_id,
        {{ rfb_flag('_c1') }}                                           as is_simples,
        {{ rfb_date('_c2') }}                                               as simples_option_date,
        {{ rfb_date('_c3') }}                                               as simples_exclusion_date,
        {{ rfb_flag('_c4') }}                                           as is_mei,
        {{ rfb_date('_c5') }}                                               as mei_option_date,
        {{ rfb_date('_c6') }}                                               as mei_exclusion_date,
        trim(cast(reference_month as string))                           as reference_month,
        _ingestion_ts

    from {{ source('bronze', 'rfb__simples') }}
    where _c0 is not null
      and {{ incremental_statement() }}

),

{{ latest_dedup(
    source_cte = 'source',
    partition_by = ['company_root_id'],
    extraction_column = 'reference_month'
) }}

select
    company_root_id,
    is_simples,
    simples_option_date,
    simples_exclusion_date,
    is_mei,
    mei_option_date,
    mei_exclusion_date,
    reference_month,
    _ingestion_ts

from dedup
