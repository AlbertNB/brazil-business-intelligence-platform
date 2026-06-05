{{ config(
    materialized = 'table',
    tags = ["bridge", "business_registry"]
) }}

with overrides as (

    select
        trim(cast(state_abbreviation as string))           as state_abbreviation,
        trim(cast(rfb_municipality_name_norm as string))   as rfb_municipality_name_norm,
        trim(cast(ibge_municipality_name_norm as string))  as ibge_municipality_name_norm
    from {{ ref('rfb__bridge_ibge_municipalities_overrides') }}

),

rfb_establishments_latest as (

    select distinct
        state_abbreviation,
        municipality_code
    from {{ ref('rfb__establishment_addresses') }}
    where _reference_month = (
        select max(_reference_month)
        from {{ ref('rfb__establishment_addresses') }}
    )
      and state_abbreviation is not null
      and municipality_code is not null
      and state_abbreviation != 'EX'
      and not (municipality_code = '6969' and state_abbreviation = 'PA')

),

rfb as (

    select
        e.state_abbreviation as source_state_abbreviation,
        e.municipality_code as source_municipality_code,
        m.municipality_name as source_municipality_name,
        m._reference_month,
        m._ingestion_ts,
        {{ rfb_normalize_municipality_name('m.municipality_name') }} as municipality_name_norm
    from rfb_establishments_latest e
    left join {{ ref('rfb__municipalities') }} m
        on e.municipality_code = m.municipality_code

),

ibge as (

    select
        municipality_id as ibge_municipality_id,
        municipality_name as ibge_municipality_name,
        state_abbreviation as ibge_state_abbreviation,
        {{ rfb_normalize_municipality_name('municipality_name') }} as municipality_name_norm
    from {{ ref('ibge__municipalities') }}

),

rfb_with_override as (

    select
        rfb.source_state_abbreviation,
        rfb.source_municipality_code,
        rfb.source_municipality_name,
        rfb._reference_month,
        rfb._ingestion_ts,
        overrides.ibge_municipality_name_norm is not null as is_override_applied,
        coalesce(
            overrides.ibge_municipality_name_norm,
            rfb.municipality_name_norm
        ) as municipality_name_norm_for_match
    from rfb
    left join overrides
        on rfb.source_state_abbreviation = overrides.state_abbreviation
       and rfb.municipality_name_norm = overrides.rfb_municipality_name_norm

)

select
    'RFB' as source_system,
    rfb.source_municipality_code,
    rfb.source_municipality_name,
    rfb.source_state_abbreviation,
    ibge.ibge_municipality_id,
    ibge.ibge_municipality_name,
    ibge.ibge_state_abbreviation,
    rfb.is_override_applied,
    case
        when ibge.ibge_municipality_id is null then 'UNMATCHED'
        else 'MATCHED'
    end as match_status,
    rfb._reference_month,
    rfb._ingestion_ts,
    current_timestamp() as _load_ts
from rfb_with_override rfb
left join ibge
    on rfb.municipality_name_norm_for_match = ibge.municipality_name_norm
   and rfb.source_state_abbreviation = ibge.ibge_state_abbreviation
