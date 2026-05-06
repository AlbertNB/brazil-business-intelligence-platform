{{ config(
    materialized = 'table'
) }}

with overrides as (

    select
        trim(cast(state_abbreviation as string)) as state_abbreviation,
        trim(cast(rfb_municipality_name_norm as string)) as rfb_municipality_name_norm,
        trim(cast(ibge_municipality_name_norm as string)) as ibge_municipality_name_norm
    from {{ ref('rfb__bridge_ibge_municipalities_overrides') }}

),

rfb_establishments_latest as (

    select
        trim(cast(_c19 as string)) as state_abbreviation,
        trim(cast(_c20 as string)) as municipality_code
    from {{ source('bronze', 'rfb__estabelecimentos') }}
    where _reference_month = (
        select max(_reference_month)
        from {{ source('bronze', 'rfb__estabelecimentos') }}
    )
      and _c19 is not null
      and _c20 is not null

),

rfb_municipalities_latest as (

    select
        trim(cast(_c0 as string)) as municipality_code,
        trim(cast(_c1 as string)) as municipality_name
    from {{ source('bronze', 'rfb__municipios') }}
    where _reference_month = (
        select max(_reference_month)
        from {{ source('bronze', 'rfb__municipios') }}
    )
      and _c0 is not null

),

rfb as (

    select distinct
        e.state_abbreviation as source_state_abbreviation,
        e.municipality_code as source_municipality_code,
        m.municipality_name as source_municipality_name,
        {{ rfb_normalize_municipality_name('m.municipality_name') }} as municipality_name_norm
    from rfb_establishments_latest e
    left join rfb_municipalities_latest m
        on e.municipality_code = m.municipality_code
    where not (e.municipality_code = '6969' and e.state_abbreviation = 'PA')
      and e.state_abbreviation != 'EX'

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
    end as match_status
from rfb_with_override rfb
left join ibge
    on rfb.municipality_name_norm_for_match = ibge.municipality_name_norm
   and rfb.source_state_abbreviation = ibge.ibge_state_abbreviation
