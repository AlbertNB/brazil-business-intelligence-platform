{{ config(
    materialized = "table",
    tags = ["dim", "location"]
) }}

with br_states as (

    -- Brazilian states (IBGE)
    select
        cast(state_id as string) as location_nk,
        state_name as location_name,
        'state' as location_type,
        cast(state_id as bigint) as ibge_id,
        cast(state_id as string) as ibge_code,

        region_id,
        region_name,
        region_abbreviation,

        state_abbreviation,

        'ibge' as source
    from {{ ref('ibge__states') }}

),

br_municipalities as (

    -- Brazilian municipalities (cities)
    select
        cast(municipality_id as string) as location_nk,
        municipality_name as location_name,
        'municipality' as location_type,
        cast(municipality_id as bigint) as ibge_id,
        cast(municipality_id as string) as ibge_code,

        region_id,
        region_name,
        region_abbreviation,

        state_id,
        state_name,
        state_abbreviation,

        mesoregion_id,
        mesoregion_name,

        microregion_id,
        microregion_name,

        immediate_region_id,
        immediate_region_name,

        intermediate_region_id,
        intermediate_region_name,

        'ibge' as source
    from {{ ref('ibge__municipalities') }}

),

manual as (

    -- Manually added fixed locations not present in IBGE
    select
        location_nk,
        location_name,
        location_type,
        cast(null as bigint) as ibge_id,
        cast(null as string) as ibge_code,
        cast(null as bigint) as state_id,
        cast(null as string) as state_name,
        cast(null as string) as state_abbreviation,
        cast(null as bigint) as region_id,
        cast(null as string) as region_name,
        cast(null as string) as region_abbreviation,
        cast(null as bigint) as mesoregion_id,
        cast(null as string) as mesoregion_name,
        cast(null as bigint) as microregion_id,
        cast(null as string) as microregion_name,
        cast(null as bigint) as immediate_region_id,
        cast(null as string) as immediate_region_name,
        cast(null as bigint) as intermediate_region_id,
        cast(null as string) as intermediate_region_name,
        'manual' as source
    from {{ ref('ibge__location_manual_rows') }}

),

unioned as (

    select
        location_nk,
        location_name,
        location_type,
        ibge_id,
        ibge_code,

        -- state-level attributes
        cast(null as bigint) as state_id,
        cast(null as string) as state_name,
        cast(null as string) as state_abbreviation,

        -- region attributes
        region_id,
        region_name,
        region_abbreviation,

        -- sub-state attributes (null for states)
        cast(null as bigint) as mesoregion_id,
        cast(null as string) as mesoregion_name,
        cast(null as bigint) as microregion_id,
        cast(null as string) as microregion_name,
        cast(null as bigint) as immediate_region_id,
        cast(null as string) as immediate_region_name,
        cast(null as bigint) as intermediate_region_id,
        cast(null as string) as intermediate_region_name,

        source
    from br_states

    union all

    select
        location_nk,
        location_name,
        location_type,
        ibge_id,
        ibge_code,

        state_id,
        state_name,
        state_abbreviation,

        region_id,
        region_name,
        region_abbreviation,

        mesoregion_id,
        mesoregion_name,
        microregion_id,
        microregion_name,
        immediate_region_id,
        immediate_region_name,
        intermediate_region_id,
        intermediate_region_name,

        source
    from br_municipalities

    union all

    select
        location_nk,
        location_name,
        location_type,
        ibge_id,
        ibge_code,
        state_id,
        state_name,
        state_abbreviation,
        region_id,
        region_name,
        region_abbreviation,
        mesoregion_id,
        mesoregion_name,
        microregion_id,
        microregion_name,
        immediate_region_id,
        immediate_region_name,
        intermediate_region_id,
        intermediate_region_name,
        source
    from manual

),

final as (

    select
        {{ generate_sk(['location_nk']) }} as location_sk,

        location_nk,
        location_name,
        location_type,
        ibge_id,
        ibge_code,

        state_id,
        state_name,
        state_abbreviation,

        region_id,
        region_name,
        region_abbreviation,

        mesoregion_id,
        mesoregion_name,
        microregion_id,
        microregion_name,
        immediate_region_id,
        immediate_region_name,
        intermediate_region_id,
        intermediate_region_name,

        source,
        current_timestamp() as _updated_at

    from unioned
)

select *
from final
