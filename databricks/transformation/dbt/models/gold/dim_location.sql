{{ config(
    materialized = "incremental",
    unique_key = "location_nk",
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

        _ingestion_ts
    from {{ ref('ibge__states') }}

),

br_municipalities as (

    -- Brazilian municipalities (cities)
    select
        cast(municipality_id as string) as location_nk,
        municipality_name as location_name,
        'city' as location_type,
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

        _ingestion_ts
    from {{ ref('ibge__municipalities') }}

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

        _ingestion_ts
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

        _ingestion_ts
    from br_municipalities

),

{{ latest_dedup(
    source_cte = "unioned",
    partition_by = ["location_nk"],
    extraction_column = "_ingestion_ts"
) }},

final as (

    select
        {{ generate_sk(['location_nk']) }} as dim_location_id,

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

        _ingestion_ts,
        current_timestamp() as _updated_at

    from dedup
)

select *
from final
where {{ incremental_statement() }}
;
