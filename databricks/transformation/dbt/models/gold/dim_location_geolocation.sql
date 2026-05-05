{{ config(
    materialized = "table",
    tags = ["dim", "location", "geolocation"]
) }}

with source as (

    select
        location_id,
        location_type,
        geojson_type,
        feature_type,
        geometry_type,
        geometry_geojson,
        geometry_source_format,
        _ingestion_ts

    from {{ ref('ibge__geolocation') }}

),

final as (

    select
        {{ generate_sk(['location_id']) }} as location_sk,

        location_id                        as location_nk,
        location_type,
        geojson_type,
        feature_type,
        geometry_type,
        geometry_geojson,
        geometry_source_format,

        _ingestion_ts,
        current_timestamp() as _updated_at

    from source

)

select * from final
