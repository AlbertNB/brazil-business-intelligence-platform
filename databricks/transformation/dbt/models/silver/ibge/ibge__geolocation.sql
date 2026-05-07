{{ config(
    materialized = "table",
) }}

with source as (

    select
        trim(cast(geo_level as string))      as location_type,
        trim(cast(type as string))           as geojson_type,
        cast(features as string)             as features_str,
        cast(_extraction_ts as timestamp)    as _extraction_ts,
        cast(_ingestion_ts as timestamp)     as _ingestion_ts,
        current_timestamp()                  as _load_ts

    from {{ source('bronze', 'ibge__geolocation') }}
    where trim(cast(geo_level as string)) is not null
      and cast(features as string) is not null
      and trim(cast(features as string)) <> ''
),

{{ latest_dedup(
    source_cte = "source",
    partition_by = ["location_type"]
) }},

base as (

    select
        location_type,
        geojson_type,

        -- Parse metadata (feature type, geometry type, codarea) — no coordinates
        from_json(
            features_str,
            'array<struct<type:string,geometry:struct<type:string>,properties:struct<codarea:string>>>'
        ) as features_meta,

        -- Pre-compute geometry JSON strings per schema depth.
        -- The correct array is selected in the exploded CTE by feature_item.geometry.type.
        transform(
            from_json(
                features_str,
                'array<struct<geometry:struct<type:string,coordinates:array<array<array<double>>>>>>'
            ),
            f -> to_json(f.geometry)
        ) as geom_polygon_jsons,

        transform(
            from_json(
                features_str,
                'array<struct<geometry:struct<type:string,coordinates:array<array<array<array<double>>>>>>>'
            ),
            f -> to_json(f.geometry)
        ) as geom_multipolygon_jsons,

        _extraction_ts,
        _ingestion_ts,
        _load_ts

    from dedup
),

exploded as (

    select
        b.location_type,
        b.geojson_type,
        feature_position,
        feature_item,
        case feature_item.geometry.type
            when 'Polygon'      then b.geom_polygon_jsons[feature_position]
            when 'MultiPolygon' then b.geom_multipolygon_jsons[feature_position]
        end as geom_json,
        b._extraction_ts,
        b._ingestion_ts,
        b._load_ts

    from base b
    lateral view posexplode(b.features_meta) t as feature_position, feature_item
)

select
    location_type,
    trim(feature_item.properties.codarea)  as location_id,
    geojson_type,
    trim(feature_item.type)                as feature_type,
    trim(feature_item.geometry.type)       as geometry_type,
    geom_json                              as geometry_geojson,
    'geoJSON'                              as geometry_source_format,
    _extraction_ts,
    _ingestion_ts,
    _load_ts

from exploded