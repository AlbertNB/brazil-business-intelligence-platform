{{ config(
    materialized = 'incremental',
    unique_key = ['location_id', 'indicator_id', 'reference_year']
) }}

with source as (

    select
        cast(_extraction as timestamp) as _extraction,
        cast(location_id as string) as location_id,
        from_json(
            cast(payload as string),
            'array<struct<id:bigint,res:array<struct<localidade:string,res:map<string,string>,notas:map<string,string>>>>>'
        ) as payload_items,
        cast(_ingestion_ts as timestamp) as _ingestion_ts,
        _source_file
    from {{ source('bronze', 'ibge__resultados') }}
    where payload is not null
      and {{ incremental_statement() }}

),

indicator_rows as (

    select
        s._extraction,
        s.location_id,
        s._ingestion_ts,
        s._source_file,
        indicator.id as indicator_id,
        indicator.res as indicator_results
    from source s
    lateral view explode(s.payload_items) exploded_indicator as indicator
    where s.payload_items is not null

),

location_rows as (

    select
        ir._extraction,
        ir.location_id,
        ir._ingestion_ts,
        ir._source_file,
        cast(ir.indicator_id as string) as indicator_id,
        result.localidade as payload_location_id,
        result.res as year_value_map,
        result.notas as year_note_map
    from indicator_rows ir
    lateral view explode(ir.indicator_results) exploded_result as result

),

year_value_rows as (

    select
        lr._extraction,
        lr.location_id,
        lr._ingestion_ts,
        lr._source_file,
        lr.indicator_id,
        lr.payload_location_id,
        reference_year,
        raw_metric_value
    from location_rows lr
    lateral view explode(lr.year_value_map) exploded_year_value as reference_year, raw_metric_value

),

base as (

    select
        location_id,

        cast(reference_year as int) as reference_year,
        indicator_id,

        try_cast(nullif(trim(replace(raw_metric_value, ',', '.')), '') as decimal(18,3)) as metric_value,
        _extraction,
        _ingestion_ts,
        _source_file,
        payload_location_id

    from year_value_rows
    where try_cast(nullif(trim(replace(raw_metric_value, ',', '.')), '') as decimal(18,3)) is not null

),

{{ latest_dedup(
    source_cte = 'base',
    partition_by = ['location_id', 'indicator_id', 'reference_year']
) }}

select
    location_id,
    reference_year,
    indicator_id,
    metric_value,
    _ingestion_ts,
    _source_file
from dedup