{{ config(
    materialized = "incremental",
    unique_key = ["dim_location_id", "year"],
    tags = ["fact", "demographics"]
) }}

with base as (

    select
        s.location_id,
        s.reference_year as year,
        s.indicator_id,
        s.metric_value,
        s._ingestion_ts
    from {{ ref('ibge__indicators_by_location_year') }} s
    where s.indicator_id in ('96385', '29171', '29167', '48980')
      and {{ incremental_statement() }}

),

scoped as (

    -- Keep only valid locations present in the location dimension
    select
        d.dim_location_id,
        b.location_id,
        b.year,
        b.indicator_id,
        b.metric_value,
        b._ingestion_ts
    from base b
    inner join {{ ref('dim_location') }} d
        on b.location_id = d.location_nk

),

census_population as (

    select
        dim_location_id,
        location_id,
        year,
        metric_value as population_census
    from scoped
    where indicator_id = '96385'

),

estimated_population as (

    select
        dim_location_id,
        location_id,
        year,
        metric_value as population_estimated
    from scoped
    where indicator_id = '29171'

),

population_years as (

    select dim_location_id, location_id, year from census_population
    union
    select dim_location_id, location_id, year from estimated_population

),

area_by_year as (

    select
        dim_location_id,
        location_id,
        year,
        max(metric_value) as area_km2
    from scoped
    where indicator_id in ('29167', '48980')
    group by dim_location_id, location_id, year

),

latest_ingestion as (

    select
        dim_location_id,
        location_id,
        year,
        max(_ingestion_ts) as _ingestion_ts
    from scoped
    group by dim_location_id, location_id, year

),

final as (

    select
        py.dim_location_id,
        py.location_id,
        py.year,
        cp.population_census,
        ep.population_estimated,
        case
            when cp.population_census is not null then cp.population_census
            else ep.population_estimated
        end as population_preferred,
        case
            when cp.population_census is not null then 'census'
            when ep.population_estimated is not null then 'estimate'
            else null
        end as population_preferred_source,
        ay.area_km2,
        round(cp.population_census / nullif(ay.area_km2, 0), 2) as density_census,
        round(ep.population_estimated / nullif(ay.area_km2, 0), 2) as density_estimated,
        round(
            (
                case
                    when cp.population_census is not null then cp.population_census
                    else ep.population_estimated
                end
            ) / nullif(ay.area_km2, 0),
            2
        ) as density_preferred,
        li._ingestion_ts
    from population_years py
    left join census_population cp
        on py.dim_location_id = cp.dim_location_id
       and py.location_id = cp.location_id
       and py.year = cp.year
    left join estimated_population ep
        on py.dim_location_id = ep.dim_location_id
       and py.location_id = ep.location_id
       and py.year = ep.year
    left join area_by_year ay
        on py.dim_location_id = ay.dim_location_id
       and py.location_id = ay.location_id
       and py.year = ay.year
    left join latest_ingestion li
        on py.dim_location_id = li.dim_location_id
       and py.location_id = li.location_id
       and py.year = li.year

)

select
    dim_location_id,
    location_id,
    year,
    population_census,
    population_estimated,
    population_preferred,
    population_preferred_source,
    area_km2,
    density_census,
    density_estimated,
    density_preferred,
    _ingestion_ts
from final