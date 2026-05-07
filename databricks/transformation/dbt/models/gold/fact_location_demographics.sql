{{ config(
    materialized = "incremental",
    unique_key = ["location_sk", "year"],
    tags = ["fact", "demographics"]
) }}

with base as (

    select
        s.location_id as location_nk,
        s.reference_year as year,
        s.indicator_id,
        s.metric_value,
        s._extraction_ts
    from {{ ref('ibge__indicators_by_location_year') }} s
    where s.indicator_id in ('96385', '29171', '29167', '48980')
      and {{ incremental_statement('_extraction_ts') }}

),

census_population as (

    select
        location_nk,
        year,
        metric_value as population_census
    from base
    where indicator_id = '96385'

),

estimated_population as (

    select
        location_nk,
        year,
        metric_value as population_estimated
    from base
    where indicator_id = '29171'

),

population_years as (

    select location_nk, year from census_population
    union
    select location_nk, year from estimated_population

),

area_by_year as (

    select
        location_nk,
        year,
        max(metric_value) as area_km2
    from base
    where indicator_id in ('29167', '48980')
    group by location_nk, year

),

latest_extraction as (

    select
        location_nk,
        year,
        max(_extraction_ts) as _extraction_ts
    from base
    group by location_nk, year

),

final as (

    select
        {{ generate_sk(['py.location_nk']) }} as location_sk,
        py.location_nk,
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
        le._extraction_ts
    from population_years py
    left join census_population cp
        on py.location_nk = cp.location_nk
       and py.year = cp.year
    left join estimated_population ep
        on py.location_nk = ep.location_nk
       and py.year = ep.year
    left join area_by_year ay
        on py.location_nk = ay.location_nk
       and py.year = ay.year
    left join latest_extraction le
        on py.location_nk = le.location_nk
       and py.year = le.year

)

select
    location_sk,
    location_nk,
    year,
    population_census,
    population_estimated,
    population_preferred,
    population_preferred_source,
    area_km2,
    density_census,
    density_estimated,
    density_preferred,
    _extraction_ts,
    current_timestamp() as _updated_at
from final