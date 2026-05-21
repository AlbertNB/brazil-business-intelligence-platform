{{ config(
    materialized = "table",
    tags = ["fact", "business_registry"]
) }}

with establishment_activity as (

    select
        fes.location_sk,
        beea.economic_activity_sk,
        beea.is_main_activity,
        fes.is_active,
        fes.is_headquarters,
        fes.is_branch,
        fes.is_simples,
        fes.is_mei,
        fes._reference_month
    from {{ ref('fact_establishment_snapshot') }} fes
    inner join {{ ref('bridge_establishment_economic_activity') }} beea
        on fes.establishment_sk = beea.establishment_sk
    where fes.location_sk is not null

),

final as (

    select
        location_sk,
        economic_activity_sk,
        is_main_activity,
        is_active,
        count(*) as establishments_count,
        sum(case when is_headquarters then 1 else 0 end) as headquarter_count,
        sum(case when is_branch then 1 else 0 end) as branch_count,
        sum(case when is_simples then 1 else 0 end) as simples_count,
        sum(case when is_mei then 1 else 0 end) as mei_count,
        max(_reference_month) as _reference_month,
        current_timestamp() as _updated_at
    from establishment_activity
    group by location_sk, economic_activity_sk, is_main_activity, is_active

)

select * from final
