{{ config(
    materialized = "table",
    tags = ["fact", "business_registry"]
) }}

with establishments as (

    select *
    from {{ ref('dim_establishment') }}

),

companies as (

    select *
    from {{ ref('dim_company') }}

),

final as (

    select
        e.establishment_sk,
        c.company_sk,
        e.location_sk,
        e.primary_economic_activity_sk,
        c.legal_nature_sk,
        c.legal_nature_nk,
        c.company_size_sk,
        c.company_size_nk,
        e.registration_status_sk,
        e.registration_status_nk,
        e.registration_status_reason_sk,
        e.registration_status_reason_nk,
        e.is_active,
        e.is_headquarters,
        e.is_branch,
        c.is_simples,
        c.is_mei,
        c.simples_nacional_option_date,
        c.simples_nacional_exclusion_date,
        c.mei_option_date,
        c.mei_exclusion_date,
        e.opening_date,
        e.closing_date,
        e.registration_status_date,
        1 as establishment_count,
        case when e.is_active then 1 else 0 end as active_establishment_count,
        case when e.is_headquarters then 1 else 0 end as headquarter_count,
        case when e.is_branch then 1 else 0 end as branch_count,
        case when c.is_simples then 1 else 0 end as simples_count,
        case when c.is_mei then 1 else 0 end as mei_count,
        e._reference_month,
        current_timestamp() as _updated_at
    from establishments e
    left join companies c
        on e.company_root_nk = c.company_root_nk

)

select * from final
