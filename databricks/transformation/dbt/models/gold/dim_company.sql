{{ config(
    materialized = "table",
    tags = ["dim", "business_registry"]
) }}

with companies_base as (

    select
        company_root_id,
        legal_name,
        legal_nature_code,
        responsible_person_qualification,
        company_size_id,
        company_size_description,
        share_capital,
        _reference_month
    from {{ ref('rfb__companies') }}

),

{{ latest_only(
    source_cte = 'companies_base',
    extraction_column = '_reference_month',
    cte_context = 'companies'
) }},

simples_base as (

    select
        company_root_id,
        is_simples_nacional,
        simples_nacional_option_date,
        simples_nacional_exclusion_date,
        is_mei,
        mei_option_date,
        mei_exclusion_date,
        _reference_month
    from {{ ref('rfb__simplified_tax_regime') }}

),

{{ latest_only(
    source_cte = 'simples_base',
    extraction_column = '_reference_month',
    cte_context = 'simples'
) }},

final as (

    select
        {{ generate_sk(['c.company_root_id']) }} as company_sk,
        c.company_root_id as company_root_nk,
        c.legal_name as company_name,
        {{ generate_sk(['c.legal_nature_code']) }} as legal_nature_sk,
        c.legal_nature_code as legal_nature_nk,
        {{ generate_sk(['c.responsible_person_qualification']) }} as responsible_qualification_sk,
        c.responsible_person_qualification as responsible_qualification_nk,
        {{ generate_sk(['c.company_size_id']) }} as company_size_sk,
        c.company_size_id as company_size_nk,
        c.company_size_description,
        c.share_capital,
        case
            when coalesce(s.is_mei, false) then true
            else coalesce(s.is_simples_nacional, false)
        end as is_simples,
        s.simples_nacional_option_date,
        s.simples_nacional_exclusion_date,
        coalesce(s.is_mei, false) as is_mei,
        s.mei_option_date,
        s.mei_exclusion_date,
        c._reference_month,
        current_timestamp() as _updated_at
    from latest_companies c
    left join latest_simples s
        on c.company_root_id = s.company_root_id

)

select * from final
