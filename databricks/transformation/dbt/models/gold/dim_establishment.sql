{{ config(
    materialized = "table",
    tags = ["dim", "business_registry"]
) }}

with core_base as (

    select
        cnpj_id,
        company_root_id,
        establishment_order,
        establishment_check_digit,
        trade_name,
        establishment_type_description,
        establishment_type_id = '1' as is_headquarters,
        establishment_type_id = '2' as is_branch,
        registration_status_id,
        case when registration_status_reason_code = '32' then '00' else registration_status_reason_code end as registration_status_reason_code,
        registration_status_date,
        activity_start_date,
        main_cnae_code,
        _reference_month
    from {{ ref('rfb__establishment_core') }}

),

{{ latest_only(
    source_cte = 'core_base',
    extraction_column = '_reference_month',
    cte_context = 'core'
) }},

addresses_base as (

    select
        cnpj_id,
        lpad(coalesce(country_code, '105'), 3, '0') as country_code,
        state_abbreviation,
        postal_code,
        nullif(concat_ws(' ', trim(address_type), trim(street_name)), '') as address,
        street_number,
        address_complement,
        neighborhood,
        municipality_code,
        _reference_month
    from {{ ref('rfb__establishment_addresses') }}

),

{{ latest_only(
    source_cte = 'addresses_base',
    extraction_column = '_reference_month',
    cte_context = 'addresses'
) }},

contacts_base as (

    select
        cnpj_id,
        nullif(concat_ws(' ', trim(phone_area_code_1), trim(phone_number_1)), '') as phone_1,
        nullif(concat_ws(' ', trim(phone_area_code_2), trim(phone_number_2)), '') as phone_2,
        nullif(concat_ws(' ', trim(fax_area_code), trim(fax_number)), '') as fax,
        email,
        _reference_month
    from {{ ref('rfb__establishment_contacts') }}

),

{{ latest_only(
    source_cte = 'contacts_base',
    extraction_column = '_reference_month',
    cte_context = 'contacts'
) }},

municipality_bridge as (

    select
        source_municipality_code as municipality_code,
        ibge_municipality_id
    from {{ ref('bridge_rfb_ibge_municipalities') }}
    where match_status = 'MATCHED'

),

final as (

    select
        {{ generate_sk(['c.cnpj_id']) }} as establishment_sk,
        c.cnpj_id as establishment_nk,
        c.company_root_id as company_root_nk,
        c.cnpj_id as cnpj,
        c.establishment_order,
        c.establishment_check_digit,
        c.trade_name,
        c.establishment_type_description as branch_type,
        c.is_headquarters,
        c.is_branch,
        {{ generate_sk(['c.registration_status_id']) }} as registration_status_sk,
        c.registration_status_id as registration_status_nk,
        c.registration_status_id = '02' as is_active,
        {{ generate_sk(['c.registration_status_reason_code']) }} as registration_status_reason_sk,
        c.registration_status_reason_code as registration_status_reason_nk,
        c.registration_status_date,
        {{ generate_sk(['c.main_cnae_code']) }} as primary_economic_activity_sk,
        c.main_cnae_code as primary_economic_activity_nk,
        case
            when c.registration_status_id = '08' then c.registration_status_date
            else cast(null as date)
        end as closing_date,
        c.activity_start_date as opening_date,
        case
            when a.state_abbreviation = 'EX' and a.municipality_code = '9707'
                then {{ generate_sk(["'EX'"]) }}
            else dl.location_sk
        end as location_sk,
        {{ generate_sk(['a.country_code']) }} as country_sk,
        a.country_code as country_nk,
        a.state_abbreviation,
        a.postal_code as zip_code,
        a.address,
        a.street_number as address_number,
        a.address_complement,
        a.neighborhood,
        ct.phone_1,
        ct.phone_2,
        ct.fax,
        ct.email,
        c._reference_month,
        current_timestamp() as _updated_at
    from latest_core c
    left join latest_addresses a
        on c.cnpj_id = a.cnpj_id
       and c._reference_month = a._reference_month
    left join latest_contacts ct
        on c.cnpj_id = ct.cnpj_id
       and c._reference_month = ct._reference_month
    left join municipality_bridge mb
        on a.municipality_code = mb.municipality_code
    left join {{ ref('dim_location') }} dl
        on cast(mb.ibge_municipality_id as string) = dl.location_nk

)

select * from final
