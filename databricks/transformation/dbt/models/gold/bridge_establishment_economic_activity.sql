{{ config(
    materialized = "table",
    tags = ["bridge", "business_registry"]
) }}

with base as (

    select
        cnpj_id,
        cnae_code,
        is_main_activity,
        _reference_month
    from {{ ref('rfb__establishment_activities') }}

),

{{ latest_only(
    source_cte = 'base',
    extraction_column = '_reference_month'
) }},

deduplicated as (

    select
        cnpj_id,
        cnae_code,
        is_main_activity,
        row_number() over (
            partition by cnpj_id, cnae_code
            order by case when is_main_activity then 0 else 1 end
        ) as row_num
    from latest

),

final as (

    select
        {{ generate_sk(['d.cnpj_id']) }} as establishment_sk,
        {{ generate_sk(['d.cnae_code']) }} as economic_activity_sk,
        case
            when d.is_main_activity then 'main'
            else 'secondary'
        end as activity_type,
        d.is_main_activity,
        current_timestamp() as _updated_at
    from deduplicated d
    where d.row_num = 1

)

select * from final
