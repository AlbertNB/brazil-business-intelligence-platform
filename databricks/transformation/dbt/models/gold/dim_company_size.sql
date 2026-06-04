{{ config(
    materialized = "table",
    tags = ["dim", "business_registry"]
) }}

select
    {{ generate_sk(["coalesce(company_size_id, '00')"]) }} as company_size_sk,
    coalesce(company_size_id, '00')                        as company_size_nk,
    coalesce(company_size_description, 'NOT_INFORMED')     as company_size_description,
    current_timestamp() as _updated_at
from {{ ref('rfb__company_sizes') }}

