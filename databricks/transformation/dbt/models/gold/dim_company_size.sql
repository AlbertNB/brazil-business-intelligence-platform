{{ config(
    materialized = "table",
    tags = ["dim", "business_registry"]
) }}

select
    {{ generate_sk(['company_size_id']) }} as company_size_sk,
    company_size_id as company_size_nk,
    company_size_description,
    current_timestamp() as _updated_at
from {{ ref('rfb__company_sizes') }}

