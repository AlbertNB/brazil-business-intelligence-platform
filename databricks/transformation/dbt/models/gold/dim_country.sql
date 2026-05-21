{{ config(
    materialized = "table",
    tags = ["dim", "business_registry"]
) }}

select
    {{ generate_sk(['country_code']) }} as country_sk,
    country_code as country_nk,
    country_name,
    current_timestamp() as _updated_at
from {{ ref('rfb__countries') }}
