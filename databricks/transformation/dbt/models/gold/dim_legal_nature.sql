{{ config(
    materialized = "table",
    tags = ["dim", "business_registry"]
) }}

select
    {{ generate_sk(['legal_nature_code']) }} as legal_nature_sk,
    legal_nature_code as legal_nature_nk,
    legal_nature_description,
    current_timestamp() as _updated_at
from {{ ref('rfb__legal_natures') }}
