{{ config(
    materialized = "table",
    tags = ["dim", "business_registry"]
) }}

select
    {{ generate_sk(['qualification_code']) }} as qualification_sk,
    qualification_code as qualification_nk,
    qualification_description,
    current_timestamp() as _updated_at
from {{ ref('rfb__qualifications') }}
