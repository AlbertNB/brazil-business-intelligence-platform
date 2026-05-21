{{ config(
    materialized = "table",
    tags = ["dim", "business_registry"]
) }}

select
    {{ generate_sk(['registration_status_reason_code']) }} as registration_status_reason_sk,
    registration_status_reason_code as registration_status_reason_nk,
    registration_status_reason_description,
    current_timestamp() as _updated_at
from {{ ref('rfb__registration_status_reasons') }}
