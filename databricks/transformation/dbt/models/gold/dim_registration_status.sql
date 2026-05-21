{{ config(
    materialized = "table",
    tags = ["dim", "business_registry"]
) }}

select
    {{ generate_sk(['registration_status_id']) }} as registration_status_sk,
    registration_status_id as registration_status_nk,
    registration_status_description,
    registration_status_id = '02' as is_active,
    current_timestamp() as _updated_at
from {{ ref('rfb__registration_statuses') }}

