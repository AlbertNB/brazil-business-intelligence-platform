{{ config(
    materialized = 'incremental',
    unique_key = ['cnpj_id', 'is_main_activity', 'cnae_code', '_reference_month']
) }}

with source as (

    select
        concat(
            trim(cast(_c0 as string)),
            trim(cast(_c1 as string)),
            trim(cast(_c2 as string))
        )                                                                                   as cnpj_id,
        nullif(trim(cast(_c11 as string)), '')                                              as main_cnae_code,
        nullif(trim(cast(_c12 as string)), '')                                              as secondary_cnae_codes,
        trim(cast(_reference_month as string))                                               as _reference_month,
        _ingestion_ts

    from {{ source('bronze', 'rfb__estabelecimentos') }}
    where _c0 is not null
            and {{ incremental_statement('_reference_month') }}

),

main_activities as (

    select
        cnpj_id,
        true                                                                                as is_main_activity,
        main_cnae_code                                                                      as cnae_code,
        _reference_month,
        _ingestion_ts
    from source
    where main_cnae_code is not null

),

secondary_activities as (

    select
        cnpj_id,
        false                                                                               as is_main_activity,
        trim(exploded_cnae)                                                                 as cnae_code,
        _reference_month,
        _ingestion_ts
    from source
    lateral view explode(split(secondary_cnae_codes, ',')) t as exploded_cnae
    where secondary_cnae_codes is not null

),

activities as (

    select * from main_activities
    union all
    select * from secondary_activities

)

select
    cnpj_id,
    is_main_activity,
    cnae_code,
    _reference_month,
    _ingestion_ts,
    current_timestamp() as _load_ts

from activities
where cnae_code is not null
  and cnae_code != ''
