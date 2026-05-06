{{ config(
    materialized = "table",
) }}

with base as (

    select
        trim(cast(id as string)) as economic_activity_id,
        trim(cast(descricao as string)) as economic_activity_description,

        from_json(
            cast(classe as string),
            'struct<
                id:string,
                descricao:string,
                grupo:struct<
                    id:string,
                    descricao:string,
                    divisao:struct<
                        id:string,
                        descricao:string,
                        secao:struct<
                            id:string,
                            descricao:string
                        >
                    >
                >,
                observacoes:array<string>
            >'
        ) as class_struct,

        from_json(cast(atividades as string), 'array<string>') as activities,
        from_json(cast(observacoes as string), 'array<string>')  as notes,

        cast(_extraction_ts as timestamp) as _extraction_ts,
        cast(_ingestion_ts as timestamp) as _ingestion_ts,
        current_timestamp() as _load_ts

    from {{ source('bronze', 'ibge__cnaes') }}
    where trim(cast(id as string)) is not null
),

{{ latest_dedup(
    source_cte = "base",
    partition_by = ["economic_activity_id"]
) }}

select
    economic_activity_id,
    economic_activity_description,

    trim(class_struct.id)                                as class_id,
    trim(class_struct.descricao)                         as class_description,
    class_struct.observacoes                             as class_notes,

    trim(class_struct.grupo.id)                          as group_id,
    trim(class_struct.grupo.descricao)                   as group_description,

    trim(class_struct.grupo.divisao.id)                  as division_id,
    trim(class_struct.grupo.divisao.descricao)           as division_description,

    trim(class_struct.grupo.divisao.secao.id)            as section_id,
    trim(class_struct.grupo.divisao.secao.descricao)     as section_description,

    activities as economic_activity_details,
    notes as economic_activity_notes,

    _ingestion_ts,
    _load_ts

from dedup