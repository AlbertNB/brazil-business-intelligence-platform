{{ config(
    materialized = "table",
) }}

with base as (

    select
        trim(cast(id as string)) as cnae_id,
        trim(cast(descricao as string)) as cnae_description,

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
        ) as classe_struct,

        from_json(cast(atividades as string), 'array<string>') as atividades,
        from_json(cast(observacoes as string), 'array<string>')  as observacoes,

        cast(_extraction as timestamp) as _extraction,
        cast(_ingestion_ts as timestamp) as _ingestion_ts,
        current_timestamp() as _load_ts

    from {{ source('bronze', 'ibge__cnaes') }}
    where trim(cast(id as string)) is not null
),

{{ latest_dedup(
    source_cte = "base",
    partition_by = ["cnae_id"]
) }}

select
    cnae_id,
    cnae_description,

    trim(classe_struct.id)                                as class_id,
    trim(classe_struct.descricao)                         as class_description,
    classe_struct.observacoes                             as class_notes,

    trim(classe_struct.grupo.id)                          as group_id,
    trim(classe_struct.grupo.descricao)                   as group_description,

    trim(classe_struct.grupo.divisao.id)                  as division_id,
    trim(classe_struct.grupo.divisao.descricao)           as division_description,

    trim(classe_struct.grupo.divisao.secao.id)            as section_id,
    trim(classe_struct.grupo.divisao.secao.descricao)     as section_description,

    atividades as cnae_activities,
    observacoes as cnae_notes,

    _ingestion_ts,
    _load_ts

from dedup