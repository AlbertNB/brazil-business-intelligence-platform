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
    trim(classe_struct.grupo.divisao.secao.id)            as secao_id,
    trim(classe_struct.grupo.divisao.secao.descricao)     as secao_descricao,

    trim(classe_struct.grupo.divisao.id)                  as divisao_id,
    trim(classe_struct.grupo.divisao.descricao)           as divisao_descricao,

    trim(classe_struct.grupo.id)                          as grupo_id,
    trim(classe_struct.grupo.descricao)                   as grupo_descricao,

    trim(classe_struct.id)                                as classe_id,
    trim(classe_struct.descricao)                         as classe_descricao,
    classe_struct.observacoes                             as classe_observacoes,

    cnae_id,
    cnae_description,

    atividades as atividades_cnae,
    observacoes as observacoes_cnae,

    _ingestion_ts,
    _load_ts

from dedup