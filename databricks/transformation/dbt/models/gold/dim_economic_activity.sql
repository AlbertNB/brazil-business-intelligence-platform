{{ config(
    materialized = "table",
    tags = ["dim", "economic_activity"]
) }}

with rfb as (

    select
        economic_activity_code as economic_activity_nk,
        upper(economic_activity_description) as economic_activity_description,
        _ingestion_ts
    from {{ ref('rfb__economic_activities') }}

),

ibge as (

    select
        economic_activity_id,

        class_id,
        class_description,
        transform(
            class_notes,
            note -> regexp_replace(note, '\\r\\n', '\\n')
        ) as class_notes,

        group_id,
        group_description,

        division_id,
        division_description,

        section_id,
        section_description,

        transform(
            economic_activity_details,
            activity -> case
                when size(split(activity, ';')) = 2 then trim(concat(
                    trim(element_at(split(activity, ';'), 2)),
                    ' ',
                    trim(element_at(split(activity, ';'), 1))
                ))
                else activity
            end
        ) as economic_activity_details,
        transform(
            economic_activity_notes,
            note -> regexp_replace(note, '\\r\\n', '\\n')
        ) as economic_activity_notes

    from {{ ref('ibge__economic_activities') }}

),

-- One row per class_id to avoid duplicates on the inferred join
ibge_by_class as (

    select *
    from ibge
    qualify row_number() over (partition by class_id order by economic_activity_id) = 1

),

final as (

    select
        {{ generate_sk(['rfb.economic_activity_nk']) }} as economic_activity_sk,

        rfb.economic_activity_nk,
        rfb.economic_activity_description      as economic_activity_name,

        coalesce(ibge_exact.class_id, ibge_by_class.class_id) as class_id,
        coalesce(ibge_exact.class_description, ibge_by_class.class_description, 'NOT_INFORMED') as class_description,
        coalesce(
            ibge_exact.class_notes,
            ibge_by_class.class_notes,
            cast(array() as array<string>)
        ) as class_notes,

        coalesce(ibge_exact.group_id, ibge_by_class.group_id) as group_id,
        coalesce(ibge_exact.group_description, ibge_by_class.group_description, 'NOT_INFORMED') as group_description,

        coalesce(ibge_exact.division_id, ibge_by_class.division_id) as division_id,
        coalesce(ibge_exact.division_description, ibge_by_class.division_description, 'NOT_INFORMED') as division_description,

        coalesce(ibge_exact.section_id, ibge_by_class.section_id) as section_id,
        coalesce(ibge_exact.section_description, ibge_by_class.section_description, 'NOT_INFORMED') as section_description,

        coalesce(ibge_exact.economic_activity_details, cast(array() as array<string>)) as economic_activity_details,
        coalesce(ibge_exact.economic_activity_notes, cast(array() as array<string>)) as economic_activity_notes,

        case
            when ibge_exact.economic_activity_id is not null then 'IBGE'
            when ibge_by_class.class_id is not null then 'INFERRED'
            else 'UNKNOWN'
        end as hierarchy_source,

        current_timestamp() as _updated_at

    from rfb
    left join ibge as ibge_exact
        on ibge_exact.economic_activity_id = rfb.economic_activity_nk
    left join ibge_by_class
        on ibge_by_class.class_id = substring(rfb.economic_activity_nk, 1, 5)
        and ibge_exact.economic_activity_id is null

)

select * from final
