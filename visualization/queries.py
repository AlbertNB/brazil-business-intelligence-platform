from __future__ import annotations

import pandas as pd
import streamlit as st

from db import run_query


CATALOG = st.secrets["DATABRICKS_CATALOG"]
SCHEMA = st.secrets["DATABRICKS_SCHEMA"]


def get_years() -> list[int]:
    query = f"""
        select distinct year
        from {CATALOG}.{SCHEMA}.fact_location_demographics
        order by year desc
    """
    df = run_query(query)
    return df["year"].astype(int).tolist()


def get_states() -> pd.DataFrame:
    query = f"""
        select distinct
            state_abbreviation,
            state_name
        from {CATALOG}.{SCHEMA}.dim_location
        where location_type = 'municipality'
        order by state_abbreviation
    """
    return run_query(query)


def get_state_demographics(year: int) -> pd.DataFrame:
    query = f"""
        select
            d.location_sk,
            d.location_name,
            d.state_abbreviation,
            d.region_name,
            f.year,
            f.population_census,
            f.population_estimated,
            f.population_preferred,
            f.population_preferred_source,
            f.area_km2,
            f.density_preferred,
            g.geometry_geojson
        from {CATALOG}.{SCHEMA}.fact_location_demographics f
        inner join {CATALOG}.{SCHEMA}.dim_location d
            on f.location_sk = d.location_sk
        inner join {CATALOG}.{SCHEMA}.dim_location_geolocation g
            on f.location_sk = g.location_sk
        where d.location_type = 'state'
          and g.location_type = 'state'
          and f.year = {year}
    """
    return run_query(query)


def get_cnae_sections() -> pd.DataFrame:
    query = f"""
        select distinct section_id, section_description
        from {CATALOG}.{SCHEMA}.dim_economic_activity
        order by section_id
    """
    return run_query(query)


def get_cnae_divisions(section_id: str) -> pd.DataFrame:
    query = f"""
        select distinct division_id, division_description
        from {CATALOG}.{SCHEMA}.dim_economic_activity
        where section_id = '{section_id}'
        order by division_id
    """
    return run_query(query)


def get_cnae_groups(section_id: str, division_ids: tuple[str, ...]) -> pd.DataFrame:
    div_filter = ""
    if division_ids:
        ids = ", ".join(f"'{d}'" for d in division_ids)
        div_filter = f"and division_id in ({ids})"
    query = f"""
        select distinct group_id, group_description
        from {CATALOG}.{SCHEMA}.dim_economic_activity
        where section_id = '{section_id}'
          {div_filter}
        order by group_id
    """
    return run_query(query)


def get_cnae_classes(
    section_id: str,
    division_ids: tuple[str, ...],
    group_ids: tuple[str, ...],
) -> pd.DataFrame:
    filters = ""
    if division_ids:
        ids = ", ".join(f"'{d}'" for d in division_ids)
        filters += f"and division_id in ({ids})\n"
    if group_ids:
        ids = ", ".join(f"'{g}'" for g in group_ids)
        filters += f"and group_id in ({ids})\n"
    query = f"""
        select distinct class_id, class_description
        from {CATALOG}.{SCHEMA}.dim_economic_activity
        where section_id = '{section_id}'
          {filters}
        order by class_id
    """
    return run_query(query)


def get_cnae_subclasses(
    section_id: str,
    division_ids: tuple[str, ...],
    group_ids: tuple[str, ...],
    class_ids: tuple[str, ...],
) -> pd.DataFrame:
    filters = ""
    if division_ids:
        ids = ", ".join(f"'{d}'" for d in division_ids)
        filters += f"and division_id in ({ids})\n"
    if group_ids:
        ids = ", ".join(f"'{g}'" for g in group_ids)
        filters += f"and group_id in ({ids})\n"
    if class_ids:
        ids = ", ".join(f"'{c}'" for c in class_ids)
        filters += f"and class_id in ({ids})\n"
    query = f"""
        select distinct economic_activity_nk, economic_activity_name
        from {CATALOG}.{SCHEMA}.dim_economic_activity
        where section_id = '{section_id}'
          {filters}
        order by economic_activity_nk
    """
    return run_query(query)


def _ea_clauses(
    division_ids: tuple[str, ...],
    group_ids: tuple[str, ...],
    class_ids: tuple[str, ...],
    subclass_ids: tuple[str, ...],
    is_main_activity: bool | None,
    is_active: bool | None,
    ignore_mei: bool = False,
) -> tuple[str, str]:
    """Returns (ea_on_clauses, where_clause) for use in CTE queries."""
    on_parts = []
    if division_ids:
        ids = ", ".join(f"'{d}'" for d in division_ids)
        on_parts.append(f"and ea.division_id in ({ids})")
    if group_ids:
        ids = ", ".join(f"'{g}'" for g in group_ids)
        on_parts.append(f"and ea.group_id in ({ids})")
    if class_ids:
        ids = ", ".join(f"'{c}'" for c in class_ids)
        on_parts.append(f"and ea.class_id in ({ids})")
    if subclass_ids:
        ids = ", ".join(f"'{s}'" for s in subclass_ids)
        on_parts.append(f"and ea.economic_activity_nk in ({ids})")

    ea_filters = "\n                ".join(on_parts)

    conditions = []
    if is_main_activity is not None:
        conditions.append(f"f.is_main_activity = {str(is_main_activity).lower()}")
    if is_active is not None:
        conditions.append(f"f.is_active = {str(is_active).lower()}")
    if ignore_mei:
        conditions.append("f.is_mei = false")

    where_clause = ("where " + " and ".join(conditions)) if conditions else ""
    return ea_filters, where_clause


def _ea_join(section_id: str | None, division_clause: str, catalog: str, schema: str) -> str:
    if section_id is None:
        return ""
    return f"""
            inner join {catalog}.{schema}.dim_economic_activity ea
                on f.economic_activity_sk = ea.economic_activity_sk
                and ea.section_id = '{section_id}'
                {division_clause}"""


def get_economic_activity_states(
    section_id: str | None,
    division_ids: tuple[str, ...],
    group_ids: tuple[str, ...],
    class_ids: tuple[str, ...],
    subclass_ids: tuple[str, ...],
    is_main_activity: bool | None,
    is_active: bool | None,
) -> pd.DataFrame:
    division_clause, activity_where = _ea_clauses(division_ids, group_ids, class_ids, subclass_ids, is_main_activity, is_active)
    ea_join = _ea_join(section_id, division_clause, CATALOG, SCHEMA)
    query = f"""
        with fact_agg as (
            select
                f.location_sk,
                sum(f.establishments_count) as establishments_count,
                sum(f.headquarter_count)    as headquarter_count,
                sum(f.branch_count)         as branch_count,
                sum(f.simples_count)        as simples_count,
                sum(f.mei_count)            as mei_count
            from {CATALOG}.{SCHEMA}.fact_municipality_economic_activity f
            {ea_join}
            {activity_where}
            group by f.location_sk
        )
        select
            s.location_sk,
            s.location_nk,
            s.location_name,
            s.state_abbreviation,
            s.region_name,
            sum(agg.establishments_count) as establishments_count,
            sum(agg.headquarter_count)    as headquarter_count,
            sum(agg.branch_count)         as branch_count,
            sum(agg.simples_count)        as simples_count,
            sum(agg.mei_count)            as mei_count,
            g.geometry_geojson
        from {CATALOG}.{SCHEMA}.dim_location s
        inner join {CATALOG}.{SCHEMA}.dim_location_geolocation g
            on s.location_sk = g.location_sk
            and g.location_type = 'state'
        inner join {CATALOG}.{SCHEMA}.dim_location m
            on m.state_abbreviation = s.state_abbreviation
            and m.location_type = 'municipality'
        left join fact_agg agg
            on agg.location_sk = m.location_sk
        where s.location_type = 'state'
        group by s.location_sk, s.location_nk, s.location_name, s.state_abbreviation, s.region_name, g.geometry_geojson
        order by establishments_count desc nulls last
    """
    return run_query(query)


def get_economic_activity_municipalities(
    section_id: str | None,
    division_ids: tuple[str, ...],
    group_ids: tuple[str, ...],
    class_ids: tuple[str, ...],
    subclass_ids: tuple[str, ...],
    is_main_activity: bool | None,
    is_active: bool | None,
    state_abbreviation: str,
) -> pd.DataFrame:
    division_clause, activity_where = _ea_clauses(division_ids, group_ids, class_ids, subclass_ids, is_main_activity, is_active)
    ea_join = _ea_join(section_id, division_clause, CATALOG, SCHEMA)
    query = f"""
        with fact_agg as (
            select
                f.location_sk,
                sum(f.establishments_count) as establishments_count,
                sum(f.headquarter_count)    as headquarter_count,
                sum(f.branch_count)         as branch_count,
                sum(f.simples_count)        as simples_count,
                sum(f.mei_count)            as mei_count
            from {CATALOG}.{SCHEMA}.fact_municipality_economic_activity f
            {ea_join}
            {activity_where}
            group by f.location_sk
        )
        select
            m.location_sk,
            m.location_nk,
            m.location_name,
            m.state_abbreviation,
            m.region_name,
            agg.establishments_count,
            agg.headquarter_count,
            agg.branch_count,
            agg.simples_count,
            agg.mei_count,
            g.geometry_geojson
        from {CATALOG}.{SCHEMA}.dim_location m
        inner join {CATALOG}.{SCHEMA}.dim_location_geolocation g
            on m.location_sk = g.location_sk
            and g.location_type = 'municipality'
        left join fact_agg agg
            on agg.location_sk = m.location_sk
        where m.location_type = 'municipality'
          and m.state_abbreviation = '{state_abbreviation}'
        order by establishments_count desc nulls last
    """
    return run_query(query)


def get_total_establishments_by_location(
    location_type: str,
    is_main_activity: bool | None,
    is_active: bool | None,
    state_abbreviation: str | None = None,
) -> pd.DataFrame:
    conditions = ["m.location_type = 'municipality'"]
    if is_main_activity is not None:
        conditions.append(f"f.is_main_activity = {str(is_main_activity).lower()}")
    if is_active is not None:
        conditions.append(f"f.is_active = {str(is_active).lower()}")
    if state_abbreviation:
        conditions.append(f"m.state_abbreviation = '{state_abbreviation}'")
    where_clause = "where " + " and ".join(conditions)

    agg_cols = """
                sum(f.establishments_count) as total_establishments_count,
                sum(f.headquarter_count)    as total_headquarter_count,
                sum(f.branch_count)         as total_branch_count,
                sum(f.simples_count)        as total_simples_count,
                sum(f.mei_count)            as total_mei_count"""

    if location_type == "state":
        query = f"""
            select
                s.location_sk,
                {agg_cols}
            from {CATALOG}.{SCHEMA}.fact_municipality_economic_activity f
            inner join {CATALOG}.{SCHEMA}.dim_location m
                on f.location_sk = m.location_sk
            inner join {CATALOG}.{SCHEMA}.dim_location s
                on m.state_abbreviation = s.state_abbreviation
                and s.location_type = 'state'
            {where_clause}
            group by s.location_sk
        """
    else:
        query = f"""
            select
                f.location_sk,
                {agg_cols}
            from {CATALOG}.{SCHEMA}.fact_municipality_economic_activity f
            inner join {CATALOG}.{SCHEMA}.dim_location m
                on f.location_sk = m.location_sk
            {where_clause}
            group by f.location_sk
        """
    return run_query(query)


def get_establishment_metadata() -> pd.DataFrame:
    query = f"""
        select max(_updated_at) as _updated_at, max(_reference_month) as _reference_month
        from {CATALOG}.{SCHEMA}.dim_establishment
    """
    return run_query(query)


def get_location_stats(location_type: str, state_abbreviation: str | None = None) -> pd.DataFrame:
    state_filter = f"and d.state_abbreviation = '{state_abbreviation}'" if state_abbreviation else ""
    query = f"""
        select
            d.location_sk,
            f.area_km2,
            f.population_preferred
        from {CATALOG}.{SCHEMA}.fact_location_demographics f
        inner join {CATALOG}.{SCHEMA}.dim_location d
            on f.location_sk = d.location_sk
        where d.location_type = '{location_type}'
          {state_filter}
          and f.year = (select max(year) from {CATALOG}.{SCHEMA}.fact_location_demographics)
    """
    return run_query(query)


def get_municipality_demographics(year: int, state_abbreviation: str) -> pd.DataFrame:
    query = f"""
        select
            d.location_sk,
            d.location_name,
            d.state_abbreviation,
            d.region_name,
            f.year,
            f.population_census,
            f.population_estimated,
            f.population_preferred,
            f.population_preferred_source,
            f.area_km2,
            f.density_preferred,
            g.geometry_geojson
        from {CATALOG}.{SCHEMA}.fact_location_demographics f
        inner join {CATALOG}.{SCHEMA}.dim_location d
            on f.location_sk = d.location_sk
        inner join {CATALOG}.{SCHEMA}.dim_location_geolocation g
            on f.location_sk = g.location_sk
        where d.location_type = 'municipality'
          and g.location_type = 'municipality'
          and d.state_abbreviation = '{state_abbreviation}'
          and f.year = {year}
    """
    return run_query(query)
