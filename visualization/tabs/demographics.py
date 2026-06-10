from __future__ import annotations

import plotly.express as px
import streamlit as st

from i18n import t
from map import prepare_dataframe, render_map
from queries import (
    get_municipality_demographics,
    get_state_demographics,
    get_states,
    get_years,
)

_HOVER_DATA = {
    "state_abbreviation": True,
    "region_name": True,
    "population_preferred": ":,.0f",
    "density_preferred": ":,.2f",
    "area_km2": ":,.2f",
}


def render(lang: str = "pt") -> None:
    years = get_years()

    metric_options = {
        "population_preferred": t("population", lang),
        "density_preferred": t("density", lang),
    }

    hover_labels = {
        "state_abbreviation": t("uf", lang),
        "region_name": t("region", lang),
        "population_preferred": t("population", lang),
        "density_preferred": t("density_habkm2", lang),
        "area_km2": t("area_km2", lang),
    }

    with st.expander(t("filters", lang), expanded=True):
        tab_period, tab_location = st.tabs([t("tab_period", lang), t("tab_location", lang)])

        with tab_period:
            selected_year = st.selectbox(t("year", lang), years, key="demo_year")

        with tab_location:
            level_key = st.radio(
                t("level", lang),
                ["states", "municipalities"],
                format_func=lambda k: t(k, lang),
                horizontal=True,
                key="demo_level",
            )

            selected_state = None
            if level_key == "municipalities":
                states_df = get_states()
                selected_state = st.selectbox(
                    t("state", lang),
                    [None] + states_df["state_abbreviation"].tolist(),
                    format_func=lambda x: t("select_state_prompt", lang) if x is None else x,
                    key="demo_state",
                )

    with st.expander(t("display", lang), expanded=True):
        col_view, col_metric = st.columns(2)

        view_key = col_view.radio(
            t("visualization", lang),
            ["map", "chart", "table"],
            format_func=lambda k: t(k, lang),
            key="demo_view",
        )

        metric_column = col_metric.selectbox(
            t("metric", lang),
            list(metric_options.keys()),
            format_func=metric_options.get,
            key="demo_metric",
        )

    if level_key == "municipalities" and selected_state is None:
        st.info(t("select_state_warn", lang))
        st.stop()

    st.divider()

    if level_key == "states":
        df = get_state_demographics(selected_year)
        geo_label = f"{t('geo_state', lang)} — {selected_year}"
    else:
        df = get_municipality_demographics(selected_year, selected_state)
        geo_label = f"{t('geo_municipality', lang)} — {selected_state} — {selected_year}"

    if df.empty:
        st.warning(t("no_data", lang))
        st.stop()

    df = prepare_dataframe(df)

    st.header(f"{metric_options[metric_column]} {geo_label}")

    def _fill_metrics(m1, m2, m3, df_m, loc_label):
        if loc_label:
            m1.metric(t("locality", lang), loc_label)
        else:
            unit_label = t("states", lang) if level_key == "states" else t("municipalities", lang)
            m1.metric(unit_label, f"{len(df_m):,}")
        pop_total = df_m["population_preferred"].sum() / 1_000_000
        density_mean = df_m["density_preferred"].mean()
        m2.metric(t("total_population", lang), f"{pop_total:.2f}M")
        m3.metric(t("avg_density", lang), f"{density_mean:.2f} {t('density_unit', lang)}")

    if view_key == "map":
        metrics_slot = st.container()
        render_map(
            df,
            metric_column=metric_column,
            metric_label=metric_options[metric_column],
            hover_data=_HOVER_DATA,
            hover_labels=hover_labels,
        )
        selected_sk = st.session_state.get("_map_selected_sk")
        if selected_sk and selected_sk in df["location_sk"].values:
            df_metrics = df[df["location_sk"] == selected_sk]
            location_label = df_metrics["location_name"].iloc[0]
        else:
            df_metrics = df
            location_label = None
        with metrics_slot:
            m1, m2, m3 = st.columns(3)
            _fill_metrics(m1, m2, m3, df_metrics, location_label)

    elif view_key == "chart":
        m1, m2, m3 = st.columns(3)
        _fill_metrics(m1, m2, m3, df, None)
        top = df.nlargest(50, metric_column).sort_values(metric_column)
        top["label"] = top["location_name"] + " – " + top["state_abbreviation"]

        fig = px.bar(
            top,
            x=metric_column,
            y="label",
            orientation="h",
            color=metric_column,
            color_continuous_scale=["#ffffcc", "#fed976", "#fd8d3c", "#e31a1c", "#800026"],
            labels={metric_column: metric_options[metric_column], "label": ""},
            title=f"{t('top_50_by', lang)} {metric_options[metric_column]}",
        )
        fig.update_layout(
            height=1200,
            coloraxis_showscale=False,
            margin={"r": 0, "t": 40, "l": 0, "b": 0},
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
        )
        st.plotly_chart(fig, use_container_width=True)

    else:
        m1, m2, m3 = st.columns(3)
        _fill_metrics(m1, m2, m3, df, None)
        st.dataframe(
            df[["location_name", "state_abbreviation", "region_name",
                "population_preferred", "population_preferred_source",
                "area_km2", "density_preferred"]]
            .sort_values(metric_column, ascending=False)
            .rename(columns={
                "location_name": t("locality", lang),
                "state_abbreviation": t("uf", lang),
                "region_name": t("region", lang),
                "population_preferred": t("population_preferred", lang),
                "population_preferred_source": t("source", lang),
                "area_km2": t("area_km2", lang),
                "density_preferred": t("density_habkm2", lang),
            }),
            use_container_width=True,
            hide_index=True,
        )
