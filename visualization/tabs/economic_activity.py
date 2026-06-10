from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from i18n import t
from map import render_map
from queries import (
    get_cnae_classes,
    get_cnae_divisions,
    get_cnae_groups,
    get_cnae_sections,
    get_cnae_subclasses,
    get_economic_activity_municipalities,
    get_economic_activity_states,
    get_location_stats,
    get_states,
    get_total_establishments_by_location,
)

_COUNT_COLS = [
    "establishments_count",
    "headquarter_count",
    "branch_count",
    "simples_count",
    "mei_count",
]

_HEADER_HOVER = {
    "location_nk": True,
    "state_abbreviation": True,
    "region_name": True,
}

_NORM_FMT = {
    "pct": ":.2f",
    "per100k": ":.2f",
    "perkm2": ":.4f",
}

_ACTIVITY_TO_IS_MAIN: dict[str, bool | None] = {
    "main_only": True,
    "both": None,
    "secondary_only": False,
}

_STATUS_TO_BOOL: dict[str, bool | None] = {
    "active": True,
    "inactive": False,
    "all": None,
}


def render(lang: str = "pt") -> None:
    metric_options = {
        "establishments_count": t("establishments", lang),
        "headquarter_count": t("headquarters", lang),
        "branch_count": t("branches", lang),
        "simples_count": t("simples", lang),
        "mei_count": t("mei", lang),
    }

    header_labels = {
        "location_nk": t("ibge_code", lang),
        "state_abbreviation": t("uf", lang),
        "region_name": t("region", lang),
    }

    abs_labels = {col: metric_options[col] for col in _COUNT_COLS}

    sections_df = get_cnae_sections()
    section_ids = sections_df["section_id"].tolist()
    section_labels = (
        sections_df["section_id"] + " – " + sections_df["section_description"]
    ).tolist()

    with st.expander(t("filters", lang), expanded=True):
        tab_cnae, tab_config, tab_location = st.tabs([
            t("tab_cnae", lang), t("tab_options", lang), t("tab_location", lang)
        ])

        with tab_cnae:
            all_sections_label = t("all_sections", lang)
            _section_display = dict(zip(section_ids, section_labels))

            selected_section = st.selectbox(
                t("cnae_section", lang),
                [None] + section_ids,
                format_func=lambda x: all_sections_label if x is None else _section_display[x],
                key="ea_section",
            )
            selected_section_label = (
                all_sections_label if selected_section is None else _section_display[selected_section]
            )

            if selected_section is not None:
                divisions_df = get_cnae_divisions(selected_section)
                division_ids = divisions_df["division_id"].tolist()
                division_labels = (
                    divisions_df["division_id"] + " – " + divisions_df["division_description"]
                ).tolist()

                selected_division_labels = st.multiselect(
                    t("division", lang), division_labels, key="ea_division"
                )
                selected_divisions = tuple(
                    division_ids[division_labels.index(lbl)] for lbl in selected_division_labels
                )

                groups_df = get_cnae_groups(selected_section, selected_divisions)
                group_ids = groups_df["group_id"].tolist()
                group_labels = (
                    groups_df["group_id"] + " – " + groups_df["group_description"]
                ).tolist()

                selected_group_labels = st.multiselect(
                    t("group", lang), group_labels, key="ea_group",
                    disabled=not group_ids,
                )
                selected_groups = tuple(
                    group_ids[group_labels.index(lbl)] for lbl in selected_group_labels
                )

                classes_df = get_cnae_classes(selected_section, selected_divisions, selected_groups)
                class_ids = classes_df["class_id"].tolist()
                class_labels = (
                    classes_df["class_id"] + " – " + classes_df["class_description"]
                ).tolist()

                selected_class_labels = st.multiselect(
                    t("class_label", lang), class_labels, key="ea_class",
                    disabled=not class_ids,
                )
                selected_classes = tuple(
                    class_ids[class_labels.index(lbl)] for lbl in selected_class_labels
                )

                subclasses_df = get_cnae_subclasses(
                    selected_section, selected_divisions, selected_groups, selected_classes
                )
                subclass_ids = subclasses_df["economic_activity_nk"].tolist()
                subclass_labels = (
                    subclasses_df["economic_activity_nk"] + " – " + subclasses_df["economic_activity_name"]
                ).tolist()

                selected_subclass_labels = st.multiselect(
                    t("subclass", lang), subclass_labels, key="ea_subclass",
                    disabled=not subclass_ids,
                )
                selected_subclasses = tuple(
                    subclass_ids[subclass_labels.index(lbl)] for lbl in selected_subclass_labels
                )
            else:
                selected_division_labels = []
                selected_divisions = ()
                selected_group_labels = []
                selected_groups = ()
                selected_class_labels = []
                selected_classes = ()
                selected_subclass_labels = []
                selected_subclasses = ()

            activity_radio_enabled = len(selected_subclasses) == 1
            if not activity_radio_enabled:
                st.session_state["ea_activity_type"] = "main_only"
            activity_type_key = st.radio(
                t("activity_type", lang),
                ["main_only", "both", "secondary_only"],
                format_func=lambda k: t(f"activity_{k}", lang),
                key="ea_activity_type",
                horizontal=True,
                disabled=not activity_radio_enabled,
            )

        with tab_config:
            col_active, col_mei = st.columns(2)

            status_key = col_active.radio(
                t("status", lang),
                ["active", "inactive", "all"],
                format_func=lambda k: t(f"status_{k}", lang),
                key="ea_is_active",
            )
            active_filter = _STATUS_TO_BOOL[status_key]

            ignore_mei = col_mei.checkbox(t("ignore_mei", lang), key="ea_ignore_mei")

        with tab_location:
            level_key = st.radio(
                t("level", lang),
                ["states", "municipalities"],
                format_func=lambda k: t(k, lang),
                key="ea_level",
                horizontal=True,
            )

            selected_state = None
            if level_key == "municipalities":
                states_df = get_states()
                selected_state = st.selectbox(
                    t("state", lang),
                    [None] + states_df["state_abbreviation"].tolist(),
                    format_func=lambda x: t("select_state_prompt", lang) if x is None else x,
                    key="ea_state",
                )

    with st.expander(t("display", lang), expanded=True):
        col_view, col_metric, col_unit = st.columns([2, 2, 3])

        view_key = col_view.radio(
            t("visualization", lang),
            ["map", "chart", "table"],
            format_func=lambda k: t(k, lang),
            key="ea_view",
        )

        metric_column = col_metric.selectbox(
            t("metric", lang),
            list(metric_options.keys()),
            format_func=metric_options.get,
            key="ea_metric",
        )

        unit_key = col_unit.radio(
            t("unit", lang),
            ["abs", "pct", "per100k", "perkm2"],
            format_func=lambda k: t(f"unit_{k}", lang),
            key="ea_unit",
            help=t("unit_help", lang),
        )

    if level_key == "municipalities" and selected_state is None:
        st.info(t("select_state_warn", lang))
        st.stop()

    st.divider()

    is_main = _ACTIVITY_TO_IS_MAIN[activity_type_key] if activity_radio_enabled else True

    if level_key == "states":
        df = get_economic_activity_states(
            selected_section, selected_divisions, selected_groups, selected_classes,
            selected_subclasses, is_main, active_filter
        )
        geo_label = t("geo_state", lang)
        location_type = "state"
    else:
        df = get_economic_activity_municipalities(
            selected_section, selected_divisions, selected_groups, selected_classes,
            selected_subclasses, is_main, active_filter, selected_state
        )
        geo_label = f"{t('geo_municipality', lang)} — {selected_state}"
        location_type = "municipality"

    if df.empty:
        st.warning(t("no_data", lang))
        st.stop()

    if ignore_mei:
        for col in ("establishments_count", "simples_count"):
            df[col] = pd.to_numeric(df[col], errors="coerce") - pd.to_numeric(df["mei_count"], errors="coerce")
        df["mei_count"] = 0

    if unit_key == "abs":
        df["_metric"] = df[metric_column]
        display_label = metric_options[metric_column]
        hover_data = {
            **_HEADER_HOVER,
            **{col: ":,.0f" for col in _COUNT_COLS},
        }
        hover_labels = {**header_labels, **abs_labels}
    else:
        suffix = t(f"unit_{unit_key}_suffix", lang)
        norm_fmt = _NORM_FMT[unit_key]

        if unit_key == "pct":
            totals_df = get_total_establishments_by_location(
                location_type, is_main, active_filter,
                selected_state if level_key == "municipalities" else None,
            )
            df = df.merge(totals_df, on="location_sk", how="left")
            for col in _COUNT_COLS:
                total_col = f"total_{col}"
                df[total_col] = pd.to_numeric(df[total_col], errors="coerce")
                df[f"_norm_{col}"] = df[col] / df[total_col] * 100
        else:
            stats = get_location_stats(location_type, selected_state if level_key == "municipalities" else None)
            df = df.merge(stats, on="location_sk", how="left")
            df["population_preferred"] = pd.to_numeric(df["population_preferred"], errors="coerce")
            df["area_km2"] = pd.to_numeric(df["area_km2"], errors="coerce")
            for col in _COUNT_COLS:
                if unit_key == "per100k":
                    df[f"_norm_{col}"] = df[col] / (df["population_preferred"] / 100_000)
                else:
                    df[f"_norm_{col}"] = df[col] / df["area_km2"]

        df["_metric"] = df[f"_norm_{metric_column}"]
        display_label = f"{metric_options[metric_column]} ({suffix})"
        hover_data = {
            **_HEADER_HOVER,
            **{col: ":,.0f" for col in _COUNT_COLS},
            **{f"_norm_{col}": norm_fmt for col in _COUNT_COLS},
        }
        hover_labels = {
            **header_labels,
            **abs_labels,
            **{f"_norm_{col}": f"{metric_options[col]} ({suffix})" for col in _COUNT_COLS},
        }

    if selected_subclass_labels:
        section_title = ", ".join(selected_subclass_labels)
    elif selected_class_labels:
        section_title = ", ".join(selected_class_labels)
    elif selected_group_labels:
        section_title = ", ".join(selected_group_labels)
    elif selected_division_labels:
        section_title = ", ".join(selected_division_labels)
    else:
        section_title = selected_section_label if selected_section else all_sections_label

    st.header(f"{section_title} — {geo_label}")
    st.caption(
        {
            None: t("caption_both", lang),
            True: t("caption_main", lang),
            False: t("caption_secondary", lang),
        }[is_main]
    )

    for col in _COUNT_COLS:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    def _fill_metrics(m1, m2, m3, m4, df_m, loc_label):
        if loc_label:
            m1.metric(t("locality", lang), loc_label)
        else:
            m1.metric(
                t("states", lang) if level_key == "states" else t("municipalities", lang),
                f"{len(df):,}",
            )
        m2.metric(t("establishments", lang), f"{int(df_m['establishments_count'].sum()):,}")
        m3.metric(t("mei", lang), f"{int(df_m['mei_count'].sum()):,}")
        m4.metric(t("simples", lang), f"{int(df_m['simples_count'].sum()):,}")

    if view_key == "map":
        metrics_slot = st.container()
        render_map(
            df,
            metric_column="_metric",
            metric_label=display_label,
            hover_data=hover_data,
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
            m1, m2, m3, m4 = st.columns(4)
            _fill_metrics(m1, m2, m3, m4, df_metrics, location_label)

    elif view_key == "chart":
        m1, m2, m3, m4 = st.columns(4)
        _fill_metrics(m1, m2, m3, m4, df, None)
        top = df.nlargest(50, "_metric").sort_values("_metric")
        top["label"] = top["location_name"] + " – " + top["state_abbreviation"]

        fig = px.bar(
            top,
            x="_metric",
            y="label",
            orientation="h",
            color="_metric",
            color_continuous_scale=["#ffffcc", "#fed976", "#fd8d3c", "#e31a1c", "#800026"],
            labels={"_metric": display_label, "label": ""},
            title=f"{t('top_50_by', lang)} {display_label}",
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
        m1, m2, m3, m4 = st.columns(4)
        _fill_metrics(m1, m2, m3, m4, df, None)
        st.dataframe(
            df.drop(columns=["location_sk", "geometry_geojson"]).rename(columns={
                "location_nk": t("ibge_code", lang),
                "location_name": t("locality", lang),
                "state_abbreviation": t("uf", lang),
                "region_name": t("region", lang),
                "establishments_count": t("establishments", lang),
                "headquarter_count": t("headquarters", lang),
                "branch_count": t("branches", lang),
                "simples_count": t("simples", lang),
                "mei_count": t("mei", lang),
            }),
            use_container_width=True,
            hide_index=True,
        )
