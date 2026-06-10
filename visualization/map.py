from __future__ import annotations

import json
import math

import pandas as pd
import plotly.graph_objects as go
import streamlit as st


def _outer_rings(geom: dict) -> list:
    if geom["type"] == "Polygon":
        return [geom["coordinates"][0]]
    if geom["type"] == "MultiPolygon":
        return [polygon[0] for polygon in geom["coordinates"]]
    return []


def _build_geojson_and_view(rows: list[dict]) -> tuple[dict, dict, int]:
    """Single pass: builds GeoJSON and computes map center/zoom from parsed geometries."""
    features = []
    min_lon = min_lat = float("inf")
    max_lon = max_lat = float("-inf")

    for row in rows:
        geom = json.loads(row["geometry_geojson"])

        for ring in _outer_rings(geom):
            for lon, lat, *_ in ring:
                if lat < min_lat: min_lat = lat
                if lat > max_lat: max_lat = lat
                if lon < min_lon: min_lon = lon
                if lon > max_lon: max_lon = lon

        features.append({
            "type": "Feature",
            "id": row["location_sk"],
            "properties": {
                "location_sk": row["location_sk"],
                "location_name": row["location_name"],
            },
            "geometry": geom,
        })

    geojson = {"type": "FeatureCollection", "features": features}
    center = {"lat": (min_lat + max_lat) / 2, "lon": (min_lon + max_lon) / 2}
    span = max(max_lat - min_lat, max_lon - min_lon)
    zoom = max(3, min(10, round(8.5 - math.log2(max(span, 0.1)))))
    return geojson, center, zoom


def prepare_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["location_sk"] = df["location_sk"].astype(str)

    numeric_cols = [
        "population_census",
        "population_estimated",
        "population_preferred",
        "area_km2",
        "density_preferred",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def _build_hover(
    hover_data: dict,
    hover_labels: dict,
    df: pd.DataFrame,
    suffix: str = "",
) -> tuple[list[str], list, str]:
    """Returns (cols, customdata, hovertemplate) for a trace."""
    cols = [c for c in hover_data if c in df.columns]
    tpl = "<b>%{hovertext}</b>"
    for i, col in enumerate(cols):
        fmt = hover_data[col]
        label = hover_labels.get(col, col.replace("_", " ").title())
        if fmt is True:
            tpl += f"<br>{label}: %{{customdata[{i}]}}"
        else:
            tpl += f"<br>{label}: %{{customdata[{i}]:{fmt.lstrip(':')}}}"
    if suffix:
        tpl += f"<br>{suffix}"
    tpl += "<extra></extra>"
    customdata = df[cols].values if cols else None
    return cols, customdata, tpl


def render_map(
    df: pd.DataFrame,
    metric_column: str,
    metric_label: str,
    hover_data: dict | None = None,
    hover_labels: dict | None = None,
):
    df = prepare_dataframe(df)
    geojson, center, zoom = _build_geojson_and_view(df.to_dict("records"))

    _hover_data = hover_data or {}
    _hover_labels = hover_labels or {}

    # Clear stale selection when dataset changes (e.g. state ↔ municipality switch)
    selected_sk = st.session_state.get("_map_selected_sk")
    if selected_sk and selected_sk not in df["location_sk"].values:
        selected_sk = None
        st.session_state.pop("_map_selected_sk", None)

    # Plotly drops NaN regions entirely — split into two traces so all
    # locations render: gray for no-data, colored for data.
    mask = df[metric_column].notna()
    df_data = df[mask]
    df_null = df[~mask]

    fig = go.Figure()

    # ── Gray base for locations with no data ──────────────────────────────
    if not df_null.empty:
        _, null_customdata, null_tpl = _build_hover(
            _hover_data, _hover_labels, df_null, suffix="<i>Sem dados</i>"
        )
        fig.add_trace(go.Choroplethmap(
            name="",
            geojson=geojson,
            locations=df_null["location_sk"],
            z=[0] * len(df_null),
            colorscale=[[0, "#cccccc"], [1, "#cccccc"]],
            showscale=False,
            marker={"opacity": 0.65, "line": {"width": 0.5, "color": "black"}},
            hovertext=df_null["location_name"],
            customdata=null_customdata,
            hovertemplate=null_tpl,
        ))

    # ── Colored layer for locations with data ─────────────────────────────
    if not df_data.empty:
        _, data_customdata, data_tpl = _build_hover(
            _hover_data, _hover_labels, df_data
        )
        fig.add_trace(go.Choroplethmap(
            name="",
            geojson=geojson,
            locations=df_data["location_sk"],
            z=df_data[metric_column],
            colorscale=[
                [0.00, "#ffffcc"],
                [0.25, "#fed976"],
                [0.50, "#fd8d3c"],
                [0.75, "#e31a1c"],
                [1.00, "#800026"],
            ],
            colorbar={"title": {"text": metric_label}},
            showscale=True,
            marker={"opacity": 0.85, "line": {"width": 0.5, "color": "black"}},
            hovertext=df_data["location_name"],
            customdata=data_customdata,
            hovertemplate=data_tpl,
        ))

    fig.update_layout(
        map={"style": "white-bg", "center": center, "zoom": zoom,
             "bounds": {"west": -120, "east": 15, "south": -80, "north": 50}},
        height=680,
        margin={"r": 0, "t": 0, "l": 0, "b": 0},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        showlegend=False,
    )

    event = st.plotly_chart(fig, use_container_width=True, on_select="rerun")

    if event:
        points = (event.selection or {}).get("points", [])
        if points:
            clicked = points[0].get("location")
            if clicked:
                if clicked == selected_sk:
                    st.session_state.pop("_map_selected_sk", None)
                else:
                    st.session_state["_map_selected_sk"] = clicked
        else:
            st.session_state.pop("_map_selected_sk", None)


