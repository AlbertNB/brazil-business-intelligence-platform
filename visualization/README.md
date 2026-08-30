# Brazil BI Platform — Visualization Layer

Interactive dashboard built with Streamlit for exploring Brazilian business and demographic data sourced from Databricks.

## Run locally

```bash
pip install -r requirements.txt
streamlit run app.py
```

Requires a `.streamlit/secrets.toml` with Databricks connection details (see `.streamlit/secrets.toml.example`).

## Features

- Translated UI (Português / English) with browser language auto-detection — data values (CNAE names, locations) remain in Portuguese
- Choropleth maps with click-to-filter metric cards
- Color scale aligned between map and bar chart (yellow → dark red)
- Metadata footer showing last update date and Federal Revenue reference month

## Required tables

- gold.dim_location
- gold.dim_location_geolocation
- gold.dim_economic_activity
- gold.dim_establishment
- gold.fact_location_demographics
- gold.fact_municipality_economic_activity
