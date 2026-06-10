# Brazil BI Platform — Visualization Layer

Interactive dashboard built with Streamlit for exploring Brazilian business and demographic data sourced from Databricks.

## Run locally

```bash
pip install -r requirements.txt
streamlit run app.py
```

Requires a `.streamlit/secrets.toml` with Databricks connection details (see `.streamlit/secrets.toml.example`).

## Pages

### Atividade Econômica / Economic Activity

Explores the distribution of CNAE-classified establishments across Brazilian states and municipalities.

**Filters:**
- **CNAE hierarchy** — filter by section, division, group, class, or subclass
- **Activity type** — main activity only, secondary only, or both (enabled when a single subclass is selected)
- **Status** — active, inactive, or all establishments
- **Ignore MEI** — excludes microempreendedores individuais from counts
- **Level** — states or municipalities (municipality view requires selecting a state)

**Metrics:** total establishments, headquarters, branches, Simples Nacional, MEI

**Units:** absolute count, local share (% of all activities in the locality), per 100k inhabitants, per km²

**Views:** choropleth map, horizontal bar chart (top 50), data table

---

### Demografia / Demographics

Explores population and demographic density across Brazilian states and municipalities.

**Filters:**
- **Year** — reference year for the demographic data
- **Level** — states or municipalities (municipality view requires selecting a state)

**Metrics:** preferred population, demographic density (inh/km²)

**Views:** choropleth map, horizontal bar chart (top 50), data table

---

## Features

- Bilingual interface (🇧🇷 Português / 🇺🇸 English) with browser language auto-detection
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
