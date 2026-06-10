from __future__ import annotations

_T: dict[str, dict[str, str]] = {
    "pt": {
        # Sidebar / metadata
        "updated_at": "🕒 Atualizado em",
        "ref_month": "📅 Ref. Receita Federal",
        # Navigation
        "page_economic_activity": "Atividade Econômica",
        "page_demographics": "Demografia",
        # Common
        "filters": "🔍 Filtros",
        "display": "📊 Exibição",
        "level": "Nível",
        "states": "Estados",
        "municipalities": "Municípios",
        "state": "Estado",
        "select_state_prompt": "Selecione um estado...",
        "select_state_warn": "Selecione um estado para visualizar os municípios.",
        "visualization": "Visualização",
        "map": "Mapa",
        "chart": "Gráfico",
        "table": "Tabela",
        "metric": "Métrica",
        "no_data": "Nenhum dado encontrado.",
        "locality": "Localidade",
        "uf": "UF",
        "region": "Região",
        # Economic Activity — CNAE tab
        "tab_cnae": "Atividade Econômica",
        "tab_options": "Opções",
        "tab_location": "Localização",
        "cnae_section": "Seção CNAE",
        "all_sections": "Todas as seções",
        "division": "Divisão",
        "group": "Grupo",
        "class_label": "Classe",
        "subclass": "Subclasse",
        "activity_type": "Tipo de atividade",
        "activity_main_only": "Apenas Principal",
        "activity_both": "Ambas",
        "activity_secondary_only": "Apenas Secundária",
        # Economic Activity — Options tab
        "status": "Status",
        "status_active": "Ativos",
        "status_inactive": "Inativos",
        "status_all": "Todos",
        "ignore_mei": "Ignorar MEI",
        # Economic Activity — Display
        "unit": "Unidade",
        "unit_abs": "Números Absolutos",
        "unit_pct": "Participação local",
        "unit_per100k": "Por 100 mil hab.",
        "unit_perkm2": "Por km²",
        "unit_pct_suffix": "% local",
        "unit_per100k_suffix": "por 100 mil hab.",
        "unit_perkm2_suffix": "por km²",
        "unit_help": (
            "**Números Absolutos** — contagem bruta de estabelecimentos.\n\n"
            "**Participação local** — participação da atividade selecionada no total de estabelecimentos "
            "da própria localidade em todas as atividades econômicas.\n\n"
            "**Por 100 mil hab.** — contagem normalizada pela população estimada, "
            "permitindo comparar localidades de tamanhos diferentes.\n\n"
            "**Por km²** — densidade de estabelecimentos em relação à área territorial."
        ),
        "caption_both": "Atividades principal e secundária combinadas",
        "caption_main": "Apenas atividade principal",
        "caption_secondary": "Apenas atividade secundária",
        "geo_state": "por Estado",
        "geo_municipality": "por Município",
        "top_50_by": "Top 50 por",
        # Metric names (shared between EA and Demographics)
        "establishments": "Estabelecimentos",
        "headquarters": "Matrizes",
        "branches": "Filiais",
        "simples": "Simples",
        "mei": "MEI",
        "ibge_code": "Código IBGE",
        # Demographics
        "tab_period": "Período",
        "year": "Ano",
        "population": "População",
        "density": "Densidade",
        "total_population": "População Total",
        "avg_density": "Densidade Média",
        "density_unit": "hab/km²",
        "population_preferred": "População Preferida",
        "source": "Fonte",
        "area_km2": "Área km²",
        "density_habkm2": "Densidade hab/km²",
    },
    "en": {
        # Sidebar / metadata
        "updated_at": "🕒 Updated at",
        "ref_month": "📅 Federal Revenue ref.",
        # Navigation
        "page_economic_activity": "Economic Activity",
        "page_demographics": "Demographics",
        # Common
        "filters": "🔍 Filters",
        "display": "📊 Display",
        "level": "Level",
        "states": "States",
        "municipalities": "Municipalities",
        "state": "State",
        "select_state_prompt": "Select a state...",
        "select_state_warn": "Select a state to view municipalities.",
        "visualization": "Visualization",
        "map": "Map",
        "chart": "Chart",
        "table": "Table",
        "metric": "Metric",
        "no_data": "No data found.",
        "locality": "Locality",
        "uf": "UF",
        "region": "Region",
        # Economic Activity — CNAE tab
        "tab_cnae": "Economic Activity",
        "tab_options": "Options",
        "tab_location": "Location",
        "cnae_section": "CNAE Section",
        "all_sections": "All sections",
        "division": "Division",
        "group": "Group",
        "class_label": "Class",
        "subclass": "Subclass",
        "activity_type": "Activity type",
        "activity_main_only": "Main Only",
        "activity_both": "Both",
        "activity_secondary_only": "Secondary Only",
        # Economic Activity — Options tab
        "status": "Status",
        "status_active": "Active",
        "status_inactive": "Inactive",
        "status_all": "All",
        "ignore_mei": "Exclude MEI",
        # Economic Activity — Display
        "unit": "Unit",
        "unit_abs": "Absolute Numbers",
        "unit_pct": "Local share",
        "unit_per100k": "Per 100k inhabitants",
        "unit_perkm2": "Per km²",
        "unit_pct_suffix": "% local",
        "unit_per100k_suffix": "per 100k inh.",
        "unit_perkm2_suffix": "per km²",
        "unit_help": (
            "**Absolute Numbers** — raw establishment count.\n\n"
            "**Local share** — share of the selected activity in the total establishments "
            "of the locality across all economic activities.\n\n"
            "**Per 100k inhabitants** — count normalized by estimated population, "
            "enabling comparison across localities of different sizes.\n\n"
            "**Per km²** — density of establishments relative to territorial area."
        ),
        "caption_both": "Primary and secondary activities combined",
        "caption_main": "Primary activity only",
        "caption_secondary": "Secondary activity only",
        "geo_state": "by State",
        "geo_municipality": "by Municipality",
        "top_50_by": "Top 50 by",
        # Metric names
        "establishments": "Establishments",
        "headquarters": "Headquarters",
        "branches": "Branches",
        "simples": "Simples",
        "mei": "MEI",
        "ibge_code": "IBGE Code",
        # Demographics
        "tab_period": "Period",
        "year": "Year",
        "population": "Population",
        "density": "Density",
        "total_population": "Total Population",
        "avg_density": "Average Density",
        "density_unit": "inh/km²",
        "population_preferred": "Preferred Population",
        "source": "Source",
        "area_km2": "Area km²",
        "density_habkm2": "Density inh/km²",
    },
}


def t(key: str, lang: str = "pt") -> str:
    return _T.get(lang, _T["pt"]).get(key, key)
