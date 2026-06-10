import pandas as pd
import streamlit as st

from i18n import t
from queries import get_establishment_metadata
from tabs import demographics, economic_activity


st.set_page_config(
    page_title="Brazil BI Platform",
    page_icon="🇧🇷",
    layout="wide",
)

if "page" not in st.session_state:
    st.session_state.page = "economic_activity"


def _detect_lang() -> str:
    try:
        accept = st.context.headers.get("Accept-Language", "")
        return "pt" if accept[:2].lower() == "pt" else "en"
    except Exception:
        return "en"


_LANG_OPTIONS = ["🇧🇷 Português", "🇺🇸 English"]

if "lang_select" not in st.session_state:
    detected = _detect_lang()
    initial = "pt" if detected == "pt" else "en"
    st.session_state["lang_select"] = _LANG_OPTIONS[0] if initial == "pt" else _LANG_OPTIONS[1]
    st.session_state["_active_lang"] = initial

# Derive lang before render so the label itself is already translated
_cur_lang = "pt" if st.session_state.get("lang_select", "").startswith("🇧🇷") else "en"
_lang_label = "Linguagem" if _cur_lang == "pt" else "Language"

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 🇧🇷 Brazil BI Platform")

    lang_choice = st.selectbox(
        _lang_label,
        _LANG_OPTIONS,
        key="lang_select",
    )
    lang = "pt" if lang_choice.startswith("🇧🇷") else "en"

    st.divider()

# Reset widget state when language changes to avoid stale session state
_preserved = {"lang_select", "page", "_active_lang"}
if st.session_state.get("_active_lang") != lang:
    for _k in [k for k in st.session_state if k not in _preserved]:
        del st.session_state[_k]
    st.session_state["_active_lang"] = lang
    st.rerun()

with st.sidebar:

    for page_key, icon in [
        ("economic_activity", "📈"),
        ("demographics", "👥"),
    ]:
        label = t(f"page_{page_key}", lang)
        is_active = st.session_state.page == page_key
        if st.button(
            f"{icon} {label}",
            use_container_width=True,
            type="primary" if is_active else "secondary",
        ):
            st.session_state.page = page_key
            st.rerun()

    try:
        meta = get_establishment_metadata()
        updated_at = pd.to_datetime(meta["_updated_at"].iloc[0])
        ref = str(meta["_reference_month"].iloc[0])
        ref_fmt = pd.to_datetime(ref).strftime("%m/%Y")
        st.caption(f"{t('updated_at', lang)}: {updated_at.strftime('%d/%m/%Y')}")
        st.caption(f"{t('ref_month', lang)}: {ref_fmt}")
    except Exception:
        pass

# ── Page routing ──────────────────────────────────────────────────────────────
if st.session_state.page == "economic_activity":
    economic_activity.render(lang)
elif st.session_state.page == "demographics":
    demographics.render(lang)
