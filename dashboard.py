#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components
from PIL import Image

from scripts.visualize_kg import (
    DEFAULT_ENDPOINT,
    PREFIXES,
    SPARQL_EXAMPLES,
    format_int,
    generate_visualizations,
    query_sparql,
    safe_label,
)


OUTPUT_DIR = Path("visualizations")
ENTITY_TYPES = ["All", "University", "GoverningBody", "UniversitySystem"]

APP_CSS = """
<style>
    .stApp {
        background:
            radial-gradient(circle at top left, rgba(56, 189, 248, 0.18), transparent 28%),
            radial-gradient(circle at top right, rgba(99, 102, 241, 0.18), transparent 24%),
            linear-gradient(180deg, #f7faff 0%, #eef4ff 100%);
    }

    .block-container {
        padding-top: 1.5rem;
        padding-bottom: 2rem;
    }

    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #0f172a 0%, #172554 100%);
        border-right: 1px solid rgba(148, 163, 184, 0.18);
    }

    [data-testid="stSidebar"] * {
        color: #e2e8f0;
    }

    .hero-shell {
        padding: 1.5rem 1.75rem;
        border-radius: 24px;
        background: linear-gradient(135deg, rgba(15, 23, 42, 0.96), rgba(30, 41, 59, 0.92));
        box-shadow: 0 18px 50px rgba(15, 23, 42, 0.18);
        margin-bottom: 1.1rem;
        border: 1px solid rgba(148, 163, 184, 0.16);
    }

    .hero-kicker {
        display: inline-block;
        font-size: 0.78rem;
        font-weight: 700;
        letter-spacing: 0.08em;
        text-transform: uppercase;
        color: #7dd3fc;
        margin-bottom: 0.6rem;
    }

    .hero-title {
        font-size: 2.2rem;
        font-weight: 800;
        line-height: 1.08;
        color: #f8fafc;
        margin: 0;
    }

    .hero-subtitle {
        margin-top: 0.85rem;
        color: #cbd5e1;
        font-size: 1rem;
        max-width: 880px;
    }

    .tag-row {
        display: flex;
        flex-wrap: wrap;
        gap: 0.55rem;
        margin-top: 1rem;
    }

    .tag-chip {
        padding: 0.32rem 0.7rem;
        border-radius: 999px;
        background: rgba(59, 130, 246, 0.18);
        color: #dbeafe;
        font-size: 0.84rem;
        border: 1px solid rgba(125, 211, 252, 0.18);
    }

    .section-card {
        background: rgba(255, 255, 255, 0.86);
        border: 1px solid rgba(148, 163, 184, 0.18);
        backdrop-filter: blur(8px);
        border-radius: 22px;
        padding: 1rem 1.05rem;
        box-shadow: 0 12px 28px rgba(15, 23, 42, 0.08);
        margin-bottom: 1rem;
    }

    .section-title {
        font-size: 1rem;
        font-weight: 750;
        color: #0f172a;
        margin-bottom: 0.18rem;
    }

    .section-copy {
        color: #475569;
        font-size: 0.92rem;
        margin-bottom: 0.8rem;
    }

    .entity-banner {
        padding: 1rem 1.1rem;
        border-radius: 20px;
        background: linear-gradient(135deg, rgba(59, 130, 246, 0.10), rgba(139, 92, 246, 0.10));
        border: 1px solid rgba(99, 102, 241, 0.12);
        margin-bottom: 0.9rem;
    }

    .entity-label {
        font-size: 1.55rem;
        font-weight: 800;
        color: #0f172a;
        margin-bottom: 0.25rem;
    }

    .entity-meta {
        color: #475569;
        font-size: 0.93rem;
    }

    .stTabs [data-baseweb="tab-list"] {
        gap: 0.55rem;
        padding: 0.2rem;
        background: rgba(255, 255, 255, 0.65);
        border-radius: 16px;
        border: 1px solid rgba(148, 163, 184, 0.18);
    }

    .stTabs [data-baseweb="tab"] {
        height: 44px;
        border-radius: 12px;
        padding: 0 1rem;
        font-weight: 650;
    }

    .stTabs [aria-selected="true"] {
        background: linear-gradient(135deg, #2563eb, #4f46e5);
        color: white;
    }

    [data-testid="stMetric"] {
        background: rgba(255, 255, 255, 0.88);
        border: 1px solid rgba(148, 163, 184, 0.18);
        padding: 0.9rem 1rem;
        border-radius: 18px;
        box-shadow: 0 10px 24px rgba(15, 23, 42, 0.06);
    }

    .stButton > button {
        border-radius: 12px;
        border: 1px solid rgba(99, 102, 241, 0.18);
    }

    .small-muted {
        color: #64748b;
        font-size: 0.88rem;
    }
</style>
""".strip()

ENTITY_DETAILS_QUERY_TEMPLATE = """
{prefixes}
SELECT DISTINCT ?type ?label ?wikidataID ?sameAs ?foundingYear ?students ?staff ?governingBody ?governingBodyLabel ?parent ?parentLabel ?site ?siteLabel ?address ?city ?cityLabel ?province ?provinceLabel ?lat ?long
WHERE {{
  BIND(<{entity_uri}> AS ?entity)
  OPTIONAL {{ ?entity a ?type . }}
  OPTIONAL {{ ?entity vio:name ?label . }}
  OPTIONAL {{ ?entity vio:hasWikidataID ?wikidataID . }}
  OPTIONAL {{ ?entity owl:sameAs ?sameAs . }}
  OPTIONAL {{ ?entity vio:foundingYearOrg ?foundingYear . }}
  OPTIONAL {{ ?entity vio:numberOfStudents ?students . }}
  OPTIONAL {{ ?entity vio:academicStaffSize ?staff . }}
  OPTIONAL {{
    ?entity vio:governedBy ?governingBody .
    OPTIONAL {{ ?governingBody vio:name ?governingBodyLabel . }}
  }}
  OPTIONAL {{
    ?entity vio:isMemberOf ?parent .
    OPTIONAL {{ ?parent vio:name ?parentLabel . }}
  }}
  OPTIONAL {{
    ?entity vio:hasSite ?site .
    OPTIONAL {{ ?site vio:name ?siteLabel . }}
    OPTIONAL {{ ?site vio:address ?address . }}
    OPTIONAL {{
      ?site vio:locatedInCity ?city .
      OPTIONAL {{ ?city vio:name ?cityLabel . }}
    }}
    OPTIONAL {{
      ?site vio:locatedInProvince ?province .
      OPTIONAL {{ ?province vio:name ?provinceLabel . }}
    }}
    OPTIONAL {{ ?site <http://www.w3.org/2003/01/geo/wgs84_pos#lat> ?lat . }}
    OPTIONAL {{ ?site <http://www.w3.org/2003/01/geo/wgs84_pos#long> ?long . }}
  }}
}}
""".strip()

ENTITY_NEIGHBORS_QUERY_TEMPLATE = """
{prefixes}
SELECT DISTINCT ?direction ?predicate ?neighbor ?neighborLabel
WHERE {{
  {{
    BIND("outgoing" AS ?direction)
    BIND(<{entity_uri}> AS ?entity)
    ?entity ?predicate ?neighbor .
    FILTER(isIRI(?neighbor))
    FILTER(STRSTARTS(STR(?predicate), "http://vi.dbpedia.org/ontology/") || ?predicate = owl:sameAs)
    OPTIONAL {{ ?neighbor vio:name ?neighborLabel }}
  }}
  UNION
  {{
    BIND("incoming" AS ?direction)
    BIND(<{entity_uri}> AS ?entity)
    ?neighbor ?predicate ?entity .
    FILTER(isIRI(?neighbor))
    FILTER(STRSTARTS(STR(?predicate), "http://vi.dbpedia.org/ontology/") || ?predicate = owl:sameAs)
    OPTIONAL {{ ?neighbor vio:name ?neighborLabel }}
  }}
}}
ORDER BY ?direction ?predicate ?neighborLabel
""".strip()


@st.cache_data(show_spinner=False)
def load_visual_assets(endpoint: str) -> dict[str, object]:
    return generate_visualizations(endpoint=endpoint, output_dir=OUTPUT_DIR)


@st.cache_data(show_spinner=False)
def load_entity_details(endpoint: str, entity_uri: str) -> dict[str, Any]:
    details_rows = query_sparql(
        endpoint,
        ENTITY_DETAILS_QUERY_TEMPLATE.format(prefixes=PREFIXES, entity_uri=entity_uri),
    )
    neighbor_rows = query_sparql(
        endpoint,
        ENTITY_NEIGHBORS_QUERY_TEMPLATE.format(prefixes=PREFIXES, entity_uri=entity_uri),
    )

    details: dict[str, Any] = {
        "types": [],
        "labels": [],
        "wikidata_ids": [],
        "same_as": [],
        "founding_years": [],
        "students": [],
        "staff": [],
        "governing_bodies": [],
        "parents": [],
        "sites": [],
        "neighbors": neighbor_rows,
    }

    for row in details_rows:
        if row.get("type") and row["type"] not in details["types"]:
            details["types"].append(row["type"])
        if row.get("label") and row["label"] not in details["labels"]:
            details["labels"].append(row["label"])
        if row.get("wikidataID") and row["wikidataID"] not in details["wikidata_ids"]:
            details["wikidata_ids"].append(row["wikidataID"])
        if row.get("sameAs") and row["sameAs"] not in details["same_as"]:
            details["same_as"].append(row["sameAs"])
        if row.get("foundingYear") and row["foundingYear"] not in details["founding_years"]:
            details["founding_years"].append(row["foundingYear"])
        if row.get("students") and row["students"] not in details["students"]:
            details["students"].append(row["students"])
        if row.get("staff") and row["staff"] not in details["staff"]:
            details["staff"].append(row["staff"])

        if row.get("governingBody"):
            governing_body = {
                "uri": row["governingBody"],
                "label": safe_label(row.get("governingBodyLabel"), row["governingBody"].rsplit("/", 1)[-1]),
            }
            if governing_body not in details["governing_bodies"]:
                details["governing_bodies"].append(governing_body)

        if row.get("parent"):
            parent = {
                "uri": row["parent"],
                "label": safe_label(row.get("parentLabel"), row["parent"].rsplit("/", 1)[-1]),
            }
            if parent not in details["parents"]:
                details["parents"].append(parent)

        if row.get("site"):
            site = {
                "uri": row["site"],
                "label": safe_label(row.get("siteLabel"), row["site"].rsplit("/", 1)[-1]),
                "address": safe_label(row.get("address"), "Unknown"),
                "city": safe_label(row.get("cityLabel"), "Unknown"),
                "province": safe_label(row.get("provinceLabel"), "Unknown"),
                "lat": row.get("lat", ""),
                "long": row.get("long", ""),
            }
            if site not in details["sites"]:
                details["sites"].append(site)

    return details


def render_html_file(path: Path, height: int) -> None:
    html = path.read_text(encoding="utf-8")
    components.html(html, height=height, scrolling=True)


def render_image(path: Path, caption: str) -> None:
    image = Image.open(path)
    st.image(image, caption=caption, width="stretch")


def inject_styles() -> None:
    st.markdown(APP_CSS, unsafe_allow_html=True)


def render_hero(endpoint: str, entity_count: int) -> None:
    st.markdown(
        f"""
        <div class="hero-shell">
            <div class="hero-kicker">Vietnamese University Knowledge Graph</div>
            <div class="hero-title">A cleaner, richer DBpedia-style explorer for your university KG</div>
            <div class="hero-subtitle">
                Search entities in the sidebar, inspect linked facts, browse sites and governing bodies,
                then jump into the map, network, analytics, and raw SPARQL views from one polished interface.
            </div>
            <div class="tag-row">
                <span class="tag-chip">Endpoint: {endpoint}</span>
                <span class="tag-chip">Indexed entities: {entity_count}</span>
                <span class="tag-chip">Fuseki + Streamlit + SPARQL</span>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def start_card(title: str, copy: str | None = None) -> None:
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.markdown(f'<div class="section-title">{title}</div>', unsafe_allow_html=True)
    if copy:
        st.markdown(f'<div class="section-copy">{copy}</div>', unsafe_allow_html=True)


def end_card() -> None:
    st.markdown("</div>", unsafe_allow_html=True)


def uri_to_label(uri: str) -> str:
    return uri.rsplit("/", 1)[-1].replace("_", " ") if uri else "Unknown"


def build_entity_index(dataframes: dict[str, pd.DataFrame]) -> pd.DataFrame:
    university_df = dataframes["universities"][["u", "name"]].drop_duplicates().rename(columns={"u": "uri", "name": "label"})
    university_df["type"] = "University"

    governing_df = dataframes["governing"][["gov", "govName"]].drop_duplicates().rename(columns={"gov": "uri", "govName": "label"})
    governing_df["type"] = "GoverningBody"

    systems_df = dataframes["systems"][["parent", "parentName"]].drop_duplicates().rename(columns={"parent": "uri", "parentName": "label"})
    systems_df["type"] = "UniversitySystem"

    entity_df = pd.concat([university_df, governing_df, systems_df], ignore_index=True)
    entity_df = entity_df.dropna(subset=["uri"])
    entity_df["label"] = entity_df.apply(
        lambda row: row["label"] if str(row.get("label", "")).strip() else uri_to_label(str(row["uri"])),
        axis=1,
    )
    entity_df = entity_df.drop_duplicates(subset=["uri", "type"]).sort_values(["type", "label"])
    return entity_df.reset_index(drop=True)


def set_selected_entity(entity_uri: str) -> None:
    st.session_state["selected_entity_uri"] = entity_uri


def render_clickable_entity_list(items: list[dict[str, Any]], section_title: str, key_prefix: str) -> None:
    if not items:
        st.caption(f"No {section_title.lower()} available.")
        return

    st.markdown(f"**{section_title}**")
    for index, item in enumerate(items):
        label = safe_label(item.get("label"), uri_to_label(str(item.get("uri", ""))))
        subtitle_parts = []
        if item.get("address") and item["address"] != "Unknown":
            subtitle_parts.append(str(item["address"]))
        if item.get("city") and item["city"] != "Unknown":
            subtitle_parts.append(str(item["city"]))
        if item.get("province") and item["province"] != "Unknown":
            subtitle_parts.append(str(item["province"]))
        subtitle = " • ".join(subtitle_parts)

        cols = st.columns([4, 1])
        cols[0].markdown(f"**{label}**" + (f"  \n<span class='small-muted'>{subtitle}</span>" if subtitle else ""), unsafe_allow_html=True)
        if item.get("uri") and cols[1].button("Open", key=f"{key_prefix}_{index}_{item['uri']}"):
            set_selected_entity(str(item["uri"]))
            st.rerun()


def render_neighbor_table(neighbors: list[dict[str, str]]) -> None:
    if not neighbors:
        st.caption("No related entities available.")
        return

    st.markdown("**Clickable entity explorer**")
    for index, row in enumerate(neighbors):
        neighbor_uri = row.get("neighbor", "")
        label = safe_label(row.get("neighborLabel"), uri_to_label(neighbor_uri))
        predicate = uri_to_label(row.get("predicate", ""))
        direction = row.get("direction", "outgoing")

        cols = st.columns([1.2, 3.2, 1])
        cols[0].markdown(f"`{direction}`")
        cols[1].markdown(f"**{predicate}** → {label}")
        if neighbor_uri and cols[2].button("Explore", key=f"neighbor_{index}_{neighbor_uri}"):
            set_selected_entity(neighbor_uri)
            st.rerun()


def render_entity_detail_panel(endpoint: str, selected_entity_uri: str) -> None:
    details = load_entity_details(endpoint, selected_entity_uri)
    label = details["labels"][0] if details["labels"] else uri_to_label(selected_entity_uri)

    st.markdown(
        f"""
        <div class="entity-banner">
            <div class="entity-label">{label}</div>
            <div class="entity-meta">{selected_entity_uri}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    metric_col1, metric_col2, metric_col3 = st.columns(3)
    metric_col1.metric("Types", len(details["types"]))
    metric_col2.metric("Sites", len(details["sites"]))
    metric_col3.metric("Linked entities", len(details["neighbors"]))

    fact_rows: list[tuple[str, str]] = []
    if details["founding_years"]:
        fact_rows.append(("Founding year", ", ".join(details["founding_years"])))
    if details["students"]:
        fact_rows.append(("Students", ", ".join(format_int(value) for value in details["students"])))
    if details["staff"]:
        fact_rows.append(("Academic staff", ", ".join(format_int(value) for value in details["staff"])))
    if details["wikidata_ids"]:
        fact_rows.append(("Wikidata ID", ", ".join(details["wikidata_ids"])))
    if details["same_as"]:
        fact_rows.append(("sameAs", "\n".join(details["same_as"])))
    if details["types"]:
        fact_rows.append(("RDF types", "\n".join(uri_to_label(uri) for uri in details["types"])))

    if fact_rows:
        start_card("Facts", "Core properties resolved live from the SPARQL endpoint.")
        st.dataframe(pd.DataFrame(fact_rows, columns=["Property", "Value"]), width="stretch", hide_index=True)
        end_card()

    start_card("Linked entities", "Use these links to move around the knowledge graph like an entity explorer.")
    render_clickable_entity_list(details["governing_bodies"], "Governing bodies", "gov")
    render_clickable_entity_list(details["parents"], "Parent systems / universities", "parent")
    render_clickable_entity_list(details["sites"], "Sites", "site")
    render_neighbor_table(details["neighbors"])
    end_card()


def main() -> None:
    st.set_page_config(page_title="Vietnam University KG Explorer", layout="wide")
    inject_styles()

    endpoint = st.sidebar.text_input("SPARQL endpoint", value=DEFAULT_ENDPOINT)
    refresh = st.sidebar.button("Refresh data")
    if refresh:
        load_visual_assets.clear()
        load_entity_details.clear()

    with st.spinner("Querying SPARQL endpoint and building visualizations..."):
        artifacts = load_visual_assets(endpoint)

    dataframes: dict[str, pd.DataFrame] = artifacts["dataframes"]  # type: ignore[assignment]
    entity_index = build_entity_index(dataframes)
    render_hero(endpoint, len(entity_index))

    if "selected_entity_uri" not in st.session_state and not entity_index.empty:
        st.session_state["selected_entity_uri"] = str(entity_index.iloc[0]["uri"])

    st.sidebar.markdown("## Entity Search")
    st.sidebar.caption("Filter, search and jump across resources quickly.")
    st.sidebar.subheader("Entity search")
    search_term = st.sidebar.text_input("Search by label", value="")
    selected_type = st.sidebar.selectbox("Entity type", options=ENTITY_TYPES, index=0)

    filtered_entities = entity_index.copy()
    if selected_type != "All":
        filtered_entities = filtered_entities[filtered_entities["type"] == selected_type]
    if search_term.strip():
        filtered_entities = filtered_entities[filtered_entities["label"].str.contains(search_term.strip(), case=False, na=False)]

    options = [f"{row.label} [{row.type}]" for row in filtered_entities.itertuples(index=False)]
    uri_lookup = {f"{row.label} [{row.type}]": row.uri for row in filtered_entities.itertuples(index=False)}
    current_uri = st.session_state.get("selected_entity_uri")
    default_option = next((option for option, uri in uri_lookup.items() if uri == current_uri), options[0] if options else None)

    if options:
        selected_option = st.sidebar.selectbox(
            "Matching entities",
            options=options,
            index=options.index(default_option) if default_option in options else 0,
        )
        st.session_state["selected_entity_uri"] = uri_lookup[selected_option]
    else:
        st.sidebar.info("No entities matched your search.")

    st.sidebar.markdown("**Quick browse**")
    for row in filtered_entities.head(8).itertuples(index=False):
        if st.sidebar.button(f"Open {row.label}", key=f"sidebar_{row.uri}"):
            set_selected_entity(str(row.uri))
            st.rerun()

    st.sidebar.markdown("---")
    st.sidebar.caption(f"{len(filtered_entities)} entities match the current filters.")

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Universities with coordinates", len(dataframes["universities"]["u"].unique()) if not dataframes["universities"].empty else 0)
    col2.metric("Governing relations", len(dataframes["governing"]))
    col3.metric("Membership relations", len(dataframes["systems"]))
    col4.metric("Province records", len(dataframes["provinces"]))

    explorer_tab, map_tab, network_tab, analytics_tab, sparql_tab = st.tabs(
        ["Entity Explorer", "Map", "Network", "Analytics", "SPARQL"]
    )

    with explorer_tab:
        left_col, right_col = st.columns([1.1, 2.2])
        with left_col:
            start_card("Search results", f"{len(filtered_entities)} entities match your current filter and keyword.")
            st.dataframe(filtered_entities[["label", "type"]], width="stretch", hide_index=True, height=420)
            end_card()
        with right_col:
            selected_entity_uri = st.session_state.get("selected_entity_uri")
            if selected_entity_uri:
                start_card("Entity explorer", "Inspect live properties, follow relations, and browse the graph one node at a time.")
                render_entity_detail_panel(endpoint, selected_entity_uri)
                end_card()
            else:
                st.info("Select an entity from the sidebar to start exploring.")

    with map_tab:
        start_card("Interactive map", "Universities with coordinates rendered over Vietnam for quick geographic exploration.")
        render_html_file(Path(artifacts["map"]), height=680)
        end_card()

    with network_tab:
        start_card("Knowledge graph network", "A high-level relation view of universities, governing bodies, and university systems.")
        render_html_file(Path(artifacts["network"]), height=780)
        end_card()

    with analytics_tab:
        start_card("Statistical charts", "A compact analytics layer for distribution and concentration patterns in the graph.")
        chart_col1, chart_col2 = st.columns(2)
        with chart_col1:
            render_image(Path(artifacts["province_chart"]), "Top provinces by number of universities")
            render_image(Path(artifacts["students_chart"]), "Distribution of student population")
        with chart_col2:
            render_image(Path(artifacts["governing_chart"]), "Universities per governing body")
        end_card()

        start_card("Preview data", "Underlying result tables behind the visualizations.")
        preview_tab1, preview_tab2, preview_tab3 = st.tabs(["Map data", "Governing relations", "System relations"])
        with preview_tab1:
            st.dataframe(dataframes["universities"], width="stretch")
        with preview_tab2:
            st.dataframe(dataframes["governing"], width="stretch")
        with preview_tab3:
            st.dataframe(dataframes["systems"], width="stretch")
        end_card()

    with sparql_tab:
        start_card("SPARQL query examples", "Ready-to-run query snippets for demoing the dataset and debugging the explorer.")
        for title, query in SPARQL_EXAMPLES:
            with st.expander(title):
                st.code(query, language="sparql")

        selected_entity_uri = st.session_state.get("selected_entity_uri")
        if selected_entity_uri:
            with st.expander("Current entity detail query"):
                st.code(
                    ENTITY_DETAILS_QUERY_TEMPLATE.format(prefixes=PREFIXES, entity_uri=selected_entity_uri),
                    language="sparql",
                )
        end_card()

    start_card("How to run", "Use the command below if you want to restart the explorer manually.")
    st.code("streamlit run dashboard.py", language="bash")
    end_card()


if __name__ == "__main__":
    main()
