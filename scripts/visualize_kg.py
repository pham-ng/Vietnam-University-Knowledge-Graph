#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Visualization pipeline for the Vietnamese university knowledge graph.

This module queries a Fuseki SPARQL endpoint and generates:
- a geographic map with Folium
- an interactive knowledge graph with NetworkX + PyVis
- statistical charts with Matplotlib
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable

import folium
import matplotlib.pyplot as plt
import networkx as nx
import pandas as pd
from pyvis.network import Network
from SPARQLWrapper import JSON, SPARQLWrapper


DEFAULT_ENDPOINT = "http://localhost:3030/vio/sparql"
DEFAULT_OUTPUT_DIR = Path("visualizations")

PREFIXES = """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX geo: <http://www.w3.org/2003/01/geo/wgs84_pos#>
PREFIX vio: <http://vi.dbpedia.org/ontology/>
PREFIX vres: <http://vi.dbpedia.org/resource/>
""".strip()

UNIVERSITY_COORDINATES_QUERY = f"""
{PREFIXES}
SELECT DISTINCT ?u ?name ?site ?lat ?long ?cityLabel ?govLabel ?students
WHERE {{
  ?u rdf:type vio:University ;
     vio:hasSite ?site ;
     vio:name ?name .
  ?site geo:lat ?lat ;
        geo:long ?long .
  OPTIONAL {{
    ?site vio:locatedInCity ?city .
    OPTIONAL {{ ?city vio:name ?cityLabel }}
  }}
  OPTIONAL {{
    ?u vio:governedBy ?gov .
    OPTIONAL {{ ?gov vio:name ?govLabel }}
  }}
  OPTIONAL {{ ?u vio:numberOfStudents ?students }}
}}
ORDER BY ?name ?site
""".strip()

GOVERNING_BODY_RELATIONS_QUERY = f"""
{PREFIXES}
SELECT DISTINCT ?u ?uName ?gov ?govName
WHERE {{
  ?u vio:governedBy ?gov .
  OPTIONAL {{ ?u vio:name ?uName }}
  OPTIONAL {{ ?gov vio:name ?govName }}
}}
ORDER BY ?govName ?uName
""".strip()

UNIVERSITY_SYSTEM_RELATIONS_QUERY = f"""
{PREFIXES}
SELECT DISTINCT ?u ?uName ?parent ?parentName
WHERE {{
  ?u vio:isMemberOf ?parent .
  OPTIONAL {{ ?u vio:name ?uName }}
  OPTIONAL {{ ?parent vio:name ?parentName }}
}}
ORDER BY ?parentName ?uName
""".strip()

TOP_PROVINCES_QUERY = f"""
{PREFIXES}
SELECT ?provinceName (COUNT(DISTINCT ?u) AS ?universityCount)
WHERE {{
  ?u rdf:type vio:University ;
     vio:hasSite ?site .
  ?site vio:locatedInProvince ?province .
  OPTIONAL {{ ?province vio:name ?provinceName }}
}}
GROUP BY ?provinceName
ORDER BY DESC(?universityCount)
""".strip()

GOVERNING_BODY_DISTRIBUTION_QUERY = f"""
{PREFIXES}
SELECT ?govName (COUNT(DISTINCT ?u) AS ?universityCount)
WHERE {{
  ?u rdf:type vio:University ;
     vio:governedBy ?gov .
  OPTIONAL {{ ?gov vio:name ?govName }}
}}
GROUP BY ?govName
ORDER BY DESC(?universityCount)
""".strip()

STUDENT_DISTRIBUTION_QUERY = f"""
{PREFIXES}
SELECT ?u ?uName ?students
WHERE {{
  ?u rdf:type vio:University ;
     vio:numberOfStudents ?students .
  OPTIONAL {{ ?u vio:name ?uName }}
}}
ORDER BY DESC(?students)
""".strip()

SPARQL_EXAMPLES: list[tuple[str, str]] = [
    (
        "University coordinates",
        UNIVERSITY_COORDINATES_QUERY,
    ),
    (
        "Governing body relations",
        GOVERNING_BODY_RELATIONS_QUERY,
    ),
    (
        "University system relations",
        UNIVERSITY_SYSTEM_RELATIONS_QUERY,
    ),
    (
        "Universities located in Hanoi",
        f"""
{PREFIXES}
SELECT DISTINCT ?u ?name
WHERE {{
  ?u rdf:type vio:University ;
     vio:hasSite ?s ;
     vio:name ?name .
  ?s vio:locatedInCity vres:Hanoi .
}}
ORDER BY ?name
""".strip(),
    ),
]


def query_sparql(endpoint: str, query: str) -> list[dict[str, str]]:
    client = SPARQLWrapper(endpoint)
    client.setReturnFormat(JSON)
    client.setQuery(query)
    results = client.query().convert()
    bindings = results.get("results", {}).get("bindings", [])

    rows: list[dict[str, str]] = []
    for binding in bindings:
        row = {key: value.get("value", "") for key, value in binding.items()}
        rows.append(row)
    return rows



def dataframe_from_query(endpoint: str, query: str) -> pd.DataFrame:
    return pd.DataFrame(query_sparql(endpoint, query))



def format_int(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        return "Unknown"
    try:
        return f"{int(float(text)):,}"
    except ValueError:
        return text



def safe_label(value: object, fallback: str = "Unknown") -> str:
    text = str(value or "").strip()
    return text if text else fallback



def build_map(university_df: pd.DataFrame, output_path: Path) -> Path:
    vietnam_map = folium.Map(location=[16.2, 106.0], zoom_start=6, tiles="CartoDB positron")

    for record in university_df.to_dict(orient="records"):
        try:
            latitude = float(record.get("lat", ""))
            longitude = float(record.get("long", ""))
        except ValueError:
            continue

        name = safe_label(record.get("name"))
        city = safe_label(record.get("cityLabel"))
        governing_body = safe_label(record.get("govLabel"))
        students = format_int(record.get("students"))

        popup_html = (
            f"<b>{name}</b><br>"
            f"Students: {students}<br>"
            f"City: {city}<br>"
            f"Governing body: {governing_body}"
        )

        folium.CircleMarker(
            location=[latitude, longitude],
            radius=5,
            color="#1f77b4",
            fill=True,
            fill_color="#1f77b4",
            fill_opacity=0.8,
            popup=folium.Popup(popup_html, max_width=320),
            tooltip=name,
        ).add_to(vietnam_map)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    vietnam_map.save(str(output_path))
    return output_path



def add_nodes_from_rows(graph: nx.Graph, rows: Iterable[dict[str, str]], source_key: str, source_label_key: str, source_type: str, target_key: str, target_label_key: str, target_type: str, edge_label: str) -> None:
    color_map = {
        "University": "#4F46E5",
        "GoverningBody": "#DC2626",
        "UniversitySystem": "#059669",
    }
    for row in rows:
        source_uri = row.get(source_key, "")
        target_uri = row.get(target_key, "")
        if not source_uri or not target_uri:
            continue

        source_label = safe_label(row.get(source_label_key), source_uri.rsplit("/", 1)[-1])
        target_label = safe_label(row.get(target_label_key), target_uri.rsplit("/", 1)[-1])

        graph.add_node(source_uri, label=source_label, type=source_type, color=color_map[source_type])
        graph.add_node(target_uri, label=target_label, type=target_type, color=color_map[target_type])
        graph.add_edge(source_uri, target_uri, label=edge_label)



def build_network(governing_df: pd.DataFrame, systems_df: pd.DataFrame, output_path: Path) -> Path:
    nx_graph = nx.Graph()
    add_nodes_from_rows(
        nx_graph,
        governing_df.to_dict(orient="records"),
        source_key="u",
        source_label_key="uName",
        source_type="University",
        target_key="gov",
        target_label_key="govName",
        target_type="GoverningBody",
        edge_label="governedBy",
    )
    add_nodes_from_rows(
        nx_graph,
        systems_df.to_dict(orient="records"),
        source_key="u",
        source_label_key="uName",
        source_type="University",
        target_key="parent",
        target_label_key="parentName",
        target_type="UniversitySystem",
        edge_label="isMemberOf",
    )

    network = Network(height="800px", width="100%", bgcolor="#ffffff", font_color="#111827")
    network.barnes_hut(gravity=-18000, central_gravity=0.15, spring_length=220)

    for node_id, attrs in nx_graph.nodes(data=True):
        network.add_node(
            node_id,
            label=attrs.get("label", node_id),
            title=f"{attrs.get('type', 'Node')}: {attrs.get('label', node_id)}",
            color=attrs.get("color", "#64748B"),
        )

    for source, target, attrs in nx_graph.edges(data=True):
        network.add_edge(source, target, title=attrs.get("label", "relatedTo"), label=attrs.get("label", ""))

    output_path.parent.mkdir(parents=True, exist_ok=True)
    network.save_graph(str(output_path))
    return output_path



def build_top_provinces_chart(df: pd.DataFrame, output_path: Path, top_n: int = 15) -> Path:
    chart_df = df.copy()
    chart_df["provinceName"] = chart_df["provinceName"].fillna("Unknown")
    chart_df["universityCount"] = chart_df["universityCount"].astype(int)
    chart_df = chart_df.head(top_n)

    plt.figure(figsize=(12, 7))
    plt.barh(chart_df["provinceName"], chart_df["universityCount"], color="#2563EB")
    plt.gca().invert_yaxis()
    plt.xlabel("Number of universities")
    plt.ylabel("Province")
    plt.title("Top provinces by number of universities")
    plt.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=200)
    plt.close()
    return output_path



def build_governing_body_chart(df: pd.DataFrame, output_path: Path, top_n: int = 15) -> Path:
    chart_df = df.copy()
    chart_df["govName"] = chart_df["govName"].fillna("Unknown")
    chart_df["universityCount"] = chart_df["universityCount"].astype(int)
    chart_df = chart_df.head(top_n)

    plt.figure(figsize=(12, 7))
    plt.barh(chart_df["govName"], chart_df["universityCount"], color="#DC2626")
    plt.gca().invert_yaxis()
    plt.xlabel("Number of universities")
    plt.ylabel("Governing body")
    plt.title("Universities per governing body")
    plt.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=200)
    plt.close()
    return output_path



def build_student_distribution_chart(df: pd.DataFrame, output_path: Path) -> Path:
    chart_df = df.copy()
    chart_df["students"] = pd.to_numeric(chart_df["students"], errors="coerce")
    chart_df = chart_df.dropna(subset=["students"])

    plt.figure(figsize=(12, 7))
    plt.hist(chart_df["students"], bins=20, color="#059669", edgecolor="white")
    plt.xlabel("Number of students")
    plt.ylabel("Number of universities")
    plt.title("Distribution of student population")
    plt.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=200)
    plt.close()
    return output_path



def generate_visualizations(endpoint: str = DEFAULT_ENDPOINT, output_dir: Path = DEFAULT_OUTPUT_DIR) -> dict[str, object]:
    output_dir.mkdir(parents=True, exist_ok=True)

    university_df = dataframe_from_query(endpoint, UNIVERSITY_COORDINATES_QUERY)
    governing_df = dataframe_from_query(endpoint, GOVERNING_BODY_RELATIONS_QUERY)
    systems_df = dataframe_from_query(endpoint, UNIVERSITY_SYSTEM_RELATIONS_QUERY)
    provinces_df = dataframe_from_query(endpoint, TOP_PROVINCES_QUERY)
    governing_distribution_df = dataframe_from_query(endpoint, GOVERNING_BODY_DISTRIBUTION_QUERY)
    students_df = dataframe_from_query(endpoint, STUDENT_DISTRIBUTION_QUERY)

    artifacts = {
        "map": build_map(university_df, output_dir / "map_universities.html"),
        "network": build_network(governing_df, systems_df, output_dir / "university_network.html"),
        "province_chart": build_top_provinces_chart(provinces_df, output_dir / "universities_by_province.png"),
        "governing_chart": build_governing_body_chart(governing_distribution_df, output_dir / "governing_body_distribution.png"),
        "students_chart": build_student_distribution_chart(students_df, output_dir / "student_population_distribution.png"),
        "dataframes": {
            "universities": university_df,
            "governing": governing_df,
            "systems": systems_df,
            "provinces": provinces_df,
            "governing_distribution": governing_distribution_df,
            "students": students_df,
        },
    }
    return artifacts



def main() -> None:
    parser = argparse.ArgumentParser(description="Build map, network and chart visualizations from the university SPARQL endpoint.")
    parser.add_argument("--endpoint", default=DEFAULT_ENDPOINT, help="SPARQL endpoint URL")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="Directory for generated HTML and PNG artifacts")
    args = parser.parse_args()

    artifacts = generate_visualizations(args.endpoint, Path(args.output_dir))
    print("Generated visualization artifacts:")
    print(f"- map: {artifacts['map']}")
    print(f"- network: {artifacts['network']}")
    print(f"- province_chart: {artifacts['province_chart']}")
    print(f"- governing_chart: {artifacts['governing_chart']}")
    print(f"- students_chart: {artifacts['students_chart']}")


if __name__ == "__main__":
    main()
