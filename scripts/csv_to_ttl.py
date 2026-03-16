#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Chuyển đổi CSV dữ liệu đại học Việt Nam sang RDF/Turtle.

Script này ánh xạ dữ liệu CSV sang ontology `vio`, đồng thời hỗ trợ:
- tạo thực thể trường đại học, người đứng đầu, thành phố và tỉnh/thành,
- nối quan hệ địa lý và quan hệ thành viên giữa các trường,
- liên kết `owl:sameAs` sang Wikidata,
- serialize sang `.ttl` và upload lên Apache Fuseki nếu cần.
"""

import argparse
import csv
import re
import unicodedata
from pathlib import Path
from typing import Optional, Tuple

import requests
from rdflib import Graph, Literal, Namespace, URIRef
from rdflib.namespace import OWL, RDF, RDFS, XSD


# --- Namespace constants ----------------------------------------------------
VIO_NS = "http://vi.dbpedia.org/ontology/"
VRES_NS = "http://vi.dbpedia.org/resource/"
DBO_NS = "http://dbpedia.org/ontology/"
GEO_NS = "http://www.w3.org/2003/01/geo/wgs84_pos#"
WD_NS = "http://www.wikidata.org/entity/"


PROVINCE_NAMES = {
    "An Giang", "Bà Rịa - Vũng Tàu", "Bạc Liêu", "Bắc Giang", "Bắc Kạn", "Bắc Ninh", "Bến Tre",
    "Bình Dương", "Bình Định", "Bình Phước", "Bình Thuận", "Cà Mau", "Cao Bằng", "Cần Thơ",
    "Đà Nẵng", "Đắk Lắk", "Đắk Nông", "Điện Biên", "Đồng Nai", "Đồng Tháp", "Gia Lai", "Hà Giang",
    "Hà Nam", "Hà Nội", "Hà Tĩnh", "Hải Dương", "Hải Phòng", "Hậu Giang", "Hòa Bình", "Hồ Chí Minh",
    "Hưng Yên", "Khánh Hòa", "Kiên Giang", "Kon Tum", "Lai Châu", "Lâm Đồng", "Lạng Sơn", "Lào Cai",
    "Long An", "Nam Định", "Nghệ An", "Ninh Bình", "Ninh Thuận", "Phú Thọ", "Phú Yên", "Quảng Bình",
    "Quảng Nam", "Quảng Ngãi", "Quảng Ninh", "Quảng Trị", "Sóc Trăng", "Sơn La", "Tây Ninh", "Thái Bình",
    "Thái Nguyên", "Thanh Hóa", "Thừa Thiên Huế", "Tiền Giang", "Trà Vinh", "Tuyên Quang", "Vĩnh Long",
    "Vĩnh Phúc", "Yên Bái",
}
CITY_NAMES = {
    "Hà Nội", "Hồ Chí Minh", "Hải Phòng", "Đà Nẵng", "Cần Thơ", "Huế", "Vinh", "Nha Trang", "Đà Lạt",
    "Quy Nhơn", "Biên Hòa", "Thái Nguyên", "Việt Trì", "Buôn Ma Thuột", "Pleiku", "Tam Kỳ", "Mỹ Tho",
    "Long Xuyên", "Rạch Giá", "Trà Vinh", "Tuy Hòa", "Nam Định", "Thanh Hóa", "Hạ Long", "Đồng Hới",
    "Phan Thiết", "Cà Mau",
}


# --- Utility helpers --------------------------------------------------------
def slugify(value: str) -> str:
    """Chuẩn hóa chuỗi tiếng Việt thành slug ASCII dùng `_`.

    Ví dụ:
    - `Đại học Bách khoa Hà Nội` -> `dai_hoc_bach_khoa_ha_noi`
    - `TP. Hồ Chí Minh` -> `tp_ho_chi_minh`
    """
    text = (value or "").strip()
    if not text:
        return ""

    text = text.replace("Đ", "D").replace("đ", "d")
    text = unicodedata.normalize("NFD", text)
    text = "".join(char for char in text if unicodedata.category(char) != "Mn")
    text = text.lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text


def make_resource_uri(namespace: Namespace, resource_type: str, value: str) -> URIRef:
    """Tạo URI tài nguyên dạng `namespace + Type/slug`.

    Ví dụ: `http://vi.dbpedia.org/resource/University/dai_hoc_x`
    """
    return namespace[f"{resource_type}/{slugify(value)}"]


def add_vi_literal(graph: Graph, subject: URIRef, predicate: URIRef, value: str) -> None:
    """Thêm literal tiếng Việt nếu giá trị có dữ liệu."""
    cleaned = (value or "").strip()
    if cleaned:
        graph.add((subject, predicate, Literal(cleaned, lang="vi")))


def add_typed_literal(graph: Graph, subject: URIRef, predicate: URIRef, value: str, datatype: URIRef) -> None:
    """Thêm literal có kiểu dữ liệu RDF nếu chuỗi không rỗng."""
    cleaned = (value or "").strip()
    if cleaned:
        graph.add((subject, predicate, Literal(cleaned, datatype=datatype)))


def add_integer_literal(graph: Graph, subject: URIRef, predicate: URIRef, value: str) -> None:
    """Chuẩn hóa và thêm số nguyên kiểu `xsd:integer`."""
    cleaned = re.sub(r"\D", "", value or "")
    if cleaned:
        graph.add((subject, predicate, Literal(int(cleaned), datatype=XSD.integer)))


def add_float_literal(graph: Graph, subject: URIRef, predicate: URIRef, value: str) -> None:
    """Chuẩn hóa và thêm số thực kiểu `xsd:float`."""
    cleaned = (value or "").strip()
    if not cleaned:
        return
    try:
        number = float(cleaned)
    except (TypeError, ValueError):
        return
    graph.add((subject, predicate, Literal(number, datatype=XSD.float)))


def pad_list(values: list[str], size: int) -> list[str]:
    """Kéo dài danh sách tới `size` phần tử bằng chuỗi rỗng."""
    if len(values) >= size:
        return values
    return values + [""] * (size - len(values))


def split_pipe_values(value: str) -> list[str]:
    """Tách chuỗi đa giá trị theo dấu `|`, bỏ phần tử rỗng và trim khoảng trắng."""
    return [part.strip() for part in (value or "").split("|") if part.strip()]


def ensure_label_and_name(graph: Graph, resource: URIRef, vio: Namespace, value: str) -> None:
    """Bổ sung `rdfs:label` và `vio:name` cho thực thể có tên tiếng Việt."""
    cleaned = (value or "").strip()
    if not cleaned:
        return
    graph.add((resource, RDFS.label, Literal(cleaned, lang="vi")))
    graph.add((resource, vio.name, Literal(cleaned, lang="vi")))


def normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip())


def infer_site_location_from_address(address: str, fallback_city: str, fallback_province: str, site_count: int) -> Tuple[str, str]:
    cleaned = normalize_text(address)
    lowered = cleaned.lower()

    detected_city = ""
    detected_province = ""
    for city_name in sorted(CITY_NAMES, key=len, reverse=True):
        if city_name.lower() in lowered:
            detected_city = city_name
            break

    for province_name in sorted(PROVINCE_NAMES, key=len, reverse=True):
        if province_name.lower() in lowered:
            detected_province = province_name
            break

    if site_count == 1:
        detected_city = detected_city or normalize_text(fallback_city)
        detected_province = detected_province or normalize_text(fallback_province)

    if detected_city and not detected_province and normalize_text(fallback_province):
        detected_province = normalize_text(fallback_province)
    return detected_city, detected_province


# --- Entity builders --------------------------------------------------------
def add_university_entity(graph: Graph, row: dict, vio: Namespace, vres: Namespace, geo: Namespace, wd: Namespace) -> Optional[URIRef]:
    """Tạo thực thể `vio:University` và các thuộc tính nền tảng của trường."""
    page = (row.get("page") or "").strip()
    name = (row.get("name") or "").strip()
    if not page:
        return None

    university_uri = make_resource_uri(vres, "University", page)
    graph.add((university_uri, RDF.type, vio.University))
    ensure_label_and_name(graph, university_uri, vio, name or page)

    wikidata_id = (row.get("wikidata_id") or "").strip()
    if wikidata_id:
        graph.add((university_uri, OWL.sameAs, wd[wikidata_id]))

    add_typed_literal(graph, university_uri, vio.foundingYearOrg, row.get("foundingYearOrg", ""), XSD.gYear)
    add_integer_literal(graph, university_uri, vio.academicStaffSize, row.get("academicStaffSize", ""))
    add_integer_literal(graph, university_uri, vio.numberOfStudents, row.get("numberOfStudents", ""))

    return university_uri


def add_academic_entity(graph: Graph, university_uri: URIRef, row: dict, vio: Namespace, vres: Namespace, dbo: Namespace) -> None:
    """Tạo thực thể `vio:Academic` cho người đứng đầu trường nếu CSV có `head_name`."""
    head_name = (row.get("head_name") or "").strip()
    head_title = (row.get("head_title") or "").strip()
    if not head_name:
        return

    academic_uri = make_resource_uri(vres, "Academic", head_name)
    graph.add((academic_uri, RDF.type, vio.Academic))
    ensure_label_and_name(graph, academic_uri, vio, head_name)

    if head_title:
        graph.add((academic_uri, dbo.title, Literal(head_title, lang="vi")))

    graph.add((university_uri, vio.headOfUniversity, academic_uri))
    graph.add((academic_uri, vio.isHeadOf, university_uri))


def add_place_entities(graph: Graph, university_uri: URIRef, row: dict, vio: Namespace, vres: Namespace) -> None:
    """Tạo thực thể `vio:City` và `vio:Province`, cùng các quan hệ địa lý."""
    city_name = (row.get("city") or "").strip()
    province_name = (row.get("province") or "").strip()

    city_uri: Optional[URIRef] = None
    province_uri: Optional[URIRef] = None

    if province_name:
        province_uri = make_resource_uri(vres, "Province", province_name)
        graph.add((province_uri, RDF.type, vio.Province))
        ensure_label_and_name(graph, province_uri, vio, province_name)
        graph.add((university_uri, vio.locatedInProvince, province_uri))

    if city_name:
        city_uri = make_resource_uri(vres, "City", city_name)
        graph.add((city_uri, RDF.type, vio.City))
        ensure_label_and_name(graph, city_uri, vio, city_name)
        graph.add((university_uri, vio.locatedInCity, city_uri))

    if city_uri and province_uri:
        graph.add((city_uri, vio.isPartOf, province_uri))


def add_governing_body_entity(graph: Graph, university_uri: URIRef, row: dict, vio: Namespace, vres: Namespace) -> None:
    """Tạo thực thể cơ quan chủ quản và nối `vio:governedBy`."""
    governing_body_name = normalize_text(row.get("governing_body", ""))
    if not governing_body_name:
        return

    governing_body_uri = make_resource_uri(vres, "GoverningBody", governing_body_name)
    graph.add((governing_body_uri, RDF.type, vio.GoverningBody))
    ensure_label_and_name(graph, governing_body_uri, vio, governing_body_name)
    graph.add((university_uri, vio.governedBy, governing_body_uri))


def add_site_entities(graph: Graph, university_uri: URIRef, row: dict, vio: Namespace, vres: Namespace, geo: Namespace) -> None:
    """Tạo các `vio:Site` từ chuỗi `address/latitude/longitude/coord_level` phân tách bởi `|`."""
    addresses = split_pipe_values(row.get("address", ""))
    latitudes = split_pipe_values(row.get("latitude", ""))
    longitudes = split_pipe_values(row.get("longitude", ""))
    coord_levels = split_pipe_values(row.get("coord_level", ""))
    row_city = normalize_text(row.get("city", ""))
    row_province = normalize_text(row.get("province", ""))

    site_count = max(len(addresses), len(latitudes), len(longitudes), len(coord_levels))
    if site_count == 0:
        return

    addresses = pad_list(addresses, site_count)
    latitudes = pad_list(latitudes, site_count)
    longitudes = pad_list(longitudes, site_count)
    coord_levels = pad_list(coord_levels, site_count)

    page = (row.get("page") or row.get("name") or "site").strip()

    for index in range(site_count):
        address = addresses[index]
        latitude = latitudes[index]
        longitude = longitudes[index]
        coord_level = coord_levels[index]

        if not any([address, latitude, longitude, coord_level]):
            continue

        site_uri = make_resource_uri(vres, "Site", f"{page}_site_{index + 1}")
        graph.add((site_uri, RDF.type, vio.Site))
        graph.add((university_uri, vio.hasSite, site_uri))

        if address:
            graph.add((site_uri, RDFS.label, Literal(f"Cơ sở {index + 1}", lang="vi")))
            add_vi_literal(graph, site_uri, vio.address, address)
        add_float_literal(graph, site_uri, geo.lat, latitude)
        add_float_literal(graph, site_uri, geo.long, longitude)
        if coord_level:
            graph.add((site_uri, vio.coordLevel, Literal(coord_level, datatype=XSD.string)))

        site_city, site_province = infer_site_location_from_address(address, row_city, row_province, site_count)
        city_uri: Optional[URIRef] = None
        province_uri: Optional[URIRef] = None
        if site_province:
            province_uri = make_resource_uri(vres, "Province", site_province)
            graph.add((province_uri, RDF.type, vio.Province))
            ensure_label_and_name(graph, province_uri, vio, site_province)
            graph.add((site_uri, vio.locatedInProvince, province_uri))
        if site_city:
            city_uri = make_resource_uri(vres, "City", site_city)
            graph.add((city_uri, RDF.type, vio.City))
            ensure_label_and_name(graph, city_uri, vio, site_city)
            graph.add((site_uri, vio.locatedInCity, city_uri))
        if city_uri and province_uri:
            graph.add((city_uri, vio.isPartOf, province_uri))


def add_membership_relation(graph: Graph, university_uri: URIRef, row: dict, vio: Namespace, vres: Namespace) -> None:
    """Ánh xạ cột `is_member_of` thành quan hệ phân cấp giữa hai trường."""
    parent_name = (row.get("is_member_of") or "").strip()
    if not parent_name:
        return

    parent_uri = make_resource_uri(vres, "University", parent_name)
    graph.add((parent_uri, RDF.type, vio.University))
    ensure_label_and_name(graph, parent_uri, vio, parent_name)
    graph.add((university_uri, vio.isMemberOf, parent_uri))
    graph.add((parent_uri, vio.hasMember, university_uri))


# --- Graph construction -----------------------------------------------------
def build_graph(csv_path: Path) -> Graph:
    """Đọc CSV và sinh RDF graph hoàn chỉnh theo ontology `vio`."""
    graph = Graph()

    vio = Namespace(VIO_NS)
    vres = Namespace(VRES_NS)
    dbo = Namespace(DBO_NS)
    geo = Namespace(GEO_NS)
    wd = Namespace(WD_NS)

    graph.bind("vio", vio, override=True)
    graph.bind("vres", vres, override=True)
    graph.bind("dbo", dbo, override=True)
    graph.bind("geo", geo, override=True)
    graph.bind("owl", OWL, override=True)
    graph.bind("xsd", XSD, override=True)
    graph.bind("wd", wd, override=True)
    graph.bind("rdf", RDF, override=True)
    graph.bind("rdfs", RDFS, override=True)

    with csv_path.open("r", encoding="utf-8-sig", newline="") as file_obj:
        reader = csv.DictReader(file_obj)
        for row in reader:
            university_uri = add_university_entity(graph, row, vio, vres, geo, wd)
            if university_uri is None:
                continue

            add_academic_entity(graph, university_uri, row, vio, vres, dbo)
            add_place_entities(graph, university_uri, row, vio, vres)
            add_governing_body_entity(graph, university_uri, row, vio, vres)
            add_site_entities(graph, university_uri, row, vio, vres, geo)
            add_membership_relation(graph, university_uri, row, vio, vres)

    return graph


# --- Fuseki upload ----------------------------------------------------------
def upload_to_fuseki(ttl_path: Path, endpoint: str, username: str = "", password: str = "", timeout: int = 60) -> None:
    """Upload file Turtle lên Apache Fuseki qua endpoint `/data`.

    Ví dụ endpoint:
    - `http://localhost:3030/vio/data`
    """
    if not endpoint:
        raise ValueError("Fuseki endpoint is required for upload.")

    auth = (username, password) if username else None
    with ttl_path.open("rb") as file_obj:
        response = requests.post(
            endpoint,
            data=file_obj,
            headers={"Content-Type": "text/turtle"},
            auth=auth,
            timeout=timeout,
        )

    response.raise_for_status()


# --- Command-line entry point ----------------------------------------------
def main() -> None:
    """Parse CLI arguments, chuyển CSV sang Turtle và upload nếu được yêu cầu."""
    parser = argparse.ArgumentParser(description="Convert Vietnamese university CSV data into RDF/Turtle.")
    parser.add_argument("--input", default="data/vietnam_universities_details_full.csv", help="Input CSV file path.")
    parser.add_argument("--output", default="data/universities_instances.ttl", help="Output Turtle file path.")
    parser.add_argument("--upload", action="store_true", help="Upload the generated Turtle file to Fuseki after serialization.")
    parser.add_argument("--fuseki-endpoint", default="", help="Fuseki data endpoint, for example http://localhost:3030/vio/data")
    parser.add_argument("--fuseki-user", default="", help="Fuseki username for Basic Auth.")
    parser.add_argument("--fuseki-password", default="", help="Fuseki password for Basic Auth.")
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Đang chuyển đổi {input_path}...")
    graph = build_graph(input_path)
    graph.serialize(destination=str(output_path), format="turtle")
    print(f"Đã lưu {len(graph)} bộ ba (triples) vào {output_path}")

    if args.upload:
        print(f"Đang upload {output_path} lên Fuseki: {args.fuseki_endpoint}")
        upload_to_fuseki(
            ttl_path=output_path,
            endpoint=args.fuseki_endpoint,
            username=args.fuseki_user,
            password=args.fuseki_password,
        )
        print("Upload Fuseki thành công.")


if __name__ == "__main__":
    main()
