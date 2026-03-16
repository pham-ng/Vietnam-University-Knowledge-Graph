from pathlib import Path

from rdflib import Literal, Namespace, URIRef
from rdflib.namespace import OWL, RDF, RDFS, SKOS, XSD

from scripts.csv_to_ttl import build_graph


VIO_NS = "http://vi.dbpedia.org/ontology/"
VRES_NS = "http://vi.dbpedia.org/resource/"


def test_build_graph_creates_site_city_province_and_governing_body_entities(tmp_path: Path) -> None:
    csv_content = """name,page,wikidata_id,foundingYearOrg,address,city,province,is_member_of,head_name,head_title,governing_body,academicStaffSize,numberOfStudents,latitude,longitude,coord_level
Hanoi University of Science and Technology,Hanoi University of Science and Technology,Q123,1956,1 Đại Cồ Việt | 48 Tạ Quang Bửu,Hà Nội,Hà Nội,Đại học Quốc gia Hà Nội,Nguyễn Văn A,Hiệu trưởng,Bộ Giáo dục và Đào tạo,1200,35000,21.003 | 21.005,105.843 | 105.845,Exact | Exact
"""
    csv_path = tmp_path / "sample.csv"
    csv_path.write_text(csv_content, encoding="utf-8")

    graph = build_graph(csv_path)
    vio = Namespace(VIO_NS)
    vres = Namespace(VRES_NS)

    university = URIRef(vres["Hanoi_University_Of_Science_And_Technology"])
    site_1 = URIRef(vres["Hanoi_University_Of_Science_And_Technology_Site1"])
    site_2 = URIRef(vres["Hanoi_University_Of_Science_And_Technology_Site2"])
    city = URIRef(vres["Hanoi"])
    province = URIRef(vres["Hanoi_Province"])
    governing_body = URIRef(vres["MOET"])
    university_system = URIRef(vres["VNU_Hanoi"])
    wikidata = URIRef("http://www.wikidata.org/entity/Q123")

    assert (university, RDF.type, vio.University) in graph
    assert (site_1, RDF.type, vio.Site) in graph
    assert (site_2, RDF.type, vio.Site) in graph
    assert (university, vio.hasSite, site_1) in graph
    assert (university, vio.hasSite, site_2) in graph
    assert (site_1, vio.locatedInCity, city) in graph
    assert (site_1, vio.locatedInProvince, province) in graph
    assert (city, vio.isPartOf, province) in graph
    assert (university, vio.governedBy, governing_body) in graph
    assert (university, vio.name, Literal("Hanoi University of Science and Technology", lang="vi")) in graph
    assert (university, RDFS.label, Literal("Hanoi University of Science and Technology", lang="vi")) in graph
    assert (university, SKOS.prefLabel, Literal("Hanoi University of Science and Technology", lang="vi")) in graph
    assert (university, vio.hasWikidataID, Literal("Q123", datatype=XSD.string)) in graph
    assert (university, OWL.sameAs, wikidata) in graph
    assert (university, vio.isMemberOf, university_system) in graph
    assert (university_system, RDF.type, vio.UniversitySystem) in graph
    assert (city, vio.name, Literal("Hà Nội", lang="vi")) in graph
    assert (city, RDFS.label, Literal("Hà Nội", lang="vi")) in graph
    assert (city, SKOS.prefLabel, Literal("Hà Nội", lang="vi")) in graph
