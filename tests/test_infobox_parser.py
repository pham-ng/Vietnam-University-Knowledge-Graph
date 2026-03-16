from scripts.extract_vi_universities import parse_university_page


def test_parse_infobox_university_basic():
    wikitext = """
{{Infobox university
| name = [[Đại học Ví dụ]]
| established = 1999
| city = [[Hà Nội]]
}}
Nội dung khác
"""
    row = parse_university_page("Đại học Ví dụ", wikitext)

    assert row["name"] == "Đại học Ví dụ"
    assert row["foundingYearOrg"] == "1999"
    assert row["city"] == "Hà Nội"


def test_parse_infobox_university_fallback_name():
    wikitext = """
{{Infobox university
| established = 2001
| city = [[Đà Nẵng]]
}}
"""
    row = parse_university_page("Trường Không Có Name", wikitext)

    assert row["name"] == "Trường Không Có Name"
    assert row["foundingYearOrg"] == "2001"
    assert row["city"] == "Đà Nẵng"
