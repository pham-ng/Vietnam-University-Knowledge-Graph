#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import csv
import re
import time
from typing import Dict, List, Optional, Set, Tuple

import requests

API_URL = "https://vi.wikipedia.org/w/api.php"
WIKIDATA_API_URL = "https://www.wikidata.org/w/api.php"
CATEGORY_TITLE = "Category:Trường đại học tại Việt Nam"
FALLBACK_CATEGORIES = ["Category:Đại học Việt Nam", "Category:Trường đại học Việt Nam"]
OUTPUT_FILE = "data/vietnam_universities_details.csv"
MIN_YEAR = 1900
HISTORICAL_MIN_YEAR = 1850
MAX_YEAR = 2025
USER_AGENT = "vio-bot/2.0 (https://example.org; contact:research@example.org)"

INFOBOX_TEMPLATES = [
    "Infobox university",
    "Infobox school",
    "Thông tin trường đại học",
    "Thông tin trường học",
    "Thông tin đơn vị giáo dục",
]
INCLUDE_TITLE_KEYWORDS = ["Trường", "Học viện", "Đại học", "Cao đẳng"]
EXCLUDE_TITLE_PATTERNS = [
    r"^Danh sách",
    r"^List",
    r"\(định hướng\)",
]
SMALL_FACULTY_PATTERNS = [
    r"^Khoa\b",
    r"^Bộ\s*môn\b",
    r"^Tổ\s*bộ\s*môn\b",
]
CENTRAL_MUNICIPALITIES = {
    "hà nội": "Hà Nội",
    "hồ chí minh": "Hồ Chí Minh",
    "thành phố hồ chí minh": "Hồ Chí Minh",
    "tp hồ chí minh": "Hồ Chí Minh",
    "tp hcm": "Hồ Chí Minh",
    "tphcm": "Hồ Chí Minh",
    "hcm": "Hồ Chí Minh",
    "đà nẵng": "Đà Nẵng",
    "hải phòng": "Hải Phòng",
    "cần thơ": "Cần Thơ",
}
PROVINCE_NAMES = {
    "An Giang",
    "Bà Rịa - Vũng Tàu",
    "Bạc Liêu",
    "Bắc Giang",
    "Bắc Kạn",
    "Bắc Ninh",
    "Bến Tre",
    "Bình Dương",
    "Bình Định",
    "Bình Phước",
    "Bình Thuận",
    "Cà Mau",
    "Cao Bằng",
    "Cần Thơ",
    "Đà Nẵng",
    "Đắk Lắk",
    "Đắk Nông",
    "Điện Biên",
    "Đồng Nai",
    "Đồng Tháp",
    "Gia Lai",
    "Hà Giang",
    "Hà Nam",
    "Hà Nội",
    "Hà Tĩnh",
    "Hải Dương",
    "Hải Phòng",
    "Hậu Giang",
    "Hòa Bình",
    "Hồ Chí Minh",
    "Hưng Yên",
    "Khánh Hòa",
    "Kiên Giang",
    "Kon Tum",
    "Lai Châu",
    "Lâm Đồng",
    "Lạng Sơn",
    "Lào Cai",
    "Long An",
    "Nam Định",
    "Nghệ An",
    "Ninh Bình",
    "Ninh Thuận",
    "Phú Thọ",
    "Phú Yên",
    "Quảng Bình",
    "Quảng Nam",
    "Quảng Ngãi",
    "Quảng Ninh",
    "Quảng Trị",
    "Sóc Trăng",
    "Sơn La",
    "Tây Ninh",
    "Thái Bình",
    "Thái Nguyên",
    "Thanh Hóa",
    "Thừa Thiên Huế",
    "Tiền Giang",
    "Trà Vinh",
    "Tuyên Quang",
    "Vĩnh Long",
    "Vĩnh Phúc",
    "Yên Bái",
}
PROVINCE_LOOKUP = {name.lower(): name for name in PROVINCE_NAMES}
PROVINCE_LOOKUP.update({alias: canonical for alias, canonical in CENTRAL_MUNICIPALITIES.items()})
CITY_TO_PROVINCE = {
    "huế": "Thừa Thiên Huế",
    "thành phố huế": "Thừa Thiên Huế",
    "vinh": "Nghệ An",
    "thành phố vinh": "Nghệ An",
    "nha trang": "Khánh Hòa",
    "thành phố nha trang": "Khánh Hòa",
    "đà lạt": "Lâm Đồng",
    "thành phố đà lạt": "Lâm Đồng",
    "quy nhơn": "Bình Định",
    "thành phố quy nhơn": "Bình Định",
    "biên hòa": "Đồng Nai",
    "thành phố biên hòa": "Đồng Nai",
    "thái nguyên": "Thái Nguyên",
    "thành phố thái nguyên": "Thái Nguyên",
    "việt trì": "Phú Thọ",
    "thành phố việt trì": "Phú Thọ",
    "buôn ma thuột": "Đắk Lắk",
    "thành phố buôn ma thuột": "Đắk Lắk",
    "pleiku": "Gia Lai",
    "thành phố pleiku": "Gia Lai",
    "tam kỳ": "Quảng Nam",
    "thành phố tam kỳ": "Quảng Nam",
    "mỹ tho": "Tiền Giang",
    "thành phố mỹ tho": "Tiền Giang",
    "long xuyên": "An Giang",
    "thành phố long xuyên": "An Giang",
    "rạch giá": "Kiên Giang",
    "thành phố rạch giá": "Kiên Giang",
    "trà vinh": "Trà Vinh",
    "thành phố trà vinh": "Trà Vinh",
    "tuy hòa": "Phú Yên",
    "thành phố tuy hòa": "Phú Yên",
}
CITY_CANONICAL_NAMES = {
    "huế": "Huế",
    "thành phố huế": "Huế",
    "vinh": "Vinh",
    "thành phố vinh": "Vinh",
    "nha trang": "Nha Trang",
    "thành phố nha trang": "Nha Trang",
    "đà lạt": "Đà Lạt",
    "thành phố đà lạt": "Đà Lạt",
    "quy nhơn": "Quy Nhơn",
    "thành phố quy nhơn": "Quy Nhơn",
    "biên hòa": "Biên Hòa",
    "thành phố biên hòa": "Biên Hòa",
    "thái nguyên": "Thái Nguyên",
    "thành phố thái nguyên": "Thái Nguyên",
    "việt trì": "Việt Trì",
    "thành phố việt trì": "Việt Trì",
    "buôn ma thuột": "Buôn Ma Thuột",
    "thành phố buôn ma thuột": "Buôn Ma Thuột",
    "pleiku": "Pleiku",
    "thành phố pleiku": "Pleiku",
    "tam kỳ": "Tam Kỳ",
    "thành phố tam kỳ": "Tam Kỳ",
    "mỹ tho": "Mỹ Tho",
    "thành phố mỹ tho": "Mỹ Tho",
    "long xuyên": "Long Xuyên",
    "thành phố long xuyên": "Long Xuyên",
    "rạch giá": "Rạch Giá",
    "thành phố rạch giá": "Rạch Giá",
    "trà vinh": "Trà Vinh",
    "thành phố trà vinh": "Trà Vinh",
    "tuy hòa": "Tuy Hòa",
    "thành phố tuy hòa": "Tuy Hòa",
}
PROVINCE_COORDS = {
    "An Giang": (10.5216, 105.1259),
    "Bà Rịa - Vũng Tàu": (10.5417, 107.2428),
    "Bạc Liêu": (9.2941, 105.7216),
    "Bắc Giang": (21.2731, 106.1946),
    "Bắc Kạn": (22.1470, 105.8348),
    "Bắc Ninh": (21.1861, 106.0763),
    "Bến Tre": (10.2434, 106.3758),
    "Bình Dương": (11.3254, 106.4770),
    "Bình Định": (13.7829, 109.2197),
    "Bình Phước": (11.7512, 106.7235),
    "Bình Thuận": (11.0904, 108.0721),
    "Cà Mau": (9.1769, 105.1524),
    "Cao Bằng": (22.6666, 106.2570),
    "Cần Thơ": (10.0452, 105.7469),
    "Đà Nẵng": (16.0544, 108.2022),
    "Đắk Lắk": (12.7100, 108.2378),
    "Đắk Nông": (12.2646, 107.6098),
    "Điện Biên": (21.3860, 103.0230),
    "Đồng Nai": (10.9447, 106.8243),
    "Đồng Tháp": (10.4938, 105.6882),
    "Gia Lai": (13.8079, 108.1094),
    "Hà Giang": (22.8026, 104.9784),
    "Hà Nam": (20.5835, 105.9229),
    "Hà Nội": (21.0278, 105.8342),
    "Hà Tĩnh": (18.3559, 105.8877),
    "Hải Dương": (20.9373, 106.3146),
    "Hải Phòng": (20.8449, 106.6881),
    "Hậu Giang": (9.7579, 105.6413),
    "Hòa Bình": (20.8130, 105.3386),
    "Hồ Chí Minh": (10.8231, 106.6297),
    "Hưng Yên": (20.6464, 106.0511),
    "Khánh Hòa": (12.2585, 109.0526),
    "Kiên Giang": (10.0125, 105.0809),
    "Kon Tum": (14.3497, 108.0005),
    "Lai Châu": (22.3964, 103.4582),
    "Lâm Đồng": (11.9404, 108.4583),
    "Lạng Sơn": (21.8537, 106.7615),
    "Lào Cai": (22.4809, 103.9755),
    "Long An": (10.6956, 106.2431),
    "Nam Định": (20.4388, 106.1621),
    "Nghệ An": (19.2342, 104.9200),
    "Ninh Bình": (20.2506, 105.9745),
    "Ninh Thuận": (11.6739, 108.8629),
    "Phú Thọ": (21.2684, 105.2046),
    "Phú Yên": (13.0882, 109.0929),
    "Quảng Bình": (17.6103, 106.3487),
    "Quảng Nam": (15.5394, 108.0191),
    "Quảng Ngãi": (15.1214, 108.8044),
    "Quảng Ninh": (21.0064, 107.2925),
    "Quảng Trị": (16.7403, 107.1855),
    "Sóc Trăng": (9.6025, 105.9739),
    "Sơn La": (21.1022, 103.7289),
    "Tây Ninh": (11.3352, 106.1099),
    "Thái Bình": (20.4463, 106.3366),
    "Thái Nguyên": (21.5672, 105.8252),
    "Thanh Hóa": (19.8067, 105.7852),
    "Thừa Thiên Huế": (16.4637, 107.5909),
    "Tiền Giang": (10.4493, 106.3421),
    "Trà Vinh": (9.8127, 106.2993),
    "Tuyên Quang": (21.7767, 105.2280),
    "Vĩnh Long": (10.2397, 105.9572),
    "Vĩnh Phúc": (21.3609, 105.5474),
    "Yên Bái": (21.7168, 104.8986),
}
CITY_COORDS = {
    "Hà Nội": (21.0278, 105.8342),
    "Hồ Chí Minh": (10.8231, 106.6297),
    "Hải Phòng": (20.8449, 106.6881),
    "Đà Nẵng": (16.0544, 108.2022),
    "Cần Thơ": (10.0452, 105.7469),
    "Huế": (16.4637, 107.5909),
    "Vinh": (18.6796, 105.6813),
    "Nha Trang": (12.2388, 109.1967),
    "Đà Lạt": (11.9404, 108.4583),
    "Quy Nhơn": (13.7820, 109.2197),
    "Biên Hòa": (10.9447, 106.8243),
    "Thái Nguyên": (21.5942, 105.8482),
    "Việt Trì": (21.3227, 105.4019),
    "Buôn Ma Thuột": (12.6667, 108.0500),
    "Pleiku": (13.9833, 108.0000),
    "Tam Kỳ": (15.5736, 108.4740),
    "Mỹ Tho": (10.3600, 106.3597),
    "Long Xuyên": (10.3864, 105.4352),
    "Rạch Giá": (10.0125, 105.0809),
    "Trà Vinh": (9.9347, 106.3453),
    "Tuy Hòa": (13.0955, 109.3209),
    "Nam Định": (20.4388, 106.1621),
    "Thanh Hóa": (19.8075, 105.7760),
    "Hạ Long": (20.9712, 107.0448),
    "Đồng Hới": (17.4689, 106.6220),
    "Phan Thiết": (10.9804, 108.2615),
    "Cà Mau": (9.1769, 105.1524),
}
DEFAULT_VIETNAM_COORDS = (16.047079, 108.206230)
IGNORED_LOCATION_VALUES = {"việt nam", "viet nam"}
LOW_LEVEL_LOCATION_PREFIXES = (
    "quận ",
    "huyện ",
    "phường ",
    "xã ",
    "thị xã ",
    "thị trấn ",
    "thôn ",
    "xóm ",
    "bản ",
    "ấp ",
    "khu phố ",
    "khu ",
    "tổ ",
    "khóm ",
)
ADDRESS_KEYWORDS = (
    "đường ",
    "đại lộ ",
    "quốc lộ ",
    "tỉnh lộ ",
    "ngõ ",
    "ngách ",
    "hẻm ",
    "kiệt ",
    "số ",
    "số nhà ",
    "km ",
)
HEAD_FIELD_LABELS = {
    "rector": "Hiệu trưởng",
    "chancellor": "Chancellor",
    "president": "Chủ tịch",
    "head": "Người đứng đầu",
    "hiệu_trưởng": "Hiệu trưởng",
    "hieu_truong": "Hiệu trưởng",
    "giám_đốc": "Giám đốc",
    "giam_doc": "Giám đốc",
}
WIKIDATA_HEAD_PROPERTIES = [
    ("P1075", "Hiệu trưởng"),
    ("P488", "Chủ tịch"),
    ("P1037", "Giám đốc"),
]
WIKIDATA_GOVERNING_BODY_PROPERTIES = ["P137", "P112"]
PERSON_ROLE_PREFIX_PATTERN = re.compile(
    r"^(?:ông|bà|gs\.?\s*ts\.?|pgs\.?\s*ts\.?|gs\.?|pgs\.?|ts\.?|ths\.?|ths\.?|th\.s\.?|dr\.?|prof\.?|assoc\.?(?:iate)?\s*prof\.?|phó\s*giáo\s*sư|giáo\s*sư|tiến\s*sĩ|thạc\s*sĩ|ngut|ngnd|ahlđ)\s+",
    flags=re.I,
)
LOCATION_NAME_CANDIDATES = sorted(
    set(PROVINCE_NAMES) | set(CENTRAL_MUNICIPALITIES.values()) | set(CITY_CANONICAL_NAMES.values()),
    key=len,
    reverse=True,
)
GOVERNING_BODY_KEYWORD_RULES = [
    ("Đại học Quốc gia Hà Nội", ["đại học quốc gia hà nội", "đhqghn"]),
    (
        "Đại học Quốc gia Thành phố Hồ Chí Minh",
        ["đại học quốc gia thành phố hồ chí minh", "đại học quốc gia tp.hcm", "đại học quốc gia tp hcm", "đhqg tp.hcm", "đhqg-hcm"],
    ),
    ("Bộ Quốc phòng", ["sĩ quan", "quân sự", "quân y", "hậu cần", "tăng thiết giáp", "phòng không", "không quân", "biên phòng", "tình báo"]),
    ("Bộ Công an", ["an ninh", "cảnh sát", "phòng cháy"]),
    ("Bộ Y tế", ["y tế", "điều dưỡng"]),
    ("Bộ Giao thông Vận tải", ["giao thông vận tải"]),
    ("Bộ Tài chính", ["tài chính", "kế toán", "thuế", "hải quan"]),
    ("Bộ Ngoại giao", ["ngoại giao"]),
    ("Bộ Công Thương", ["công nghiệp", "điện lực", "xúc tiến thương mại"]),
    ("Bộ Văn hóa, Thể thao và Du lịch", ["du lịch", "nghệ thuật", "âm nhạc", "mỹ thuật", "thể dục thể thao"]),
]
GOVERNING_BODY_ALIASES = {
    "bộ gd&đt": "Bộ Giáo dục và Đào tạo",
    "bộ gdđt": "Bộ Giáo dục và Đào tạo",
    "bộ giáo dục & đào tạo": "Bộ Giáo dục và Đào tạo",
    "bộ giáo dục và đào tạo": "Bộ Giáo dục và Đào tạo",
    "bộ quốc phòng việt nam": "Bộ Quốc phòng",
    "bộ quốc phòng": "Bộ Quốc phòng",
    "bộ công an": "Bộ Công an",
    "bộ y tế": "Bộ Y tế",
    "bộ giao thông vận tải": "Bộ Giao thông Vận tải",
    "bộ tài chính": "Bộ Tài chính",
    "bộ ngoại giao": "Bộ Ngoại giao",
    "bộ công thương": "Bộ Công Thương",
    "bộ văn hóa thể thao và du lịch": "Bộ Văn hóa, Thể thao và Du lịch",
    "bộ văn hóa, thể thao và du lịch": "Bộ Văn hóa, Thể thao và Du lịch",
    "đại học quốc gia hà nội": "Đại học Quốc gia Hà Nội",
    "đại học quốc gia thành phố hồ chí minh": "Đại học Quốc gia Thành phố Hồ Chí Minh",
    "đại học quốc gia tp.hcm": "Đại học Quốc gia Thành phố Hồ Chí Minh",
    "đại học quốc gia tp hcm": "Đại học Quốc gia Thành phố Hồ Chí Minh",
}
MANUAL_GOVERNING_BODY_MAP = {
    "Đại học Cần Thơ": "Bộ Giáo dục và Đào tạo",
    "Trường Đại học Đà Lạt": "Bộ Giáo dục và Đào tạo",
    "Trường Đại học Cần Thơ": "Bộ Giáo dục và Đào tạo",
    "Đại học Đà Lạt": "Bộ Giáo dục và Đào tạo",
    "Đại học Y Hà Nội": "Bộ Y tế",
    "Trường Đại học Y Hà Nội": "Bộ Y tế",
    "Trường Đại học Dược Hà Nội": "Bộ Y tế",
    "Trường Đại học Y tế công cộng": "Bộ Y tế",
    "Trường Đại học Điều dưỡng Nam Định": "Bộ Y tế",
    "Trường Đại học Kỹ thuật Y tế Hải Dương": "Bộ Y tế",
    "Trường Đại học Y Dược Thành phố Hồ Chí Minh": "Bộ Y tế",
    "Trường Đại học Y Dược Cần Thơ": "Bộ Y tế",
    "Trường Đại học Y Dược Hải Phòng": "Bộ Y tế",
    "Trường Đại học Y Dược Thái Bình": "Bộ Y tế",
    "Trường Đại học Kỹ thuật Y Dược Đà Nẵng": "Bộ Y tế",
    "Học viện Y – Dược học cổ truyền Việt Nam": "Bộ Y tế",
    "Học viện Y Dược học cổ truyền Việt Nam": "Bộ Y tế",
    "Trường Đại học Y Dược, Đại học Huế": "Đại học Huế",
    "Trường Đại học Y Dược, Đại học Thái Nguyên": "Đại học Thái Nguyên",
    "Trường Đại học Y Dược, Đại học Quốc gia Hà Nội": "Đại học Quốc gia Hà Nội",
    "Học viện Ngoại giao": "Bộ Ngoại giao",
    "Học viện Ngân hàng": "Ngân hàng Nhà nước Việt Nam",
    "Trường Đại học Ngân hàng Thành phố Hồ Chí Minh": "Ngân hàng Nhà nước Việt Nam",
    "Đại học Ngân hàng Thành phố Hồ Chí Minh": "Ngân hàng Nhà nước Việt Nam",
    "Học viện Tài chính": "Bộ Tài chính",
    "Học viện Chính sách và Phát triển": "Bộ Tài chính",
    "Trường Đại học Tài chính - Kế toán": "Bộ Tài chính",
    "Trường Đại học Tài chính – Kế toán": "Bộ Tài chính",
    "Trường Đại học Tài chính – Marketing": "Bộ Tài chính",
    "Trường Đại học Tài chính – Quản trị kinh doanh": "Bộ Tài chính",
    "Trường Đại học Luật Hà Nội": "Bộ Tư pháp",
    "Đại học Luật Hà Nội": "Bộ Tư pháp",
    "Học viện Tư pháp": "Bộ Tư pháp",
    "Học viện Tòa án": "Tòa án nhân dân tối cao",
    "Trường Đại học Kiểm sát": "Viện kiểm sát nhân dân tối cao",
    "Trường Đại học Kiểm sát Hà Nội": "Viện kiểm sát nhân dân tối cao",
    "Học viện Báo chí và Tuyên truyền": "Học viện Chính trị quốc gia Hồ Chí Minh",
    "Học viện Hành chính và Quản trị công": "Học viện Chính trị quốc gia Hồ Chí Minh",
    "Học viện Chính trị Quốc gia Hồ Chí Minh": "Ban Chấp hành Trung ương Đảng Cộng sản Việt Nam",
    "Học viện Thanh thiếu niên Việt Nam": "Trung ương Đoàn Thanh niên Cộng sản Hồ Chí Minh",
    "Học viện Phụ nữ Việt Nam": "Trung ương Hội Liên hiệp Phụ nữ Việt Nam",
    "Học viện Dân tộc": "Bộ Dân tộc và Tôn giáo",
    "Học viện Quản lý giáo dục": "Bộ Giáo dục và Đào tạo",
    "Học viện Nông nghiệp Việt Nam": "Bộ Nông nghiệp và Môi trường",
    "Học viện Công nghệ Bưu chính Viễn thông": "Bộ Khoa học và Công nghệ",
    "Học viện Khoa học và Công nghệ": "Viện Hàn lâm Khoa học và Công nghệ Việt Nam",
    "Học viện Khoa học Xã hội": "Viện Hàn lâm Khoa học Xã hội Việt Nam",
    "Học viện Kỹ thuật Mật mã": "Ban Cơ yếu Chính phủ",
    "Học viện Âm nhạc Huế": "Bộ Văn hóa, Thể thao và Du lịch",
    "Học viện Âm nhạc Quốc gia Việt Nam": "Bộ Văn hóa, Thể thao và Du lịch",
    "Học viện Múa Việt Nam": "Bộ Văn hóa, Thể thao và Du lịch",
    "Học viện Hàng không Việt Nam": "Bộ Giao thông Vận tải",
    "Học viện Chính trị Công an nhân dân": "Bộ Công an",
    "Học viện Kỹ thuật và Công nghệ An ninh": "Bộ Công an",
    "Học viện An ninh nhân dân": "Bộ Công an",
    "Học viện Cảnh sát nhân dân": "Bộ Công an",
    "Học viện Hải quân": "Bộ Quốc phòng",
    "Học viện Quân chính": "Bộ Quốc phòng",
    "Học viện Chính trị": "Bộ Quốc phòng",
    "Học viện Hậu cần": "Bộ Quốc phòng",
    "Học viện Khoa học Quân sự": "Bộ Quốc phòng",
    "Học viện Quân y": "Bộ Quốc phòng",
    "Học viện Kỹ thuật Quân sự": "Bộ Quốc phòng",
    "Trường Đại học Công đoàn": "Tổng Liên đoàn Lao động Việt Nam",
    "Trường Đại học Tôn Đức Thắng": "Tổng Liên đoàn Lao động Việt Nam",
    "Đại học Tôn Đức Thắng": "Tổng Liên đoàn Lao động Việt Nam",
    "Học viện Cán bộ Thành phố Hồ Chí Minh": "Thành ủy Thành phố Hồ Chí Minh",
    "Học viện Phật giáo Việt Nam": "Giáo hội Phật giáo Việt Nam",
    "Học viện Công giáo Việt Nam": "Hội đồng Giám mục Việt Nam",
}


def create_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    return session


def request_json(
    session: requests.Session,
    url: str,
    params: Dict[str, object],
    max_retries: int = 3,
    timeout: int = 30,
) -> Dict:
    last_error: Optional[Exception] = None
    for attempt in range(max_retries):
        try:
            response = session.get(url, params=params, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except Exception as exc:
            last_error = exc
            if attempt < max_retries - 1:
                time.sleep(0.8 * (attempt + 1))
    raise last_error if last_error else RuntimeError("Request failed")


def clean_wiki_markup(value: str) -> str:
    if not value:
        return ""

    text = value
    text = re.sub(r"<!--.*?-->", "", text, flags=re.S)
    text = re.sub(r"<ref[^>/]*/>", "", text, flags=re.I)
    text = re.sub(r"<ref[^>]*>.*?</ref>", "", text, flags=re.I | re.S)
    text = re.sub(r"\[\[([^|\]]+)\|([^\]]+)\]\]", r"\2", text)
    text = re.sub(r"\[\[([^\]]+)\]\]", r"\1", text)
    text = re.sub(r"\{\{lang\|[^|]+\|([^}]+)\}\}", r"\1", text, flags=re.I)
    text = re.sub(r"\{\{nowrap\|([^}]+)\}\}", r"\1", text, flags=re.I)
    text = re.sub(r"\{\{convert\|([^|}]+)(?:\|[^}]*)?\}\}", r"\1", text, flags=re.I)
    text = re.sub(r"\{\{(?:flag|flagicon|VNM|VN|viết tắt|citation needed|cn|sfnp|sfn|efn|clarify|fact)\b[^}]*\}\}", "", text, flags=re.I)
    previous = None
    while previous != text:
        previous = text
        text = re.sub(r"\{\{[^{}]*\}\}", "", text)
    text = re.sub(r"''+", "", text)
    text = re.sub(r"<[^>]+>", "", text)
    previous = None
    while previous != text:
        previous = text
        text = re.sub(r"\([^()]*\)", "", text)
        text = re.sub(r"（[^（）]*）", "", text)
    text = re.sub(r"\b(?:cần dẫn nguồn|citation needed|không rõ nguồn)\b", "", text, flags=re.I)
    text = re.sub(r"^[•·●▪◦▪*]+", "", text)
    text = re.sub(r"\s*[•·●▪◦▪]+\s*", " ", text)
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"\s*,\s*", ", ", text)
    text = re.sub(r"\s*;\s*", "; ", text)
    text = re.sub(r"(^[\s,;.-]+|[\s,;.-]+$)", "", text)
    return text.strip()


def clean_person_name(value: str) -> str:
    text = clean_wiki_markup(value)
    if not text:
        return ""

    text = re.sub(r"\b(?:hiệu\s*trưởng|chancellor|president|head|giám\s*đốc|rector|chairperson|director)\b\s*[:\-–]\s*", "", text, flags=re.I)
    text = re.sub(r"\b(PGS|GS|TS|ThS|Th\.S)\s*,\s*", r"\1 ", text, flags=re.I)
    text = re.split(r"\s*(?:;|/|\||\n)\s*", text, maxsplit=1)[0]

    previous = None
    while previous != text:
        previous = text
        text = PERSON_ROLE_PREFIX_PATTERN.sub("", text).strip()

    text = re.sub(r"\b(?:hiệu\s*trưởng|chancellor|president|head|giám\s*đốc|rector|chairperson|director)\b", "", text, flags=re.I)
    text = re.sub(r"\s{2,}", " ", text)
    return text.strip(" ,;:-")


def clean_organization_name(value: str) -> str:
    text = clean_wiki_markup(value)
    if not text:
        return ""

    cutoff_pattern = r"\s+(?:có\s+trách\s+nhiệm|được\s+thành\s+lập|là|trực\s+thuộc|nhằm|với\s+mục\s+đích)\b"

    ministry_match = re.search(r"(Bộ\s+[A-ZÀ-ỴĐ][^,;:.]{2,100}|Ủy ban nhân dân\s+[^,;:.]{2,100}|UBND\s+[^,;:.]{2,100})", text, flags=re.I)
    if ministry_match:
        candidate = clean_wiki_markup(ministry_match.group(1)).strip(" ,;:-")
        candidate = re.split(cutoff_pattern, candidate, maxsplit=1, flags=re.I)[0]
        if len(candidate) > 50:
            candidate = re.split(r"[,.]", candidate, maxsplit=1)[0]
        return candidate.strip(" ,;:-")

    text = re.sub(r"^(?:cơ\s*quan\s*chủ\s*quản|chu\s*quan|chủ\s*quản|trực\s*thuộc|thuộc)\s*[:\-–]?\s*", "", text, flags=re.I)
    text = re.split(r"\s*(?:\n|;|/|\|)\s*", text, maxsplit=1)[0]
    text = re.split(cutoff_pattern, text, maxsplit=1, flags=re.I)[0]
    if len(text) > 50:
        text = re.split(r"[,.]", text, maxsplit=1)[0]
    text = re.sub(r"\s{2,}", " ", text)
    return text.strip(" ,;:-")


def normalize_governing_body_name(value: str) -> str:
    organization = clean_organization_name(value)
    if not organization:
        return ""

    normalized = organization.replace("ĐH Quốc gia", "Đại học Quốc gia")
    lowered = normalized.lower()
    lowered = re.sub(r"\s+", " ", lowered).strip()
    lowered = lowered.replace("tp.hcm", "thành phố hồ chí minh").replace("tp hcm", "thành phố hồ chí minh")
    lowered = lowered.replace("gd&đt", "giáo dục và đào tạo").replace("gdđt", "giáo dục và đào tạo")
    lowered = lowered.replace("&", " và ")
    lowered = re.sub(r"\s+", " ", lowered).strip(" ,;:-")

    if lowered in GOVERNING_BODY_ALIASES:
        return GOVERNING_BODY_ALIASES[lowered]
    return normalized.strip(" ,;:-")


def infer_governing_body_from_name(name: str) -> str:
    lowered = clean_wiki_markup(name).lower()
    if not lowered:
        return ""

    for governing_body, keywords in GOVERNING_BODY_KEYWORD_RULES:
        if any(keyword in lowered for keyword in keywords):
            if governing_body == "Bộ Y tế" and "điều dưỡng" in lowered:
                return governing_body
            if governing_body == "Bộ Y tế" and "cao đẳng" in lowered:
                return governing_body
            if governing_body != "Bộ Y tế":
                return governing_body
            if "y tế" in lowered:
                return governing_body
    return ""


def format_ubnd_governing_body(province: str) -> str:
    canonical = canonicalize_province_name(province) or canonicalize_city_name(province) or normalize_location_name(province)
    if not canonical:
        return ""
    if canonical in {"Hà Nội", "Hồ Chí Minh", "Hải Phòng", "Đà Nẵng", "Cần Thơ"}:
        return f"UBND Thành phố {canonical}"
    return f"UBND Tỉnh {canonical}"


def get_manual_governing_body(university_name: str, page_title: str) -> str:
    for candidate in [university_name, page_title]:
        cleaned = clean_wiki_markup(candidate).strip()
        if cleaned in MANUAL_GOVERNING_BODY_MAP:
            return MANUAL_GOVERNING_BODY_MAP[cleaned]
    return ""


def is_general_university_institution(university_name: str, page_title: str) -> bool:
    text = clean_wiki_markup(" ".join(part for part in [university_name, page_title] if part)).lower()
    return any(keyword in text for keyword in ["trường đại học", "đại học ", " university"])


def get_final_governing_body(
    university_name: str,
    page_title: str,
    wikidata_governing_body: str,
    infobox_governing_body: str,
) -> str:
    manual_override = get_manual_governing_body(university_name, page_title)
    if manual_override:
        return manual_override

    for candidate in [wikidata_governing_body, infobox_governing_body]:
        normalized = normalize_governing_body_name(candidate)
        if normalized:
            return normalized

    heuristic = infer_governing_body_from_name(university_name or page_title)
    if heuristic:
        return heuristic

    if is_general_university_institution(university_name, page_title):
        return "Bộ Giáo dục và Đào tạo"

    _, inferred_city, inferred_province = infer_location_from_university_name(university_name, page_title)
    ubnd_province = inferred_province or inferred_city
    if ubnd_province:
        return format_ubnd_governing_body(ubnd_province)
    return ""


def get_intro_plain_text(wikitext: str, max_chars: int = 1800) -> str:
    intro = (wikitext or "").split("==", 1)[0][:max_chars]
    return clean_wiki_markup(intro)


def infer_location_from_free_text(value: str) -> Tuple[str, str]:
    cleaned = clean_wiki_markup(value)
    if not cleaned:
        return "", ""

    lowered = cleaned.lower()
    for candidate in LOCATION_NAME_CANDIDATES:
        candidate_lower = candidate.lower()
        if re.search(rf"(?<!\w){re.escape(candidate_lower)}(?!\w)", lowered, flags=re.I):
            city = canonicalize_city_name(candidate)
            province = canonicalize_province_name(candidate)
            if city:
                return city, province or infer_province_from_city(city)
            if province:
                return "", province
    return "", ""


def extract_location_from_text_context(wikitext: str) -> Tuple[str, str, str]:
    sample = get_intro_plain_text(wikitext, max_chars=2200)
    if not sample:
        return "", "", ""

    patterns = [
        r"(?:đóng tại|đặt tại|tọa lạc tại|trụ sở(?: chính)? tại|đặt trụ sở tại)\s+([^.\n;]{0,160})",
        r"(?:nằm tại|ở tại|ở)\s+([^.\n;]{0,120})",
    ]
    for pattern in patterns:
        for match in re.finditer(pattern, sample, flags=re.I):
            fragment = clean_wiki_markup(match.group(1)).strip(" ,;:-")
            fragment = re.split(r"(?:ngày\s+\d|lịch\s*sử|được\s*thành\s*lập|thành\s*lập)", fragment, maxsplit=1, flags=re.I)[0].strip(" ,;:-")
            if not fragment:
                continue
            address, city, province = parse_location_components(fragment)
            if city or province:
                return address, city, province
            inferred_city, inferred_province = infer_location_from_free_text(fragment)
            if inferred_city or inferred_province:
                return fragment, inferred_city, inferred_province
    return "", "", ""


def infer_location_from_university_name(name: str, page_title: str) -> Tuple[str, str, str]:
    for candidate_text in [name, page_title]:
        city, province = infer_location_from_free_text(candidate_text)
        if city or province:
            return city or province, city, province
    return "", "", ""


def extract_managing_org_from_infobox(fields: Dict[str, str]) -> str:
    value = pick_field(
        fields,
        [
            "governing_body",
            "operator",
            "academic_affiliations",
            "academic_affiliation",
            "affiliations",
            "affiliation",
            "bộ_chủ_quản",
            "bo_chu_quan",
            "owner",
            "parent",
            "cơ_quan_chủ_quản",
            "co_quan_chu_quan",
            "chủ_quản",
            "chu_quan",
            "trực_thuộc",
            "truc_thuoc",
            "agency",
        ],
    )
    return normalize_governing_body_name(value)


def extract_managing_org_from_text(wikitext: str) -> str:
    sample = get_intro_plain_text(wikitext, max_chars=2200)
    patterns = [
        r"(?:trực thuộc|thuộc|cơ quan chủ quản(?: là)?)\s+((?:Bộ|Ủy ban nhân dân|UBND|Đại học Quốc gia|Quân chủng|Tổng Liên đoàn)[^.\n;]{0,120})",
        r"[,\-]\s*((?:Bộ|Ủy ban nhân dân|UBND)\s+[^.\n;]{0,100})",
    ]
    for pattern in patterns:
        match = re.search(pattern, sample, flags=re.I)
        if match:
            organization = clean_organization_name(match.group(1))
            if organization:
                return organization
    return ""


def pick_field_with_key(fields: Dict[str, str], candidates: List[str]) -> Tuple[str, str]:
    for candidate in candidates:
        key = candidate.lower().replace(" ", "_")
        if key in fields and fields[key].strip():
            return key, fields[key].strip()
    return "", ""


def extract_head_from_infobox(fields: Dict[str, str]) -> Tuple[str, str]:
    head_key, head_value = pick_field_with_key(fields, list(HEAD_FIELD_LABELS.keys()))
    if not head_value:
        return "", ""

    title = HEAD_FIELD_LABELS.get(head_key, "")
    title_match = re.search(
        r"\b(hiệu\s*trưởng|chancellor|president|head|giám\s*đốc|rector|chairperson|director)\b",
        clean_wiki_markup(head_value),
        flags=re.I,
    )
    if title_match and not title:
        title = title_match.group(1).strip()

    return clean_person_name(head_value), clean_wiki_markup(title)


def extract_head_from_text_context(wikitext: str) -> Tuple[str, str]:
    sample = get_intro_plain_text(wikitext, max_chars=3200)
    if not sample:
        return "", ""

    title_patterns = [
        (r"(?:hiệu\s*trưởng|quyền\s*hiệu\s*trưởng)", "Hiệu trưởng"),
        (r"(?:giám\s*đốc|quyền\s*giám\s*đốc)", "Giám đốc"),
        (r"(?:chủ\s*tịch)", "Chủ tịch"),
        (r"(?:người\s*đứng\s*đầu)", "Người đứng đầu"),
    ]
    for pattern, default_title in title_patterns:
        match = re.search(pattern, sample, flags=re.I)
        if not match:
            continue
        candidate = sample[match.end():match.end() + 120]
        candidate = re.sub(r"^(?:\s+hiện\s+nay)?(?:\s+là|\s*:|\s*[-–])?\s*", "", candidate, flags=re.I)
        candidate = re.split(r"(?:[;\n]|,\s*(?:nhiệm\s*kỳ|từ\s*năm|sinh\s*năm|đồng\s*thời|đã|là\s*một))", candidate, maxsplit=1, flags=re.I)[0]
        person_name = clean_person_name(candidate)
        if person_name and len(person_name) >= 6 and not re.search(r"\d", person_name):
            return person_name, default_title
    return "", ""


def extract_representative_people(value: str, limit: int = 3) -> List[str]:
    cleaned = clean_wiki_markup(value)
    if not cleaned:
        return []

    candidates = re.split(r"\s*(?:\n|;|/|\||•|,\s*(?=[A-ZÀ-ỴĐ]))\s*", cleaned)
    names: List[str] = []
    seen: Set[str] = set()
    for candidate in candidates:
        person_name = clean_person_name(candidate)
        if not person_name or len(person_name) < 5:
            continue
        lowered = person_name.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        names.append(person_name)
        if len(names) >= limit:
            break
    return names


ADDRESS_LIKE_CONTEXT_RE = re.compile(
    r"(?i)(?:\bsố\b|\bsố nhà\b|\bđường\b|\bquốc lộ\b|\bql\b|\bkm\b|\bngõ\b|\bhẻm\b|\bphường\b|\bquận\b|\bthôn\b|\bxã\b|\bhuyện\b|\btỉnh\b)"
)

HISTORICAL_YEAR_CONTEXT_RE = re.compile(
    r"(?i)(?:\btiền thân\b|\btrường\b|\btrường học\b|\bchủng viện\b|\bhọc viện\b|\bcollege\b|\bschool\b)"
)


def is_address_like_context(text: str) -> bool:
    if not text:
        return False
    return bool(ADDRESS_LIKE_CONTEXT_RE.search(clean_wiki_markup(text)))


def is_acceptable_year(year: int, context: str = "", source: str = "text") -> bool:
    if year > MAX_YEAR:
        return False
    if source == "wikidata":
        return HISTORICAL_MIN_YEAR <= year <= MAX_YEAR
    if year >= MIN_YEAR:
        return True
    if HISTORICAL_MIN_YEAR <= year < MIN_YEAR:
        return bool(HISTORICAL_YEAR_CONTEXT_RE.search(clean_wiki_markup(context)))
    return False


def extract_year(value: str, source: str = "text", context_hint: bool = False) -> str:
    cleaned = clean_wiki_markup(value or "")
    if not cleaned:
        return ""
    if source != "wikidata" and is_address_like_context(cleaned):
        return ""

    candidates = re.findall(r"\b(18\d{2}|19\d{2}|20\d{2})\b", cleaned)
    for year_text in candidates:
        year_number = int(year_text)
        if not is_acceptable_year(year_number, cleaned, source=source):
            continue
        if source == "wikidata" or context_hint:
            return year_text
    return ""


def extract_integer(value: str) -> str:
    if not value:
        return ""

    raw_match = re.search(r"\d[\d\s.,]*", value)
    if raw_match:
        digits = re.sub(r"\D", "", raw_match.group(0))
        if digits:
            return digits

    cleaned = clean_wiki_markup(value)
    clean_match = re.search(r"\d[\d\s.,]*", cleaned)
    if clean_match:
        digits = re.sub(r"\D", "", clean_match.group(0))
        if digits:
            return digits
    return ""


def extract_best_number_from_patterns(text: str, patterns: List[str], minimum: int = 10, maximum: int = 2_000_000) -> str:
    if not text:
        return ""

    cleaned_text = clean_wiki_markup(text)
    candidates: List[int] = []
    for pattern in patterns:
        for match in re.finditer(pattern, cleaned_text, flags=re.I):
            for group in match.groups():
                number_text = extract_integer(group or "")
                if not number_text:
                    continue
                try:
                    value = int(number_text)
                except ValueError:
                    continue
                if minimum <= value <= maximum:
                    candidates.append(value)
    return str(max(candidates)) if candidates else ""


def normalize_location_name(value: str) -> str:
    text = clean_wiki_markup(value)
    text = re.sub(r"\s+", " ", text).strip()
    if not text:
        return ""

    prefixes = [
        r"^thành\s*phố\s*trực\s*thuộc\s*trung\s*ương\s*",
        r"^thành\s*phố\s*",
        r"^tp\.\s*",
        r"^tp\s+",
        r"^tỉnh\s*",
    ]

    updated = True
    while updated:
        updated = False
        for pattern in prefixes:
            new_text = re.sub(pattern, "", text, flags=re.I).strip()
            if new_text != text:
                text = new_text
                updated = True

    text = re.sub(r"\s+", " ", text).strip(" ,")
    return "" if text.lower() in IGNORED_LOCATION_VALUES else text


def canonicalize_province_name(value: str) -> str:
    normalized = normalize_location_name(value)
    if not normalized:
        return ""
    lookup_key = normalized.lower()
    return PROVINCE_LOOKUP.get(lookup_key, normalized if lookup_key in PROVINCE_LOOKUP else "")


def canonicalize_city_name(value: str) -> str:
    normalized = normalize_location_name(value)
    if not normalized:
        return ""

    lookup_key = normalized.lower()
    if lookup_key in CENTRAL_MUNICIPALITIES:
        return CENTRAL_MUNICIPALITIES[lookup_key]
    if lookup_key in CITY_CANONICAL_NAMES:
        return CITY_CANONICAL_NAMES[lookup_key]

    raw = clean_wiki_markup(value).strip().lower()
    if raw.startswith("thành phố ") or raw.startswith("tp.") or raw.startswith("tp "):
        return CITY_CANONICAL_NAMES.get(raw, normalized)
    return ""


def is_low_level_location_part(value: str) -> bool:
    text = clean_wiki_markup(value).strip(" ,;.-")
    if not text:
        return False

    lowered = text.lower()
    if lowered in IGNORED_LOCATION_VALUES:
        return False
    if lowered.startswith(LOW_LEVEL_LOCATION_PREFIXES):
        return True
    if any(keyword in lowered for keyword in ADDRESS_KEYWORDS):
        return True
    if re.search(r"\d", lowered):
        return True
    if re.search(r"\b(q\.|p\.|tx\.|tt\.)\b", lowered):
        return True
    return False


def infer_province_from_city(city: str) -> str:
    canonical_city = canonicalize_city_name(city)
    if not canonical_city:
        return ""
    city_key = canonical_city.lower()
    return CENTRAL_MUNICIPALITIES.get(city_key, CITY_TO_PROVINCE.get(city_key, ""))


def parse_location_components(value: str) -> Tuple[str, str, str]:
    cleaned = clean_wiki_markup(value)
    if not cleaned:
        return "", "", ""

    raw_parts = [part.strip(" ,;.-") for part in re.split(r"[,;\n]+", cleaned) if part.strip(" ,;.-")]
    if not raw_parts:
        return cleaned, "", ""

    address_parts: List[str] = []
    city = ""
    province = ""

    for raw_part in reversed(raw_parts):
        cleaned_part = clean_wiki_markup(raw_part).strip(" ,;.-")
        if not cleaned_part:
            continue

        lowered_part = cleaned_part.lower()
        if lowered_part in IGNORED_LOCATION_VALUES:
            continue

        if is_low_level_location_part(cleaned_part):
            address_parts.insert(0, cleaned_part)
            continue

        province_candidate = canonicalize_province_name(cleaned_part)
        city_candidate = canonicalize_city_name(cleaned_part)

        if not city and city_candidate:
            city = city_candidate
            if not province:
                province = infer_province_from_city(city)
            continue

        if not province and province_candidate:
            province = province_candidate
            if not city and province_candidate.lower() in CENTRAL_MUNICIPALITIES:
                city = province_candidate
            continue

        if not province and cleaned_part.lower().startswith("tỉnh "):
            province = normalize_location_name(cleaned_part)
            continue

        if not province and not city:
            inferred_city, inferred_province = infer_location_from_free_text(cleaned_part)
            if inferred_city or inferred_province:
                city = city or inferred_city
                province = province or inferred_province
                continue

        address_parts.insert(0, cleaned_part)

    if city and not province:
        province = infer_province_from_city(city)

    if not city and not province and not address_parts:
        return cleaned, "", ""

    if not city and not province and address_parts:
        return ", ".join(address_parts).strip(" ,"), "", ""

    if not address_parts:
        address = ""
    else:
        address = ", ".join(address_parts).strip(" ,")

    return address, city, province


def split_infobox_site_segments(value: str) -> List[str]:
    raw = value or ""
    if not raw.strip():
        return []

    text = re.sub(r"<br\s*/?>", "\n", raw, flags=re.I)
    has_list_template = bool(re.search(r"\{\{(?:bulleted list|ubl)\|", text, flags=re.I))
    text = re.sub(r"\{\{(?:bulleted list|ubl)\|", "", text, flags=re.I)
    if has_list_template:
        text = text.replace("|", "\n")
    text = re.sub(r"\s*[•●▪◦]\s*", "\n", text)
    text = re.sub(r"(?i)(?=cơ\s*sở\s*(?:chính|\d+)\s*[:\-])", "\n", text)
    text = re.sub(r"(?i)(?=phân\s*hiệu\s*[:\-])", "\n", text)

    parts = [clean_wiki_markup(part).strip(" ,;:-") for part in re.split(r"\n+", text) if clean_wiki_markup(part).strip(" ,;:-")]
    normalized_parts: List[str] = []
    for part in parts:
        part = clean_wiki_markup(part).strip(" ,;:-")
        part = re.sub(r"[{}|]+", "", part).strip(" ,;:-")
        if len(part) < 3:
            continue
        normalized_parts.append(part)
    return normalized_parts


def extract_site_location_data(raw_value: str) -> Tuple[List[str], str, str]:
    segments = split_infobox_site_segments(raw_value)
    if not segments and raw_value:
        segments = [clean_wiki_markup(raw_value).strip(" ,;:-")]

    addresses: List[str] = []
    city = ""
    province = ""

    for segment in segments:
        address_value, city_value, province_value = parse_location_components(segment)
        if address_value:
            addresses.append(address_value)
        elif segment:
            addresses.append(segment)

        if not city and city_value:
            city = city_value
        if not province and province_value:
            province = province_value

    deduped_addresses: List[str] = []
    seen_addresses: Set[str] = set()
    for address in addresses:
        key = address.lower()
        if key in seen_addresses:
            continue
        seen_addresses.add(key)
        deduped_addresses.append(address)

    return deduped_addresses, city, province


def split_pipe_values(value: str) -> List[str]:
    if not value:
        return []
    return [item.strip() for item in value.split("|") if item.strip()]


def normalize_coordinate_pair(latitude: object, longitude: object) -> Tuple[str, str]:
    try:
        latitude_value = round(float(str(latitude).strip()), 6)
        longitude_value = round(float(str(longitude).strip()), 6)
    except (TypeError, ValueError):
        return "", ""

    if not (8.0 <= latitude_value <= 24.5 and 102.0 <= longitude_value <= 110.5):
        return "", ""
    return f"{latitude_value:.6f}", f"{longitude_value:.6f}"


def dedupe_coordinate_pairs(coordinates: List[Tuple[str, str]]) -> List[Tuple[str, str]]:
    result: List[Tuple[str, str]] = []
    seen: Set[Tuple[str, str]] = set()
    for latitude, longitude in coordinates:
        normalized_pair = normalize_coordinate_pair(latitude, longitude)
        if not normalized_pair or normalized_pair in seen:
            continue
        seen.add(normalized_pair)
        result.append(normalized_pair)
    return result


def get_city_center_coordinates(city: str) -> Tuple[str, str]:
    canonical_city = canonicalize_city_name(city)
    if not canonical_city:
        return "", ""
    coords = CITY_COORDS.get(canonical_city)
    if not coords:
        province = infer_province_from_city(canonical_city)
        if province:
            coords = PROVINCE_COORDS.get(province)
    if not coords:
        return "", ""
    return normalize_coordinate_pair(coords[0], coords[1])


def get_province_center_coordinates(province: str) -> Tuple[str, str]:
    canonical_province = canonicalize_province_name(province) or infer_province_from_city(province)
    if not canonical_province:
        return "", ""
    coords = PROVINCE_COORDS.get(canonical_province)
    if not coords:
        return "", ""
    return normalize_coordinate_pair(coords[0], coords[1])


def build_site_records(site_addresses: List[str], default_city: str, default_province: str) -> List[Dict[str, str]]:
    records: List[Dict[str, str]] = []
    normalized_city = canonicalize_city_name(default_city) or normalize_location_name(default_city)
    normalized_province = canonicalize_province_name(default_province) or infer_province_from_city(default_city) or normalize_location_name(default_province)

    source_addresses = site_addresses[:] if site_addresses else [normalized_city or normalized_province or "Việt Nam"]
    for raw_address in source_addresses:
        cleaned_address = clean_wiki_markup(raw_address).strip(" ,;:-")
        searchable_address = re.sub(r"^(?:cơ\s*sở\s*(?:chính|\d+)?|phân\s*hiệu)\s*[:\-]\s*", "", cleaned_address, flags=re.I)
        address_value, city_value, province_value = parse_location_components(searchable_address)
        if not city_value and not province_value:
            inferred_city, inferred_province = infer_location_from_free_text(searchable_address)
            city_value = city_value or inferred_city
            province_value = province_value or inferred_province
        site_city = canonicalize_city_name(city_value) or normalized_city
        site_province = canonicalize_province_name(province_value) or normalized_province
        if not site_province and site_city:
            site_province = infer_province_from_city(site_city)

        final_address = address_value or cleaned_address or site_city or site_province or "Việt Nam"
        records.append(
            {
                "address": final_address,
                "city": site_city,
                "province": site_province,
            }
        )
    return records


def resolve_site_coordinates(
    site_addresses: List[str],
    default_city: str,
    default_province: str,
    exact_coordinates: List[Tuple[str, str]],
) -> Tuple[List[str], str, str, List[str], List[str], List[str]]:
    site_records = build_site_records(site_addresses, default_city, default_province)
    exact_pairs = dedupe_coordinate_pairs(exact_coordinates)

    resolved_addresses: List[str] = []
    latitude_values: List[str] = []
    longitude_values: List[str] = []
    coord_levels: List[str] = []
    resolved_city = ""
    resolved_province = ""

    for index, site in enumerate(site_records):
        site_city = site.get("city", "")
        site_province = site.get("province", "")
        coordinate_pair = exact_pairs[index] if index < len(exact_pairs) else ("", "")
        level = "Exact"

        if not coordinate_pair[0] or not coordinate_pair[1]:
            coordinate_pair = get_city_center_coordinates(site_city)
            level = "Approximate"
        if not coordinate_pair[0] or not coordinate_pair[1]:
            coordinate_pair = get_province_center_coordinates(site_province)
            level = "Approximate"
        if not coordinate_pair[0] or not coordinate_pair[1]:
            coordinate_pair = get_city_center_coordinates(default_city)
            level = "Approximate"
        if not coordinate_pair[0] or not coordinate_pair[1]:
            coordinate_pair = get_province_center_coordinates(default_province)
            level = "Approximate"
        if not coordinate_pair[0] or not coordinate_pair[1]:
            coordinate_pair = normalize_coordinate_pair(DEFAULT_VIETNAM_COORDS[0], DEFAULT_VIETNAM_COORDS[1])
            level = "Approximate"

        resolved_addresses.append(site.get("address", "") or site_city or site_province or "Việt Nam")
        latitude_values.append(coordinate_pair[0])
        longitude_values.append(coordinate_pair[1])
        coord_levels.append(level)

        if not resolved_city and site_city:
            resolved_city = site_city
        if not resolved_province and site_province:
            resolved_province = site_province

    if not resolved_province and resolved_city:
        resolved_province = infer_province_from_city(resolved_city)

    return resolved_addresses, resolved_city or normalize_location_name(default_city), resolved_province or normalize_location_name(default_province), latitude_values, longitude_values, coord_levels


def dms_to_decimal(degrees: float, minutes: float, seconds: float, direction: str) -> float:
    decimal = abs(degrees) + minutes / 60 + seconds / 3600
    if direction.upper() in {"S", "W"}:
        decimal *= -1
    return round(decimal, 6)


def extract_coordinates_from_template(value: str) -> Tuple[str, str]:
    if not value:
        return "", ""

    decimal_match = re.search(
        r"coord\|\s*([+-]?\d+(?:\.\d+)?)\|\s*([+-]?\d+(?:\.\d+)?)",
        value,
        flags=re.I,
    )
    if decimal_match:
        return normalize_coordinate_pair(decimal_match.group(1), decimal_match.group(2))

    dms_match = re.search(
        r"coord\|\s*(\d+(?:\.\d+)?)\|\s*(\d+(?:\.\d+)?)?\|\s*(\d+(?:\.\d+)?)?\|\s*([NS])\|\s*(\d+(?:\.\d+)?)\|\s*(\d+(?:\.\d+)?)?\|\s*(\d+(?:\.\d+)?)?\|\s*([EW])",
        value,
        flags=re.I,
    )
    if dms_match:
        latitude = dms_to_decimal(
            float(dms_match.group(1)),
            float(dms_match.group(2) or 0),
            float(dms_match.group(3) or 0),
            dms_match.group(4),
        )
        longitude = dms_to_decimal(
            float(dms_match.group(5)),
            float(dms_match.group(6) or 0),
            float(dms_match.group(7) or 0),
            dms_match.group(8),
        )
        return normalize_coordinate_pair(latitude, longitude)

    return "", ""


def extract_all_coordinates_from_text(value: str) -> List[Tuple[str, str]]:
    if not value:
        return []

    coordinates: List[Tuple[str, str]] = []
    seen: Set[Tuple[str, str]] = set()

    template_matches = re.findall(r"\{\{\s*coord\|.*?\}\}", value, flags=re.I | re.S)
    for template_value in template_matches:
        latitude, longitude = extract_coordinates_from_template(template_value)
        if latitude and longitude and (latitude, longitude) not in seen:
            seen.add((latitude, longitude))
            coordinates.append((latitude, longitude))

    text = re.sub(r"<br\s*/?>", "\n", value, flags=re.I)
    text = re.sub(r"\s*[•●▪◦]\s*", "\n", text)
    for segment in re.split(r"\n+|;", text):
        cleaned_segment = clean_wiki_markup(segment)
        decimal_match = re.search(r"([+-]?\d+(?:\.\d+)?)\s*[,/]\s*([+-]?\d+(?:\.\d+)?)", cleaned_segment)
        if decimal_match:
            pair = normalize_coordinate_pair(decimal_match.group(1), decimal_match.group(2))
            if pair not in seen:
                if pair[0] and pair[1]:
                    seen.add(pair)
                    coordinates.append(pair)

        dms_match = re.search(
            r"(\d{1,2})\s*[°º]\s*(\d{1,2})?\s*['′]?(\d{1,2}(?:\.\d+)?)?\s*[\"″]?\s*([NS])[^\d]{0,20}(\d{1,3})\s*[°º]\s*(\d{1,2})?\s*['′]?(\d{1,2}(?:\.\d+)?)?\s*[\"″]?\s*([EW])",
            cleaned_segment,
            flags=re.I,
        )
        if dms_match:
            latitude = str(
                dms_to_decimal(
                    float(dms_match.group(1)),
                    float(dms_match.group(2) or 0),
                    float(dms_match.group(3) or 0),
                    dms_match.group(4),
                )
            )
            longitude = str(
                dms_to_decimal(
                    float(dms_match.group(5)),
                    float(dms_match.group(6) or 0),
                    float(dms_match.group(7) or 0),
                    dms_match.group(8),
                )
            )
            pair = normalize_coordinate_pair(latitude, longitude)
            if pair not in seen:
                if pair[0] and pair[1]:
                    seen.add(pair)
                    coordinates.append(pair)
    return dedupe_coordinate_pairs(coordinates)


def extract_coordinates_from_text_context(wikitext: str) -> List[Tuple[str, str]]:
    sample = (wikitext or "")[:8000]
    return dedupe_coordinate_pairs(extract_all_coordinates_from_text(sample))


def extract_balanced_template(wikitext: str, template_names: List[str]) -> str:
    if not wikitext:
        return ""

    template_regex = "|".join(re.escape(name) for name in template_names)
    start_match = re.search(r"\{\{\s*(?:" + template_regex + r")\b", wikitext, flags=re.I)
    if not start_match:
        return ""

    start = start_match.start()
    index = start
    depth = 0
    length = len(wikitext)

    while index < length - 1:
        token = wikitext[index:index + 2]
        if token == "{{":
            depth += 1
            index += 2
            continue
        if token == "}}":
            depth -= 1
            index += 2
            if depth == 0:
                return wikitext[start:index]
            continue
        index += 1

    return ""


def extract_supported_infobox(wikitext: str) -> str:
    return extract_balanced_template(wikitext, INFOBOX_TEMPLATES)


def extract_infobox_fields(infobox_text: str) -> Dict[str, str]:
    if not infobox_text:
        return {}

    fields: Dict[str, str] = {}
    current_key: Optional[str] = None
    current_lines: List[str] = []

    for line in infobox_text.splitlines()[1:]:
        stripped = line.strip()
        if stripped.startswith("}}"):
            if current_key is not None:
                fields[current_key] = "\n".join(current_lines).strip()
            break
        if stripped.startswith("|"):
            if current_key is not None:
                fields[current_key] = "\n".join(current_lines).strip()
            match = re.match(r"^\|\s*([^=]+?)\s*=\s*(.*)$", line)
            if match:
                current_key = match.group(1).strip().lower().replace(" ", "_")
                current_lines = [match.group(2).rstrip()]
            else:
                current_key = None
                current_lines = []
        elif current_key is not None:
            current_lines.append(line.rstrip())

    if current_key is not None and current_key not in fields:
        fields[current_key] = "\n".join(current_lines).strip()

    return fields


def pick_field(fields: Dict[str, str], candidates: List[str]) -> str:
    for candidate in candidates:
        key = candidate.lower().replace(" ", "_")
        if key in fields and fields[key].strip():
            return fields[key].strip()
    return ""


def extract_coordinates_from_infobox_fields(fields: Dict[str, str]) -> Tuple[str, str]:
    coord_value = pick_field(fields, ["coordinates", "coord", "tọa_độ", "toa_do"])
    latitude, longitude = extract_coordinates_from_template(coord_value)
    if latitude and longitude:
        return latitude, longitude

    lat_d = pick_field(fields, ["lat_d", "latitude", "vĩ_độ", "vi_do"])
    lat_m = pick_field(fields, ["lat_m"])
    lat_s = pick_field(fields, ["lat_s"])
    lat_ns = pick_field(fields, ["lat_ns", "lat_direction", "vĩ_hướng", "vi_huong"]) or "N"
    lon_d = pick_field(fields, ["long_d", "longitude", "kinh_độ", "kinh_do"])
    lon_m = pick_field(fields, ["long_m"])
    lon_s = pick_field(fields, ["long_s"])
    lon_ew = pick_field(fields, ["long_ew", "long_direction", "kinh_hướng", "kinh_huong"]) or "E"

    if lat_d and lon_d:
        latitude = dms_to_decimal(float(lat_d), float(lat_m or 0), float(lat_s or 0), lat_ns)
        longitude = dms_to_decimal(float(lon_d), float(lon_m or 0), float(lon_s or 0), lon_ew)
        return normalize_coordinate_pair(latitude, longitude)

    return "", ""


def extract_site_coordinates_from_infobox_fields(fields: Dict[str, str]) -> List[Tuple[str, str]]:
    coord_values = [
        pick_field(fields, ["coordinates", "coord", "tọa_độ", "toa_do"]),
        pick_field(fields, ["vị_trí", "vi_tri", "location", "headquarters", "địa_điểm", "địa điểm", "dia_diem"]),
    ]

    coordinates: List[Tuple[str, str]] = []
    seen: Set[Tuple[str, str]] = set()
    for coord_value in coord_values:
        for latitude, longitude in extract_all_coordinates_from_text(coord_value):
            if (latitude, longitude) in seen:
                continue
            seen.add((latitude, longitude))
            coordinates.append((latitude, longitude))

    if coordinates:
        return dedupe_coordinate_pairs(coordinates)

    latitude, longitude = extract_coordinates_from_infobox_fields(fields)
    if latitude and longitude:
        return [(latitude, longitude)]
    return []


def extract_year_from_text_context(wikitext: str) -> str:
    sample = clean_wiki_markup((wikitext or "")[:3000])
    patterns = [
        re.compile(r"(?i)\bthành\s+lập(?:\s+vào|\s+từ|\s+ngày)?\s*(?:năm\s*)?(18\d{2}|19\d{2}|20\d{2})\b"),
        re.compile(r"(?i)\bnăm\s+thành\s+lập\s*(?:là\s*)?(18\d{2}|19\d{2}|20\d{2})\b"),
        re.compile(r"(?i)\bđược\s+thành\s+lập(?:\s+vào|\s+từ|\s+ngày)?\s*(?:năm\s*)?(18\d{2}|19\d{2}|20\d{2})\b"),
        re.compile(r"(?i)\b(?:founded|established)\s*(?:in\s*)?(18\d{2}|19\d{2}|20\d{2})\b"),
        re.compile(r"(?i)\bquyết\s+định\s+số[^\n.]{0,80}?\bnăm\s*(18\d{2}|19\d{2}|20\d{2})\b"),
    ]
    for pattern in patterns:
        match = pattern.search(sample)
        if not match:
            continue
        snippet = sample[max(0, match.start() - 40):min(len(sample), match.end() + 40)]
        if is_address_like_context(snippet):
            continue
        year_number = int(match.group(1))
        if is_acceptable_year(year_number, snippet, source="text"):
            return str(year_number)

    historical_patterns = [
        re.compile(r"(?i)\btiền\s+thân\b[^\n.]{0,80}?\b(18\d{2}|19\d{2})\b"),
        re.compile(r"(?i)\btrường\b[^\n.]{0,80}?\b(18\d{2}|19\d{2})\b"),
    ]
    for pattern in historical_patterns:
        match = pattern.search(sample)
        if not match:
            continue
        snippet = sample[max(0, match.start() - 40):min(len(sample), match.end() + 40)]
        if is_address_like_context(snippet):
            continue
        year_number = int(match.group(1))
        if is_acceptable_year(year_number, snippet, source="text"):
            return str(year_number)
    return ""


def extract_staff_from_text(wikitext: str) -> str:
    sample = (wikitext or "")[:5000]
    patterns = [
        r"đội\s*ngũ[^\n]{0,80}?([\d\s.,]+)[^\n]{0,40}?giảng\s*viên",
        r"([\d\s.,]+)[^\n]{0,40}?giảng\s*viên",
        r"giảng\s*viên[^\n]{0,40}?([\d\s.,]+)",
        r"([\d\s.,]+)[^\n]{0,40}?(?:cán\s*bộ\s*,?\s*giảng\s*viên|cán\s*bộ\s*giảng\s*viên|giảng\s*viên\s*cơ\s*hữu)",
        r"(?:có|gồm|hiện\s*có)[^\n]{0,40}?([\d\s.,]+)[^\n]{0,40}?(?:giảng\s*viên|nhà\s*giáo)",
    ]
    return extract_best_number_from_patterns(sample, patterns, minimum=20, maximum=100_000)


def extract_student_count_from_text(wikitext: str) -> str:
    sample = (wikitext or "")[:7000]
    patterns = [
        r"(?:hơn|gần|khoảng|trên|xấp\s*xỉ)?\s*([\d\s.,]+)\s*(?:sinh\s*viên|học\s*viên)",
        r"(?:quy\s*mô\s*đào\s*tạo|quy\s*mô)[^\n]{0,50}?([\d\s.,]+)\s*(?:sinh\s*viên|học\s*viên|người\s*học)?",
        r"(?:có|gồm|hiện\s*có|đào\s*tạo)[^\n]{0,30}?(?:hơn|gần|khoảng|trên)?\s*([\d\s.,]+)[^\n]{0,15}?(?:sinh\s*viên|học\s*viên)",
        r"(?:sinh\s*viên|học\s*viên|người\s*học)[^\n]{0,20}?([\d\s.,]+)",
    ]
    return extract_best_number_from_patterns(sample, patterns, minimum=50, maximum=2_000_000)


def extract_qid_from_page_data(page_data: Dict) -> str:
    return page_data.get("pageprops", {}).get("wikibase_item", "")


def extract_claim_entity_id(claim: Dict) -> str:
    value = claim.get("mainsnak", {}).get("datavalue", {}).get("value")
    if isinstance(value, dict):
        return value.get("id", "")
    return ""


def extract_claim_numeric(claim: Dict) -> str:
    value = claim.get("mainsnak", {}).get("datavalue", {}).get("value")
    if isinstance(value, dict) and "amount" in value:
        return extract_integer(str(value.get("amount", "")))
    if isinstance(value, str):
        return extract_integer(value)
    return ""


def extract_claim_year(claim: Dict) -> str:
    value = claim.get("mainsnak", {}).get("datavalue", {}).get("value")
    if isinstance(value, dict):
        time_value = str(value.get("time", ""))
        return extract_year(time_value, source="wikidata")
    return ""


def extract_claim_coordinate(claim: Dict) -> Tuple[str, str]:
    value = claim.get("mainsnak", {}).get("datavalue", {}).get("value")
    if isinstance(value, dict):
        latitude = value.get("latitude")
        longitude = value.get("longitude")
        if latitude is not None and longitude is not None:
            return normalize_coordinate_pair(latitude, longitude)
    return "", ""


def get_entity_label(session: requests.Session, entity_id: str, label_cache: Dict[str, str]) -> str:
    if not entity_id:
        return ""
    if entity_id in label_cache:
        return label_cache[entity_id]

    params = {
        "action": "wbgetentities",
        "format": "json",
        "ids": entity_id,
        "props": "labels",
        "languages": "vi|en",
    }
    data = request_json(session, WIKIDATA_API_URL, params)
    entity = data.get("entities", {}).get(entity_id, {})
    labels = entity.get("labels", {})
    label = labels.get("vi", {}).get("value") or labels.get("en", {}).get("value") or ""
    label_cache[entity_id] = label
    return label


def classify_location_label(label: str) -> Tuple[str, str]:
    city = canonicalize_city_name(label)
    province = canonicalize_province_name(label)

    if city:
        return city, province or infer_province_from_city(city)
    if province:
        if province.lower() in CENTRAL_MUNICIPALITIES:
            return province, province
        return "", province
    return "", ""


def get_wikidata_enrichment(
    session: requests.Session,
    qid: str,
    enrichment_cache: Dict[str, Dict[str, str]],
    label_cache: Dict[str, str],
) -> Dict[str, str]:
    empty = {
        "wikidata_id": qid or "",
        "foundingYearOrg": "",
        "city": "",
        "province": "",
        "address": "",
        "head_name": "",
        "head_title": "",
        "governing_body": "",
        "academicStaffSize": "",
        "numberOfStudents": "",
        "latitude": "",
        "longitude": "",
        "coord_level": "",
    }
    if not qid:
        return empty
    if qid in enrichment_cache:
        return enrichment_cache[qid]

    params = {
        "action": "wbgetentities",
        "format": "json",
        "ids": qid,
        "props": "claims",
    }

    try:
        data = request_json(session, WIKIDATA_API_URL, params)
    except Exception:
        enrichment_cache[qid] = empty
        return empty

    claims = data.get("entities", {}).get(qid, {}).get("claims", {})
    result = dict(empty)

    for claim in claims.get("P571", []):
        year_value = extract_claim_year(claim)
        if year_value:
            result["foundingYearOrg"] = year_value
            break

    for prop in ["P2196", "P1971"]:
        for claim in claims.get(prop, []):
            number = extract_claim_numeric(claim)
            if number:
                result["numberOfStudents"] = number
                break
        if result["numberOfStudents"]:
            break

    for claim in claims.get("P1128", []):
        number = extract_claim_numeric(claim)
        if number:
            result["academicStaffSize"] = number
            break

    for prop, default_title in WIKIDATA_HEAD_PROPERTIES:
        for claim in claims.get(prop, []):
            entity_id = extract_claim_entity_id(claim)
            person_label = get_entity_label(session, entity_id, label_cache)
            person_name = clean_person_name(person_label)
            if person_name:
                result["head_name"] = person_name
                result["head_title"] = default_title
                break
        if result["head_name"]:
            break

    for prop in WIKIDATA_GOVERNING_BODY_PROPERTIES:
        for claim in claims.get(prop, []):
            entity_id = extract_claim_entity_id(claim)
            organization_label = get_entity_label(session, entity_id, label_cache)
            organization_name = normalize_governing_body_name(organization_label)
            if organization_name:
                result["governing_body"] = organization_name
                break
        if result["governing_body"]:
            break

    for claim in claims.get("P625", []):
        latitude, longitude = extract_claim_coordinate(claim)
        if latitude and longitude:
            result["latitude"] = latitude
            result["longitude"] = longitude
            result["coord_level"] = "Exact"
            break

    for prop in ["P159", "P131"]:
        for claim in claims.get(prop, []):
            entity_id = extract_claim_entity_id(claim)
            label = get_entity_label(session, entity_id, label_cache)
            city_value, province_value = classify_location_label(label)
            if not result["city"] and city_value:
                result["city"] = city_value
            if not result["province"] and province_value:
                result["province"] = province_value
            if result["city"] or result["province"]:
                break

    enrichment_cache[qid] = result
    return result


def is_relevant_title(title: str) -> bool:
    if not any(keyword.lower() in title.lower() for keyword in INCLUDE_TITLE_KEYWORDS):
        return False
    for pattern in EXCLUDE_TITLE_PATTERNS:
        if re.search(pattern, title, flags=re.I):
            return False
    return True


def is_small_faculty_title(title: str) -> bool:
    return any(re.search(pattern, title.strip(), flags=re.I) for pattern in SMALL_FACULTY_PATTERNS)


def detect_member_parent_org(page_title: str, wikitext: str = "") -> str:
    comma_match = re.search(
        r"^\s*(?:Trường|Học viện|Đại học|Cao đẳng)[^,]+,\s*(.+?)\s*$",
        page_title,
        flags=re.I,
    )
    if comma_match:
        return clean_wiki_markup(comma_match.group(1)).strip(" ,")

    direct_match = re.search(r"trực\s*thuộc\s+(.+)$", page_title, flags=re.I)
    if direct_match:
        return clean_wiki_markup(direct_match.group(1)).strip(" ,")

    branch_match = re.match(r"^Phân hiệu\s+(Đại học[^,]+?)\s+tại\s+", clean_wiki_markup(page_title), flags=re.I)
    if branch_match:
        return clean_wiki_markup(branch_match.group(1)).strip(" ,")

    sample = wikitext[:1200]
    text_match = re.search(r"trực\s*thuộc\s+(Đại học[^\n,.]+)", sample, flags=re.I)
    if text_match:
        return clean_wiki_markup(text_match.group(1)).strip(" ,")

    return ""


def normalize_member_unit_name(page_title: str, parsed_name: str, parent_org: str) -> str:
    candidate = clean_wiki_markup(page_title or parsed_name)
    if "," in candidate:
        candidate = candidate.split(",", 1)[0].strip()
    if "trực thuộc" in candidate.lower():
        candidate = re.split(r"trực\s*thuộc", candidate, flags=re.I)[0].strip(" ,.-")
    return clean_wiki_markup(candidate)


def get_category_members_recursive(
    session: requests.Session,
    category_title: str,
    processed_categories: Optional[Set[str]] = None,
    depth: int = 0,
    max_depth: int = 4,
) -> List[str]:
    if processed_categories is None:
        processed_categories = set()
    if depth > max_depth or category_title in processed_categories:
        return []

    processed_categories.add(category_title)
    titles: List[str] = []
    continuation: Optional[str] = None

    while True:
        params: Dict[str, object] = {
            "action": "query",
            "format": "json",
            "list": "categorymembers",
            "cmtitle": category_title,
            "cmlimit": "500",
        }
        if continuation:
            params["cmcontinue"] = continuation

        data = request_json(session, API_URL, params)
        members = data.get("query", {}).get("categorymembers", [])

        for item in members:
            namespace = item.get("ns")
            title = item.get("title", "")
            if namespace == 0 and title:
                titles.append(title)
            elif namespace == 14 and title:
                titles.extend(
                    get_category_members_recursive(
                        session,
                        title,
                        processed_categories,
                        depth + 1,
                        max_depth,
                    )
                )

        continuation = data.get("continue", {}).get("cmcontinue")
        if not continuation:
            break

    return list(dict.fromkeys(titles))


def resolve_category_with_members(
    session: requests.Session,
    category_title: str,
    max_depth: int,
) -> Tuple[str, List[str]]:
    candidates = [category_title] + [item for item in FALLBACK_CATEGORIES if item != category_title]
    for candidate in candidates:
        titles = get_category_members_recursive(session, candidate, max_depth=max_depth)
        if titles:
            return candidate, titles
    return category_title, []


def get_page_wikitext_and_qid(session: requests.Session, title: str) -> Tuple[str, str]:
    params: Dict[str, object] = {
        "action": "query",
        "format": "json",
        "prop": "revisions|pageprops",
        "rvslots": "main",
        "rvprop": "content",
        "titles": title,
        "redirects": 1,
        "ppprop": "wikibase_item",
    }
    data = request_json(session, API_URL, params)
    pages = data.get("query", {}).get("pages", {})
    for page_data in pages.values():
        qid = extract_qid_from_page_data(page_data)
        revisions = page_data.get("revisions", [])
        if revisions:
            wikitext = revisions[0].get("slots", {}).get("main", {}).get("*", "")
            return wikitext, qid
    return "", ""


def parse_university_page(title: str, wikitext: str) -> Dict[str, str]:
    infobox = extract_supported_infobox(wikitext)
    fields = extract_infobox_fields(infobox)

    raw_name = clean_wiki_markup(pick_field(fields, ["name", "tên", "ten"])) or clean_wiki_markup(title)
    head_name, head_title = extract_head_from_infobox(fields)
    if not head_name:
        head_name, head_title = extract_head_from_text_context(wikitext)
    infobox_governing_body = extract_managing_org_from_infobox(fields) or extract_managing_org_from_text(wikitext)

    founding_year = extract_year(
        pick_field(
            fields,
            [
                "established",
                "founded",
                "formation",
                "thành_lập",
                "năm_thành_lập",
                "nam_thanh_lap",
                "thành_lập_năm",
            ],
        ),
        context_hint=True,
    )
    if not founding_year:
        founding_year = extract_year_from_text_context(wikitext)

    location_value = pick_field(
        fields,
        [
            "city",
            "location",
            "headquarters",
            "thành_phố",
            "thanh_pho",
            "địa_điểm",
            "địa điểm",
            "dia_diem",
            "vị_trí",
            "vi_tri",
            "trụ_sở",
            "tru_so",
            "địa_chỉ",
            "dia_chi",
            "văn_phòng",
            "van_phong",
        ],
    )
    site_addresses, city, inferred_province = extract_site_location_data(location_value)
    explicit_province = normalize_location_name(
        pick_field(fields, ["province", "tỉnh", "tinh", "tỉnh_thành", "tinh_thanh", "state"])
    )
    if not site_addresses and not city and not explicit_province and not inferred_province:
        inferred_address, city, inferred_province = infer_location_from_university_name(raw_name, title)
        if inferred_address:
            site_addresses = [inferred_address]
    exact_site_coordinates = dedupe_coordinate_pairs(
        extract_site_coordinates_from_infobox_fields(fields) + extract_coordinates_from_text_context(wikitext)
    )
    resolved_addresses, resolved_city, resolved_province, latitude_values, longitude_values, coord_levels = resolve_site_coordinates(
        site_addresses,
        city,
        explicit_province or inferred_province,
        exact_site_coordinates,
    )

    academic_staff_size = extract_integer(
        pick_field(
            fields,
            [
                "academic_staff",
                "faculty",
                "giảng_viên",
                "số_giảng_viên",
                "so_giang_vien",
                "nhân_viên",
                "staff",
                "teaching_staff",
                "giảng_viên_cơ_hữu",
                "giang_vien_co_huu",
                "cán_bộ_giảng_viên",
                "can_bo_giang_vien",
            ],
        )
    ) or extract_staff_from_text(wikitext)

    number_of_students = extract_integer(
        pick_field(
            fields,
            [
                "students",
                "student",
                "sinh_viên",
                "số_sinh_viên",
                "so_sinh_vien",
                "quy_mô",
                "quy_mo",
                "quy_mo_sinh_vien",
                "student_body",
                "student_enrolment",
                "student_enrollment",
                "undergraduates",
                "postgraduates",
            ],
        )
    ) or extract_student_count_from_text(wikitext)

    return {
        "name": raw_name,
        "page": clean_wiki_markup(title),
        "wikidata_id": "",
        "foundingYearOrg": founding_year,
        "address": " | ".join(resolved_addresses),
        "city": resolved_city,
        "province": resolved_province,
        "is_member_of": "",
        "head_name": head_name,
        "head_title": head_title,
        "governing_body": get_final_governing_body(raw_name, title, "", infobox_governing_body),
        "academicStaffSize": academic_staff_size,
        "numberOfStudents": number_of_students,
        "latitude": " | ".join(latitude_values),
        "longitude": " | ".join(longitude_values),
        "coord_level": " | ".join(coord_levels),
    }


def enrich_row_with_fallbacks(row: Dict[str, str], wikitext: str, wikidata_data: Dict[str, str]) -> Dict[str, str]:
    combined_location = ", ".join(
        part for part in [row.get("address", ""), row.get("city", ""), row.get("province", "")] if part
    )
    address, city, province = parse_location_components(combined_location)

    row["address"] = row.get("address", "") or address
    row["city"] = normalize_location_name(row.get("city", "") or city)
    row["province"] = normalize_location_name(row.get("province", "") or province)

    if not row.get("city") and not row.get("province"):
        inferred_address, inferred_city, inferred_province = infer_location_from_university_name(
            row.get("name", ""), row.get("page", "")
        )
        if not row.get("address") and inferred_address:
            row["address"] = inferred_address
        if inferred_city and not row.get("city"):
            row["city"] = inferred_city
        if inferred_province and not row.get("province"):
            row["province"] = inferred_province

    if not row.get("city") and not row.get("province"):
        text_address, text_city, text_province = extract_location_from_text_context(wikitext)
        if not row.get("address") and text_address:
            row["address"] = text_address
        if text_city and not row.get("city"):
            row["city"] = text_city
        if text_province and not row.get("province"):
            row["province"] = text_province

    if not row.get("address") and (row.get("city") or row.get("province")):
        row["address"] = row.get("city") or row.get("province")

    if row.get("city", "").lower().startswith(LOW_LEVEL_LOCATION_PREFIXES):
        row["address"] = ", ".join(part for part in [row.get("address", ""), row.get("city", "")] if part).strip(", ")
        row["city"] = ""

    if not row.get("province") and row.get("city"):
        row["province"] = infer_province_from_city(row["city"])

    if not row.get("academicStaffSize"):
        row["academicStaffSize"] = extract_staff_from_text(wikitext)
    if not row.get("numberOfStudents"):
        row["numberOfStudents"] = extract_student_count_from_text(wikitext)
    if not row.get("head_name"):
        text_head_name, text_head_title = extract_head_from_text_context(wikitext)
        if text_head_name:
            row["head_name"] = text_head_name
        if text_head_title and not row.get("head_title"):
            row["head_title"] = text_head_title

    if wikidata_data.get("foundingYearOrg"):
        row["foundingYearOrg"] = wikidata_data["foundingYearOrg"]
    if not row.get("head_name") and wikidata_data.get("head_name"):
        row["head_name"] = wikidata_data["head_name"]
    if not row.get("head_title") and wikidata_data.get("head_title"):
        row["head_title"] = wikidata_data["head_title"]
    row["governing_body"] = get_final_governing_body(
        row.get("name", ""),
        row.get("page", ""),
        wikidata_data.get("governing_body", ""),
        row.get("governing_body", "") or extract_managing_org_from_text(wikitext),
    )
    if not row.get("numberOfStudents") and wikidata_data.get("numberOfStudents"):
        row["numberOfStudents"] = wikidata_data["numberOfStudents"]
    if not row.get("academicStaffSize") and wikidata_data.get("academicStaffSize"):
        row["academicStaffSize"] = wikidata_data["academicStaffSize"]
    if not row.get("address") and wikidata_data.get("address"):
        row["address"] = clean_wiki_markup(wikidata_data["address"])
    if not row.get("latitude") and wikidata_data.get("latitude"):
        row["latitude"] = wikidata_data["latitude"]
    if not row.get("longitude") and wikidata_data.get("longitude"):
        row["longitude"] = wikidata_data["longitude"]
    if not row.get("coord_level") and wikidata_data.get("coord_level"):
        row["coord_level"] = wikidata_data["coord_level"]
    if not row.get("city") and wikidata_data.get("city"):
        row["city"] = normalize_location_name(wikidata_data["city"])
    if not row.get("province") and wikidata_data.get("province"):
        row["province"] = normalize_location_name(wikidata_data["province"])

    if not row.get("province") and row.get("city"):
        row["province"] = infer_province_from_city(row["city"])

    exact_coordinates = extract_coordinates_from_text_context(wikitext)
    wikidata_pair = normalize_coordinate_pair(wikidata_data.get("latitude", ""), wikidata_data.get("longitude", ""))
    if wikidata_pair[0] and wikidata_pair[1]:
        exact_coordinates.append(wikidata_pair)
    resolved_addresses, resolved_city, resolved_province, latitude_values, longitude_values, coord_levels = resolve_site_coordinates(
        split_pipe_values(row.get("address", "")),
        row.get("city", ""),
        row.get("province", ""),
        exact_coordinates,
    )
    row["address"] = " | ".join(resolved_addresses)
    row["city"] = resolved_city
    row["province"] = resolved_province
    row["latitude"] = " | ".join(latitude_values)
    row["longitude"] = " | ".join(longitude_values)
    row["coord_level"] = " | ".join(coord_levels)

    row["city"] = normalize_location_name(row.get("city", ""))
    row["province"] = normalize_location_name(row.get("province", ""))
    row["address"] = clean_wiki_markup(row.get("address", ""))
    row["name"] = clean_wiki_markup(row.get("name", ""))
    row["is_member_of"] = clean_wiki_markup(row.get("is_member_of", ""))
    row["head_name"] = clean_person_name(row.get("head_name", ""))
    row["head_title"] = clean_wiki_markup(row.get("head_title", ""))
    row["governing_body"] = normalize_governing_body_name(row.get("governing_body", ""))
    row["coord_level"] = clean_wiki_markup(row.get("coord_level", ""))
    return row


def should_skip_page(title: str, has_infobox: bool) -> bool:
    return (not has_infobox) and is_small_faculty_title(title)


def run(
    output_file: str,
    category_title: str,
    limit: Optional[int] = None,
    sleep_sec: float = 0.05,
    max_depth: int = 4,
) -> None:
    wiki_session = create_session()
    wikidata_session = create_session()

    print(f"Đang quét danh mục: {category_title}...")
    selected_category, titles = resolve_category_with_members(wiki_session, category_title, max_depth=max_depth)
    if selected_category != category_title:
        print(f"Danh mục gốc rỗng, chuyển sang: {selected_category}")

    filtered_titles = [title for title in titles if is_relevant_title(title)]
    if limit:
        filtered_titles = filtered_titles[:limit]

    print(f"Tìm thấy {len(titles)} bài, sau lọc còn {len(filtered_titles)} bài.")

    rows: List[Dict[str, str]] = []
    skipped_small_faculty = 0
    retained_without_infobox = 0
    wikidata_fill_count = 0
    wikidata_cache: Dict[str, Dict[str, str]] = {}
    label_cache: Dict[str, str] = {}

    for index, title in enumerate(filtered_titles, start=1):
        try:
            wikitext, qid = get_page_wikitext_and_qid(wiki_session, title)
            has_infobox = bool(extract_supported_infobox(wikitext))
            if should_skip_page(title, has_infobox):
                skipped_small_faculty += 1
                continue
            if not has_infobox:
                retained_without_infobox += 1

            row = parse_university_page(title, wikitext)
            parent_org = detect_member_parent_org(title, wikitext)
            row["name"] = normalize_member_unit_name(title, row.get("name", ""), parent_org)
            row["is_member_of"] = parent_org
            row["wikidata_id"] = qid

            before = {
                field: row.get(field, "")
                for field in ["foundingYearOrg", "address", "city", "province", "head_name", "head_title", "governing_body", "academicStaffSize", "numberOfStudents", "latitude", "longitude", "coord_level"]
            }
            wikidata_data = get_wikidata_enrichment(wikidata_session, qid, wikidata_cache, label_cache)
            row = enrich_row_with_fallbacks(row, wikitext, wikidata_data)
            for field in before:
                if not before[field] and row.get(field) and wikidata_data.get(field):
                    wikidata_fill_count += 1

            rows.append(row)
            if index % 10 == 0 or index == len(filtered_titles):
                print(f"Đã xử lý {index}/{len(filtered_titles)}")
            if sleep_sec > 0:
                time.sleep(sleep_sec)
        except Exception as exc:
            print(f"Lỗi tại {title}: {exc}")

    headers = [
        "name",
        "page",
        "wikidata_id",
        "foundingYearOrg",
        "address",
        "city",
        "province",
        "is_member_of",
        "head_name",
        "head_title",
        "governing_body",
        "academicStaffSize",
        "numberOfStudents",
        "latitude",
        "longitude",
        "coord_level",
    ]
    with open(output_file, "w", encoding="utf-8-sig", newline="") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Đã bỏ qua {skipped_small_faculty} bài khoa/bộ môn nhỏ không có infobox chuẩn.")
    print(f"Giữ lại {retained_without_infobox} bài không có infobox nhờ fallback từ văn bản/Wikidata.")
    print(f"Đã lấp {wikidata_fill_count} ô trống từ Wikidata.")
    print(f"Đã lưu {len(rows)} dòng vào {output_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract Vietnamese university data for DBpedia ETL.")
    parser.add_argument("--output", default=OUTPUT_FILE)
    parser.add_argument("--category", default=CATEGORY_TITLE)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--sleep", type=float, default=0.05)
    parser.add_argument("--max-depth", type=int, default=4)
    arguments = parser.parse_args()

    run(
        output_file=arguments.output,
        category_title=arguments.category,
        limit=arguments.limit,
        sleep_sec=arguments.sleep,
        max_depth=arguments.max_depth,
    )
