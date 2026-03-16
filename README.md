# VIO Project

Ontology và ETL pipeline để xây dựng knowledge graph kiểu Vietnamese DBpedia cho miền dữ liệu các trường đại học Việt Nam.

## Overview

`vio-project` thu thập dữ liệu từ Wikipedia tiếng Việt và Wikidata, chuẩn hóa thành CSV, ánh xạ sang RDF/Turtle bằng ontology `vio`, rồi nạp vào Apache Jena Fuseki để truy vấn bằng SPARQL.

Project này phù hợp cho:

- đồ án Semantic Web,
- demo tri thức liên kết,
- báo cáo học phần về ontology, RDF, SPARQL,
- thực nghiệm với dữ liệu giáo dục Việt Nam.

## Highlights

- Crawl đệ quy từ category và sub-category của Wikipedia tiếng Việt.
- Trích xuất lai từ infobox, ngữ cảnh văn bản, và Wikidata.
- Chuẩn hóa thực thể `University`, `Academic`, `GoverningBody`, `Site`, `City`, `Province`, `Person`.
- Sinh RDF/Turtle bằng `rdflib` theo ontology `vio`.
- Có thể đóng gói toàn bộ môi trường chạy bằng Docker và Docker Compose.
- Hỗ trợ quan hệ trường thành viên, cơ quan chủ quản, site/campus, và liên kết `owl:sameAs` tới Wikidata.
- Làm giàu tọa độ với mức chất lượng `Exact` và `Approximate`.
- Siết logic năm thành lập:
   - ưu tiên tuyệt đối Wikidata `P571`,
   - chỉ nhận năm trong ngữ cảnh tin cậy như `thành lập năm`, `năm thành lập`, `founded`, `established`, `quyết định ... năm`,
   - loại các số giống địa chỉ như `số`, `đường`, `quốc lộ`, `km`,
   - mặc định nhận `1900..2025`, và chỉ cho phép `1850..1899` khi có ngữ cảnh lịch sử rõ ràng.

## Verified Outputs

Các artifact đã được kiểm tra gần nhất:

- `data/vietnam_universities_details_full.csv`: `324` dòng.
- `ontology/vio.owl.ttl`: parse thành công với `188` triples ontology.
- `data/universities_instances.ttl`: parse thành công với `7558` triples dữ liệu.
- Fuseki dataset `vio`: tổng `7746` triples sau khi nạp ontology + data.

## Project Structure

- `Dockerfile` — image Python để chạy ETL scripts trong container.
- `docker-compose.yml` — orchestration cho service ETL và Apache Fuseki.
- `.dockerignore` — loại bỏ file không cần thiết khỏi Docker build context.
- `ontology/vio.owl.ttl` — ontology schema của namespace `vio`.
- `scripts/extract_vi_universities.py` — pipeline crawl, parse, enrich và xuất CSV.
- `scripts/csv_to_ttl.py` — chuyển CSV sang RDF/Turtle.
- `data/` — dữ liệu đầu ra CSV và Turtle.
- `queries/` — truy vấn SPARQL mẫu.
- `tests/` — test cho parser và logic trích xuất.
- `requirements.txt` — dependencies Python.

## Technology Stack

- Python
- `requests`
- `rdflib`
- `pytest`
- Apache Jena Fuseki
- MediaWiki API
- Wikidata API

## Ontology Summary

### Main Classes

- `vio:University`
- `vio:Academic`
- `vio:GoverningBody`
- `vio:Person`
- `vio:Site`
- `vio:City`
- `vio:Province`

### Main Object Properties

- `vio:hasSite`
- `vio:isMemberOf`
- `vio:hasMember`
- `vio:governedBy`
- `vio:governs`
- `vio:headOfUniversity`
- `vio:isHeadOf`
- `vio:locatedInCity`
- `vio:locatedInProvince`
- `vio:isPartOf`

### Main Datatype Properties

- `vio:name`
- `vio:address`
- `vio:foundingYearOrg`
- `vio:numberOfStudents`
- `vio:academicStaffSize`
- `vio:coordLevel`

## Setup

### 1. Tạo môi trường Python

```powershell
cd D:\Sematicweb\vio-project
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### 2. Dependencies

`requirements.txt` hiện dùng:

- `requests>=2.32.0`
- `rdflib>=7.0.0`
- `pytest>=8.0.0`

## Docker Packaging

Project hiện hỗ trợ hai cách chạy:

- chạy trực tiếp bằng Python local,
- hoặc chạy qua Docker Compose để đồng nhất môi trường ETL và Fuseki.

### Container Architecture

- `vio-etl` — container Python chứa toàn bộ source code và dependencies để chạy `extract_vi_universities.py` và `csv_to_ttl.py`.
- `fuseki` — container Apache Jena Fuseki để publish ontology và knowledge graph qua SPARQL endpoint.

### Start the packaged stack

```powershell
cd D:\Sematicweb\vio-project
docker compose up -d --build
```

Sau khi chạy:

- ETL container có tên `vio-etl`
- Fuseki có tên `vio-fuseki`
- Fuseki UI mặc định ở `http://localhost:3030`

### Stop the stack

```powershell
cd D:\Sematicweb\vio-project
docker compose down
```

### Rebuild after source changes

```powershell
cd D:\Sematicweb\vio-project
docker compose up -d --build
```

### Run ETL commands inside the container

```powershell
cd D:\Sematicweb\vio-project
docker compose exec vio-etl python .\scripts\extract_vi_universities.py --category "Category:Đại học Việt Nam" --output .\data\vietnam_universities_details_full.csv --sleep 0.02 --max-depth 5
docker compose exec vio-etl python .\scripts\csv_to_ttl.py --input .\data\vietnam_universities_details_full.csv --output .\data\universities_instances.ttl
```

Lưu ý: thư mục project được mount vào `/app` trong container, nên dữ liệu sinh ra vẫn xuất hiện trực tiếp ở thư mục `data/` trên máy host.

## Run the ETL Pipeline

### 1. Crawl và sinh CSV

```powershell
cd D:\Sematicweb\vio-project
.\.venv\Scripts\python.exe .\scripts\extract_vi_universities.py --category "Category:Đại học Việt Nam" --output .\data\vietnam_universities_details_full.csv --sleep 0.02 --max-depth 5
```

### 2. Chuyển CSV sang RDF/Turtle

```powershell
cd D:\Sematicweb\vio-project
.\.venv\Scripts\python.exe .\scripts\csv_to_ttl.py --input .\data\vietnam_universities_details_full.csv --output .\data\universities_instances.ttl
```

## Run Tests

```powershell
cd D:\Sematicweb\vio-project
.\.venv\Scripts\python.exe -m pytest .\tests\test_infobox_parser.py
```

## Use with Apache Fuseki

### Start Fuseki by Docker

```powershell
docker run -d --name fuseki -p 3030:3030 stain/jena-fuseki
```

Hoặc nếu muốn chạy bản đóng gói của project:

```powershell
cd D:\Sematicweb\vio-project
docker compose up -d fuseki
```

Local UI:

- `http://localhost:3030`

### Load ontology and data

Nếu dataset `vio` đã tồn tại, có thể nạp ontology và dữ liệu bằng PowerShell:

```powershell
$pair = 'admin:YOUR_PASSWORD'
$auth = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes($pair))
$headers = @{ Authorization = "Basic $auth" }

Invoke-WebRequest -UseBasicParsing -Method Post -Uri 'http://localhost:3030/vio/update' -Headers $headers -ContentType 'application/sparql-update' -Body 'CLEAR DEFAULT'
Invoke-WebRequest -UseBasicParsing -Method Post -Uri 'http://localhost:3030/vio/data?default' -Headers $headers -InFile '.\ontology\vio.owl.ttl' -ContentType 'text/turtle'
Invoke-WebRequest -UseBasicParsing -Method Post -Uri 'http://localhost:3030/vio/data?default' -Headers $headers -InFile '.\data\universities_instances.ttl' -ContentType 'text/turtle'
```

Nếu đang dùng Docker Compose với password mặc định trong `docker-compose.yml`, tài khoản admin mặc định là:

- username: `admin`
- password: `admin123`

Bạn nên đổi password này bằng biến môi trường `FUSEKI_ADMIN_PASSWORD` trước khi public hoặc demo.

SPARQL endpoint:

- `http://localhost:3030/vio/sparql`

Ví dụ truy vấn đếm triple:

```sparql
SELECT (COUNT(*) AS ?count)
WHERE { ?s ?p ?o }
```

## Sample SPARQL Queries

Các truy vấn mẫu hiện có:

- `queries/member_universities.rq`
- `queries/universities_by_city.rq`
- `queries/universities_by_province.rq`
- `queries/universities_by_year.rq`
- `queries/universities_wikidata_links.rq`
- `queries/universities_with_coordinates.rq`

Use cases tiêu biểu:

- thống kê trường theo thành phố hoặc tỉnh,
- phân tích quan hệ trường thành viên,
- hiển thị dữ liệu không gian với tọa độ,
- khai thác liên kết `owl:sameAs` tới Wikidata,
- phân tích theo năm thành lập.

## ETL Flow

1. Crawl các bài từ `Category:Đại học Việt Nam` và sub-category liên quan.
2. Parse infobox và phần mở đầu bài viết.
3. Enrich thiếu hụt bằng Wikidata.
4. Chuẩn hóa tên, địa điểm, cơ quan chủ quản, người đứng đầu, tọa độ và quan hệ thành viên.
5. Xuất CSV chuẩn hóa.
6. Chuyển CSV sang RDF/Turtle theo ontology `vio`.
7. Nạp ontology và instance data vào Fuseki.
8. Truy vấn knowledge graph bằng SPARQL.

## Docker Workflow

Quy trình đóng gói khuyến nghị:

1. Chạy `docker compose up -d --build`.
2. Dùng `docker compose exec vio-etl ...` để crawl và sinh lại CSV/Turtle.
3. Tạo dataset `vio` trong Fuseki UI hoặc qua API.
4. Nạp `ontology/vio.owl.ttl` và `data/universities_instances.ttl` vào Fuseki.
5. Truy vấn dữ liệu tại `http://localhost:3030/vio/sparql`.

## Data Quality Notes

- Một số bài viết không có infobox đầy đủ; extractor sẽ fallback sang văn bản và Wikidata.
- `coord_level` biểu diễn chất lượng tọa độ: `Exact` hoặc `Approximate`.
- Trường nhiều cơ sở được mô hình hóa bằng `vio:Site`.
- Cơ quan chủ quản được chuẩn hóa từ override thủ công, heuristic và Wikidata.
- Dữ liệu mang tính nghiên cứu/học thuật; nội dung nguồn có thể thay đổi theo Wikipedia và Wikidata.

## Repository Publishing Checklist

Khi đưa lên GitHub, nên giữ đầy đủ:

- `scripts/`
- `ontology/`
- `data/`
- `queries/`
- `tests/`
- `Dockerfile`
- `docker-compose.yml`
- `.dockerignore`
- `requirements.txt`
- `.gitignore`
- `README.md`

## License

Hãy thêm license phù hợp với yêu cầu môn học hoặc giảng viên trước khi public repo.

## Author

Bạn có thể cập nhật thêm tên, lớp, trường, hoặc link báo cáo để repo dùng luôn như portfolio học thuật.
