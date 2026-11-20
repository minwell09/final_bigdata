# AI truy xuất xu hướng để gắn nhãn Affiliate cho sàn TMĐT

**Môn:** Dữ liệu lớn (BigData 2026) – **Đồ án 2 / Checkpoint 2:** Environment → Ingestion → Raw Storage
**Nhóm:** Trần Tấn Toàn (nhóm trưởng), Phạm Xuân Khoa, Hồ Tuấn Anh, Trần Thanh Lam

## 1. Bài toán
Hệ thống thu thập **tín hiệu xu hướng** (Google Trends) và **dữ liệu sản phẩm** trên sàn TMĐT (Tiki),
để ở các buổi sau dùng Spark xử lý → tính *trend score* → **gắn nhãn affiliate** (`HOT`, `RISING`,
`STABLE`, `COLD`) cho sản phẩm nên ưu tiên đặt link affiliate.

Checkpoint 2 chỉ làm phần: **dữ liệu thật được ingest và lưu an toàn vào raw storage có manifest**.

```
[Google Trends RSS] ─┐
[Google Trends API] ─┼─► src/ingest_*.py ─► data/raw/<dataset>/ingest_date=YYYY-MM-DD/run_<UTC>/ ─► Spark (verify)
[Tiki listing API ] ─┘        (Python)            + _manifest.json + _SUCCESS + metadata/ingest_log.jsonl
```

## 2. Cấu trúc repo
```
trend-affiliate-bigdata/
├─ README.md
├─ requirements.txt          # runtime: pyspark, pytrends, requests, pandas
├─ requirements-docs.txt     # chỉ để sinh báo cáo
├─ .env.example
├─ config/sources.json       # từ khoá, danh mục Tiki, tham số
├─ src/
│  ├─ env_check.py           # smoke test Spark
│  ├─ common.py              # run_id, path convention, manifest
│  ├─ ingest_trends_rss.py   # Google Trends Trending Now (RSS)
│  ├─ ingest_google_trends.py# Google Trends interest-over-time (pytrends)
│  ├─ ingest_tiki_products.py# Tiki product listing (JSON)
│  ├─ ingest_all.py          # chạy cả 3 nguồn
│  └─ verify_raw_spark.py    # Spark đọc raw + đối chiếu manifest
├─ data/raw/ , data/sample/
├─ logs/                     # env_check.txt, verify_raw.txt, *.log
├─ metadata/                 # ingest_log.jsonl, sources.md
├─ docs/                     # architecture, checkpoint report (pptx/docx)
└─ tools/                    # script sinh báo cáo
```

## 3. Setup (≤ 10 phút, Windows)
Yêu cầu: **Python 3.11**, **JDK 17** (Temurin), Git.
```powershell
winget install -e --id EclipseAdoptium.Temurin.17.JDK
winget install -e --id Python.Python.3.11

cd trend-affiliate-bigdata
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
copy .env.example .env        # sửa JAVA_HOME nếu khác
```
Linux/macOS: `python3.11 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt`.

## 4. Chạy
| Bước | Lệnh | Evidence |
|---|---|---|
| Smoke test | `python src/env_check.py` | `logs/env_check.txt` (RESULT: PASS) |
| Ingest tất cả | `python src/ingest_all.py` | `data/raw/...`, `metadata/ingest_log.jsonl` |
| Ingest 1 nguồn | `python src/ingest_tiki_products.py --pages 3` | `_manifest.json` trong run folder |
| Verify bằng Spark | `python src/verify_raw_spark.py` | `logs/verify_raw.txt` (VERIFY RESULT: PASS) |

## 5. Quy ước raw storage
```
data/raw/<dataset>/ingest_date=YYYY-MM-DD/run_<YYYYMMDDTHHMMSSffffffZ>/
    <file raw nguyên bản>     # bytes đúng như source trả về (JSON/XML) hoặc output pytrends (CSV)
    _manifest.json            # source, params, ingest time, bytes, sha256, records, status, errors
    _SUCCESS                  # chỉ có khi run SUCCESS/PARTIAL
```
Dataset: `tiki_products`, `google_trends`, `google_trends_rss`.
Mọi run cũng được append vào `metadata/ingest_log.jsonl` (sổ cái toàn cục).

## 6. Rerun behavior (chạy ingest 3 lần liên tiếp)
- **Append-only theo run_id**: mỗi lần chạy tạo một thư mục `run_<UTC timestamp micro-giây>` mới
  (`mkdir(exist_ok=False)`) → **không bao giờ overwrite** run cũ; raw là bất biến.
- 3 lần chạy ⇒ 3 thư mục run + 3 manifest + 3 dòng trong `ingest_log.jsonl`.
- **Duplicate handling**: sha256 từng file được so với các run SUCCESS trước; nếu trùng, manifest ghi
  `duplicate_of_run` (vẫn giữ file để bảo toàn snapshot). Khử trùng lặp nội dung (theo `product_id` +
  `snapshot_time`) thực hiện ở tầng staging (Buổi 7), không sửa raw.
- Run lỗi giữa chừng: manifest `FAILED`/`PARTIAL`, không có `_SUCCESS` (FAILED) → downstream bỏ qua.

## 7. Nguồn dữ liệu & provenance
Xem [metadata/sources.md](metadata/sources.md).

---

## 8. Hướng dẫn chạy lại Đồ án 4 (Checkpoint 4 – Benchmark & Tối ưu Pipeline)

Mọi thành viên trong nhóm có thể tái lập 100% kết quả benchmark của Đồ án 4 chỉ với 4 bước sau.

### Bước 1: Clone repo & cài đặt môi trường
Yêu cầu: **Python 3.11/3.12**, **JDK 17 hoặc 21**, **Git**.

**Linux / macOS / Cloud Shell:**
```bash
git clone https://github.com/minwell09/final_bigdata.git
cd final_bigdata
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt -r requirements-docs.txt
```

**Windows (PowerShell):**
```powershell
git clone https://github.com/minwell09/final_bigdata.git
cd final_bigdata
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt -r requirements-docs.txt
```

### Bước 2: Đảm bảo có dữ liệu raw (nếu máy mới clone chưa có `data/raw/`)
Vì `data/raw/` không được commit lên Git, nếu thư mục chưa có dữ liệu thì chạy ingest trước:
```bash
python src/ingest_all.py
```

### Bước 3: Sinh bộ dữ liệu benchmark cố định (1,172,000 dòng)
```bash
python src/prepare_benchmark_data.py --scale 2000
```
Kết quả tạo ra trong `data/benchmark/`:
- `products_fact.parquet` (~10.3 MB, 1,172,000 dòng)
- `trends_dim.csv` (~12.9 MB, 32,500 dòng)

### Bước 4: Chạy Benchmark & kiểm tra Correctness
**Chạy lẻ từng chế độ (`baseline`, `e01_bhj`, `e02_part`, `e03_cache`):**
```bash
python src/pipeline_affiliate_spark.py --mode baseline
python src/pipeline_affiliate_spark.py --mode e02_part
```

**Chạy trọn bộ 4 thí nghiệm × 3 runs (12 runs + Spark UI metrics + 7 correctness assertions):**
```bash
python src/run_benchmark_cp4.py
```
Kết quả tự động xuất vào `reports/`:
- `reports/benchmark.csv` — toàn bộ 12 runs + Spark UI metrics (`shuffle`, `tasks`, `maxTask`, `execTime`)
- `reports/benchmark_summary.json` — median, variance, speedup, 7 correctness assertions & SHA-256 checksum
- `reports/explain-before.txt`, `reports/explain-after.txt` — execution plan trước/sau
- `reports/spark_ui_stages_before.json`, `reports/spark_ui_stages_after.json` — bằng chứng Spark UI

### Bước 5 (Tuỳ chọn): Sinh lại biểu đồ & báo cáo Word/PPTX
```bash
python tools/generate_cp4_reports.py
```
Xuất ra `reports/benchmark_before_after.png`, `docs/BigData_DoAn4_Brief_Checkpoint4_Report.docx` và `docs/BigData_DoAn4_Checkpoint4_Report.pptx`.

### Các Git Tags quan trọng
- `checkpoint-2` — Hoàn thành Ingestion & Raw Storage
- `baseline-cp4` — Freeze pipeline gốc Đồ án 4
- `optimized-cp4` / `checkpoint-4` — Bản tối ưu (`E02`: `shuffle.partitions=8`, -28.33% runtime, -92.9% tasks, 7/7 correctness PASS)
