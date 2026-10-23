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
