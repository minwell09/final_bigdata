# Checkpoint 4 – Benchmark & Tối ưu Pipeline

**Đề tài:** AI truy xuất xu hướng để gắn nhãn Affiliate cho sàn TMĐT (`trend-affiliate-bigdata`)  
**Nhóm:** Trần Tấn Toàn (nhóm trưởng), Phạm Xuân Khoa, Hồ Tuấn Anh, Trần Thanh Lam

---

## 1. Baseline & Reproducibility (`tag: baseline-cp4`)
- **Pipeline:** `src/pipeline_affiliate_spark.py` — đọc `products_fact.parquet` (**1,172,000 dòng**, 10.29 MB) và `trends_dim.csv` (**32,500 dòng**, 12.92 MB), lọc `price > 0`, join theo `trend_key`, tính `trend_score`, gắn nhãn affiliate (`HOT`, `RISING`, `STABLE`, `COLD`), tổng hợp theo `(category_id, category_name, affiliate_label)` và ghi Parquet.
- **Cấu hình tài nguyên cố định (giữ nguyên qua mọi experiment):**
  - Spark `3.5.3` | `master=local[2]` | `spark.driver.memory=2g` | `autoBroadcastJoinThreshold=10MB` (mặc định Spark) | `spark.sql.adaptive.enabled=false` | `cold JVM` mỗi run.
- **Baseline (`E00`, `shuffle.partitions=200` mặc định — 3 runs):**
  - Runs: `19.244s`, `18.756s`, `18.042s` → **Median = `18.756s`** (spread `6.41%`)
  - Spark UI telemetry: **`425 tasks`** (`7 stages`), `medianTask = 23 ms`, `maxTask = 2,840 ms`, `executorRunTime = 12,278 ms`, `shuffle = 23.0 KB` (`14,395 B read` / `8,615 B write`).

## 2. Bottleneck Evidence (Plan + Spark UI Telemetry)
- **Observation 1 (Plan – `reports/explain-before.txt`):** Catalyst đã tự động chọn `BroadcastHashJoin` cho `trends_dim.csv` sau khi column-pruning còn 3 cột (nhỏ hơn 10 MB), **nhưng** ở 2 bước sau join (`HashAggregate` và `Sort`), Spark phát sinh:
  - `(12) Exchange hashpartitioning(category_id, category_name, affiliate_label, 200)`
  - `(14) Exchange rangepartitioning(category_id ASC, affiliate_label ASC, 200)`
- **Observation 2 (Spark UI – `reports/spark_ui_stages_before.json`):** Tổng cộng **`425 tasks`** được lập lịch chỉ để xử lý **`23 KB` dữ liệu shuffle** và ghi ra **`19 dòng` tổng hợp**; `medianTask = 23 ms` chứng tỏ hàng trăm task rỗng gây **task scheduling overhead & small-file write overhead** rất lớn trên `local[2]`.
- **Bottleneck Claim:** *"Chúng tôi cho rằng nút thắt hiệu năng chính nằm ở over-partitioning (`spark.sql.shuffle.partitions=200` ở operator 12 và 14) vì tạo ra 425 task siêu nhỏ (`median=23 ms`) cho chỉ 23 KB shuffle, chiếm hơn 4.5 giây executor/scheduler overhead."*

## 3. Hypotheses & Controlled Experiments (1 biến / experiment)
1. **E01 (`F.broadcast(dim_df)` tường minh):** Kiểm chứng liệu ép broadcast bảng 12.92 MB có giảm runtime không → **0.06% (`18.744s`)**, nằm hoàn toàn trong nhiễu vì Catalyst đã tự broadcast sau column-pruning → **REVERT**.
2. **E02 (`spark.sql.shuffle.partitions: 200 → 8` — Tối ưu chính):**
   > **Nếu** nút thắt over-partitioning xảy ra vì mặc định `shuffle.partitions=200` tạo 425 task siêu nhỏ (`median=23ms`) cho 23 KB shuffle trên `local[2]`, **thì** khi giảm **duy nhất** `spark.sql.shuffle.partitions` từ `200` xuống `8` (4× cores), tôi kỳ vọng `Exchange (12)` và `Exchange (14)` giảm từ `200 → 8` partitions, số task giảm ~90%, và median runtime giảm mạnh, **trong khi** toàn bộ 7 correctness assertions (bao gồm SHA-256 checksum) giữ nguyên tuyệt đối.
   → **Kết quả:** Median **`18.756s → 13.442s` (-28.33%)**, `tasks: 425 → 30 (-92.9%)`, `execTime: 12,278 → 7,761 ms (-36.8%)` → **KEEP**.
3. **E03 (`scored.cache()` trên pipeline 1-pass — Negative Control):** Kiểm chứng anti-pattern "cache mọi thứ" → Median tăng ngược lên **`21.988s` (+63.6% chậm hơn E02)**, `maxTask` vọt từ `2,651 ms → 9,987 ms` (3.8×) do chi phí serialize/lưu 1.17M dòng vào RAM → **REVERT**.

## 4. Before / After Summary (`E00 Baseline` vs `E02 Optimized`)
| Metric | E00 Baseline (`200` part) | E02 Optimized (`8` part) | Δ (Thay đổi) |
|---|---:|---:|---:|
| Runtime Runs (3 runs) | 19.244s / 18.756s / 18.042s | 13.773s / 13.442s / 13.312s | Ổn định hơn (`6.41% → 3.43%`) |
| **Median Runtime** | **18.756 s** | **13.442 s** | **-5.314 s (-28.33%)** |
| **Total Tasks (Spark UI)** | **425 tasks** | **30 tasks** | **-395 tasks (-92.9%)** |
| **Executor Run Time** | **12,278 ms** | **7,761 ms** | **-4,517 ms (-36.8%)** |
| **Shuffle Read / Write** | 14.1 KB / 8.4 KB (22.5 KB) | 11.3 KB / 7.0 KB (18.3 KB) | -18.4% metadata overhead |
| **Max Task Time** | 2,840 ms | 2,651 ms | -189 ms (-6.7%) |
| **Output Rows (Total / Summary)** | 1,172,000 / 19 | 1,172,000 / 19 | 0 (Exact match) |
| **GMV Proxy** | 1,580,457,124,578,000 | 1,580,457,124,578,000 | 0 (Exact match) |

## 5. Correctness Verification (7/7 PASS)
1. `row_count_match`: **PASS** (`1,172,000 == 1,172,000`)
2. `summary_rows_match`: **PASS** (`19 == 19`)
3. `zero_null_or_dup_keys`: **PASS** (`0` null keys, `0` duplicate keys)
4. `label_distribution_match`: **PASS** (`HOT: 355,820` | `RISING: 379,090` | `STABLE: 163,453` | `COLD: 273,637`)
5. `gmv_proxy_match`: **PASS** (`1,580,457,124,578,000.0`, sai số `< 1e-2`)
6. `schema_contract_match`: **PASS** (khớp 100% tên cột & kiểu dữ liệu)
7. `output_checksum_match`: **PASS** (`SHA-256 = f7dc7bd06fbfa7de06aa06c9282d73bcf9e49d8355f748792d5f635f055c8fd5`)

## 6. Decision, Limitations & Next Action
- **Decision:** **KEEP `E02`** (`spark.sql.shuffle.partitions=8`); **REVERT `E01`** (dư thừa) và **REVERT `E03`** (regression +63.6%).
- **Lý do:** Giảm **28.33%** median runtime (~8× độ biến động `3.43%`), giảm **92.9%** số task (`425 → 30`), giảm **36.8%** executor CPU time, pass **7/7 correctness assertions** (kể cả SHA-256 checksum).
- **Giới hạn kết luận:** Kết quả đo trên `local[2]` với 1.17M dòng (~23 MB tổng input) nên `shuffle.partitions=8` tối ưu cho máy 2 cores; nếu dữ liệu tăng 10×–100× trên cluster nhiều node, cần tăng `shuffle.partitions` tỷ lệ với tổng số cores và kích thước shuffle (hoặc bật AQE `coalescePartitions`) để tránh task quá lớn/OOM.
- **Next Action (Buổi 11 – Storage):** Đánh giá chiến lược lưu trữ output (Parquet partitioning theo `ingest_date`/`category_id`, nén Snappy vs Zstd, hoặc đưa tầng serving vào NoSQL/Data Lake).
