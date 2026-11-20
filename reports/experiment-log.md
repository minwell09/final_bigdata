# Experiment Log – Đồ án 4 (Benchmark & Tối ưu Pipeline)

| EXP ID | Mode / Tag | Change (đúng 1 biến / experiment) | Hypothesis | Median Runtime (Spread) | Spark UI / Plan Signal | Correctness (7 assertions) | Decision |
|---|---|---|---|---:|---|---|---|
| **E00** | `baseline` (`baseline-cp4`) | Pipeline gốc Spark mặc định (`shuffle.partitions=200`, `autoBroadcastJoinThreshold=10MB`, no cache) | Baseline cố định trên 1,172,000 dòng (`products_fact.parquet` + `trends_dim.csv`) | **18.756s** (18.042s–19.244s, ±6.41%) | `425 tasks`, `7 stages`, `medianTask=23ms`, `maxTask=2,840ms`, `execTime=12,278ms`, `Exchange(..., 200)` × 2 | PASS (7/7, SHA-256 baseline) | **BASELINE** |
| **E01** | `e01_bhj` | Thêm tường minh `F.broadcast(dim_df)` (giữ `shuffle.partitions=200`) | Kiểm chứng liệu ép broadcast bảng `trends_dim.csv` (12.92 MB trên đĩa) có giảm shuffle/runtime hay không | **18.744s** (18.477s–19.089s, ±3.27%) | **0.06% (< variance)**: Catalyst đã tự `BroadcastHashJoin` sau column pruning (3 cột < 10 MB), vẫn còn `425 tasks` | PASS (7/7) | **REVERT** (vô hiệu, không cải thiện) |
| **E02** | `e02_part` (`optimized-cp4`) | Giảm `spark.sql.shuffle.partitions` từ **`200 → 8`** (4× cores trên `local[2]`, giữ nguyên mọi biến khác) | Nếu `E00` chậm do 400 task shuffle siêu nhỏ (`medianTask=23ms` cho chỉ `23 KB` shuffle & `19` dòng output), thì giảm xuống `8` partitions sẽ cắt scheduling & small-file overhead | **13.442s** (13.312s–13.773s, ±3.43%) | **`tasks: 425 → 30 (-92.9%)`**, `execTime: 12,278 → 7,761ms (-36.8%)`, `maxTask: 2,840 → 2,651ms`, `Exchange(..., 8)` × 2 | PASS (7/7, SHA-256 khớp 100%) | **KEEP (-28.33%)** |
| **E03** | `e03_cache` | Thêm `scored.cache()` trên nền `E02` (negative control / kiểm chứng anti-pattern) | Kiểm chứng giả thuyết cache DataFrame `scored` (1.17M dòng) trên pipeline chỉ dùng 1 lần có giúp tăng tốc không | **21.988s** (21.747s–22.370s, ±2.83%) | **Chậm hơn +63.6% vs E02 (-17.23% vs E00)**; `maxTask: 2,651 → 9,987ms` (3.8×), `execTime: 7,761 → 17,605ms`, `+2 stages` | PASS (7/7) | **REVERT** (regression hiệu năng) |

## Điều kiện đo & Reproducibility
- **Dataset cố định** (sinh từ raw thật `data/raw/` ngày `2026-10-23` qua `src/prepare_benchmark_data.py --scale 2000`):
  - `data/benchmark/products_fact.parquet`: **1,172,000 dòng** (10.29 MB)
  - `data/benchmark/trends_dim.csv`: **32,500 dòng** (12.92 MB)
- **Tài nguyên & Spark config cố định giữa mọi lần chạy:**
  - Spark `3.5.3` | `master=local[2]` | `spark.driver.memory=2g` | `spark.sql.autoBroadcastJoinThreshold=10MB` (mặc định) | `spark.sql.adaptive.enabled=false`
- **Trạng thái warm-up/cache:** Mỗi run khởi chạy một tiến trình JVM độc lập (`cold JVM`), không dùng OS/Spark cache giữa các run.
- **Bằng chứng Spark UI thật:** Đọc trực tiếp qua Spark REST API (`/api/v1/applications/{id}/stages?details=true`), lưu tại `reports/benchmark.csv`, `reports/spark_ui_stages_before.json`, `reports/spark_ui_stages_after.json`.
