# Experiment Log – Đồ án 4 (Benchmark & Tối ưu Pipeline)

| EXP ID | Mode / Tag | Change (1 biến duy nhất) | Hypothesis | Median Runtime | Plan Signal | Correctness | Decision |
|---|---|---|---|---:|---|---|---|
| E00 | `baseline` (`baseline-cp4`) | Pipeline gốc (`fact_df.join(dim_df, on="category_id")`) | Baseline cố định để đo hiệu năng và tìm bottleneck | **16.228s** (runs: 16.445s, 15.831s, 16.228s) | `SortMergeJoin` + 2× `Exchange hashpartitioning(category_id, 64)` + `Sort` | PASS (5/5 assertions) | BASELINE |
| E01 | `optimized` (`optimized-cp4`) | `fact_df.join(F.broadcast(dim_df), on="category_id")` | Nếu `SortMergeJoin` gây shuffle + sort toàn bộ 879,000 dòng bảng fact trên `category_id`, thì broadcast bảng dim nhỏ (5 dòng, 3.85 KB) sẽ chuyển sang `BroadcastHashJoin`, loại bỏ shuffle/sort bảng fact và giảm runtime, trong khi output giữ nguyên. | **13.458s** (runs: 13.458s, 13.467s, 13.302s) | `BroadcastHashJoin` + `BroadcastExchange`, không còn `SortMergeJoin`/`Sort` và bỏ `Exchange` trên bảng fact | PASS (5/5 assertions) | **KEEP (-17.07%)** |

## Ghi chú điều kiện đo (Reproducibility)
- **Workload cố định:** `products_fact.parquet` (879,000 dòng, 5.74 MB) join `category_trends.parquet` (5 dòng, 3.85 KB), sinh từ dữ liệu raw thật `data/raw/` (`ingest_date=2026-10-23`).
- **Tài nguyên & Spark config cố định giữa Baseline và After:**
  - Spark `3.5.3`, `master=local[2]`, `spark.driver.memory=2g`, `spark.sql.shuffle.partitions=64`, `spark.sql.autoBroadcastJoinThreshold=-1`, `spark.sql.adaptive.enabled=false`.
- **Trạng thái cache/warm-up:** Mỗi run khởi chạy process Spark mới độc lập (cold JVM), không cache dữ liệu giữa các run.
- **Độ biến động (variance):** Baseline dao động 15.831s–16.445s (~3.8%); Optimized dao động 13.302s–13.467s (~1.2%). Mức cải thiện **17.07%** (nhanh hơn 2.77s) vượt xa nhiễu đo đạc.
