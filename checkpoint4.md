# Checkpoint 4 – Benchmark & Tối ưu Pipeline

**Đề tài:** AI truy xuất xu hướng để gắn nhãn Affiliate cho sàn TMĐT (`trend-affiliate-bigdata`)  
**Nhóm:** Trần Tấn Toàn (nhóm trưởng), Phạm Xuân Khoa, Hồ Tuấn Anh, Trần Thanh Lam

---

## 1. Baseline & Reproducibility
- **Pipeline:** `src/pipeline_affiliate_spark.py` — đọc `products_fact.parquet` (879,000 dòng, 5.74 MB) và `category_trends.parquet` (5 dòng, 3.85 KB), lọc `price > 0`, join theo `category_id`, tính `trend_score`, gắn nhãn affiliate (`HOT`, `RISING`, `STABLE`, `COLD`), tổng hợp theo `(category_id, category_name, affiliate_label)` và ghi Parquet.
- **Cấu hình cố định (giữ nguyên cho cả Baseline và After):**
  - Spark `3.5.3` | `master=local[2]` | `spark.driver.memory=2g` | `spark.sql.shuffle.partitions=64` | `spark.sql.adaptive.enabled=false` | `spark.sql.autoBroadcastJoinThreshold=-1`
- **Kết quả Baseline (3 runs độc lập, cold JVM):**
  - Run 1: `16.445s` | Run 2: `15.831s` | Run 3: `16.228s`
  - **Median Baseline:** **`16.228s`**

## 2. Bottleneck Evidence
- Trong `reports/explain-before.txt`, toán tử join thực thi bằng **`SortMergeJoin [category_id], [category_id], Inner`**.
- Trước khi join, Spark phải thực hiện **`Exchange hashpartitioning(category_id, 64)`** và **`Sort [category_id ASC]`** trên **cả bảng fact lớn (879,000 dòng)** lẫn bảng dim nhỏ (5 dòng).
- Việc shuffle và sort toàn bộ bảng fact chỉ để join với bảng dimension 5 dòng (3.85 KB) là nút thắt cổ chai không cần thiết.

## 3. Optimization Hypothesis (1 thay đổi chính)
> **Nếu** nút thắt `SortMergeJoin` xảy ra vì Spark phải shuffle (`Exchange hashpartitioning`) và sort toàn bộ 879,000 dòng bảng `products_fact` khi join với bảng `category_trends` rất nhỏ (5 dòng, 3.85 KB), **thì** khi áp dụng **một thay đổi duy nhất** `F.broadcast(dim_df)` trong `src/pipeline_affiliate_spark.py`, tôi kỳ vọng execution plan chuyển từ `SortMergeJoin` sang **`BroadcastHashJoin`** (bỏ shuffle và sort trên bảng fact) và giảm median runtime, **trong khi** tất cả correctness metrics (row count, phân bố nhãn affiliate, tổng GMV proxy, schema) giữ nguyên tuyệt đối.

## 4. Before / After Benchmark
| Chỉ số | Baseline (`SortMergeJoin`) | Optimized (`BroadcastHashJoin`) | Chênh lệch |
|---|---:|---:|---:|
| Run 1 | 16.445s | 13.458s | -2.987s |
| Run 2 | 15.831s | 13.467s | -2.364s |
| Run 3 | 16.228s | 13.302s | -2.926s |
| **Median** | **16.228s** | **13.458s** | **-2.770s (-17.07%)** |
| Physical Join | `SortMergeJoin` + `Exchange` + `Sort` | `BroadcastHashJoin` + `BroadcastExchange` | Bỏ shuffle & sort bảng fact |

## 5. Correctness Verification (5/5 PASS)
- `row_count_match`: **PASS** — `879,000 == 879,000`
- `summary_rows_match`: **PASS** — số dòng bảng tổng hợp trùng khớp hoàn toàn
- `label_distribution_match`: **PASS** — `HOT: 267,000` | `RISING: 285,000` | `STABLE: 123,000` | `COLD: 204,000`
- `gmv_proxy_match`: **PASS** — `1,185,342,843,433,500.0` (chênh lệch `< 1e-2`)
- `schema_contract_match`: **PASS** — schema output giữ nguyên hoàn toàn

## 6. Decision & Limitations
- **Quyết định:** **KEEP** thay đổi `F.broadcast(dim_df)` (E01).
- **Lý do:** Giảm median runtime **17.07%** (vượt xa độ biến động giữa các run ~1.2%–3.8%), xác nhận rõ tín hiệu `BroadcastHashJoin` trên `reports/explain-after.txt` và pass 5/5 correctness assertions.
- **Giới hạn kết luận:** Kết quả đo trên `local[2]` với dataset 879,000 dòng (~5.74 MB Parquet) nên thời gian khởi tạo JVM chiếm phần lớn tổng runtime; trên cluster phân tán với dữ liệu hàng chục GB, tỷ lệ cải thiện do loại bỏ network shuffle sẽ còn lớn hơn nữa. Chỉ nên dùng `broadcast()` khi bảng dimension đảm bảo nhỏ hơn ngưỡng bộ nhớ driver/executor để tránh OOM.
