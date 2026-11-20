# Hướng dẫn Pitch 4 phút & Checklist Buổi 10 – Đồ án 4 (Checkpoint 4)

**Repo:** https://github.com/minwell09/final_bigdata  
**Tags:** `baseline-cp4` (`15150da`) | `optimized-cp4` / `checkpoint-4`  
**Slide trình chiếu:** `docs/BigData_DoAn4_Checkpoint4_Report.pptx`  
**Báo cáo Word:** `docs/BigData_DoAn4_Brief_Checkpoint4_Report.docx`

---

## 1. Checklist kiểm tra trước khi vào lớp
- [x] **Freeze baseline:** Tag `baseline-cp4` (commit `15150da`).
- [x] **Baseline ≥ 3 runs + median:** `19.244s`, `18.756s`, `18.042s` → **Median = `18.756s`** (`reports/benchmark.csv`).
- [x] **Đầy đủ context:**
  - Input: `products_fact.parquet` (1,172,000 dòng, 10.29 MB) + `trends_dim.csv` (32,500 dòng, 12.92 MB).
  - Config cố định: Spark `3.5.3` | `local[2]` | `driver.memory=2g` | `shuffle.partitions=200` (mặc định) | `autoBroadcast=10MB` | `AQE=false` | `cold JVM`.
- [x] **Execution evidence:** `reports/explain-before.txt`, `reports/explain-after.txt`, `reports/spark_ui_stages_before.json`, `reports/spark_ui_stages_after.json`.
- [x] **Correctness:** 7/7 assertions **PASS** (kể cả SHA-256 checksum).

---

## 2. Kịch bản Pitch đầu buổi (≤ 4 phút – bám sát Slide 1 → Slide 4)

### Phút 0:00 – 1:00 | Baseline (Slide 1 & 2)
> "Pipeline của nhóm tính `trend_score` và gắn nhãn affiliate (`HOT/RISING/STABLE/COLD`) trên **1,172,000 dòng** sản phẩm Tiki join với **32,500 dòng** Google Trends. Nhóm đã freeze ở tag `baseline-cp4` (`15150da`), chạy trên Spark 3.5.3, `local[2]`, `driver=2g`, `shuffle.partitions=200` mặc định, cold JVM. Chạy 3 lần được `19.24s`, `18.76s`, `18.04s` → **Median = 18.76s**, output 19 dòng summary và tổng GMV proxy cố định."

### Phút 1:00 – 2:15 | Bottleneck Evidence (Slide 3)
> "Kiểm tra `explain("formatted")` (`reports/explain-before.txt`), Catalyst đã tự động chọn `BroadcastHashJoin` sau column-pruning nên join **không phải** nút thắt. Nút thắt thật nằm ở 2 bước sau join: operator `(12) Exchange hashpartitioning(200)` và `(14) Exchange rangepartitioning(200)`. Số liệu Spark UI cho thấy Spark phải lập lịch tới **425 tasks** (`medianTask = 23 ms`) chỉ để xử lý **22.5 KB** shuffle và ghi **19 dòng** output — gây overhead lập lịch task và ghi nhiều file nhỏ rất lớn."

### Phút 2:15 – 3:30 | Hypothesis & 1 thay đổi chính (Slide 4)
> "Nhóm đặt giả thuyết: **Nếu** over-partitioning (`200` partitions) tạo 425 task siêu nhỏ trên `local[2]`, **thì** khi thay **duy nhất 1 config** `spark.sql.shuffle.partitions` từ `200 → 8` (4× cores), giữ nguyên toàn bộ input và tài nguyên, kỳ vọng số task giảm ~90%, `Exchange(200) → Exchange(8)` trên plan và median runtime giảm mạnh, trong khi 7 correctness assertions (kể cả SHA-256 checksum) giữ nguyên."

### Phút 3:30 – 4:00 | Kết quả Before/After & Decision (Slide 5 & 6 – trình bày ở phần Checkpoint)
> "Kết quả đo 3 runs: median giảm từ **18.76s → 13.44s (-28.33%)**, vượt xa variance `3.43%`, tasks giảm **425 → 30 (-92.9%)**, executor time giảm **36.8%**, và **7/7 correctness PASS** → **KEEP**. Nhóm cũng chạy đối chứng `E01` (ép broadcast tường minh: +0.06% → **REVERT**) và `E03` (cache 1-pass: chậm thêm +63.6% → **REVERT**)."

---

## 3. Lệnh demo trực tiếp trên lớp (nếu giảng viên yêu cầu)
```bash
cd ~/trend-affiliate-bigdata
source .venv/bin/activate
python src/run_benchmark_cp4.py
```

---

## 4. Bộ câu trả lời nhanh cho câu hỏi Audit của giảng viên
1. **Baseline có cùng workload và tài nguyên với After không?**  
   Có — cùng 1,172,000 dòng input, cùng `local[2]`, `driver=2g`, `autoBroadcast=10MB`, `AQE=false`, cùng `cold JVM`, chỉ thay đúng 1 biến `spark.sql.shuffle.partitions: 200 → 8`.
2. **Metric nào chứng minh bottleneck? Nằm ở operator nào?**  
   Spark UI ghi nhận `425 tasks` với `medianTask = 23 ms` cho chỉ `22.5 KB` shuffle; trên `explain-before.txt` là operator `(12) Exchange hashpartitioning(..., 200)` và `(14) Exchange rangepartitioning(..., 200)`.
3. **Output sau tối ưu có đúng như baseline không?**  
   Đúng tuyệt đối — PASS 7/7 assertions: row count (`1,172,000`), 19 dòng summary, `0` null/duplicate keys, phân bố nhãn affiliate, tổng GMV proxy, schema và **SHA-256 checksum** (`f7dc7bd06fbfa7de…`).
4. **Nếu speedup nhỏ hơn variance thì kết luận gì?**  
   Như trường hợp `E01` (+0.06% < variance 3.27%) → không kết luận nhanh hơn, **REVERT**. Còn `E02` cải thiện **28.33%**, gấp ~8× variance (`3.43%`) → đủ cơ sở **KEEP**.
5. **Nếu data tăng 10× thì `shuffle.partitions=8` còn hợp lý không?**  
   Không cố định `8` — `8` tối ưu cho `local[2]` với ~23 MB input. Khi scale 10×–100× trên cluster nhiều node cần tăng `shuffle.partitions` tỷ lệ với số cores/kích thước shuffle hoặc bật AQE `coalescePartitions` để tránh task quá lớn/OOM.
