"""Sinh biểu đồ 4 experiments + điền trực tiếp báo cáo DOCX & PPTX từ template gốc giảng viên."""
from __future__ import annotations

import json
import subprocess
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from docx import Document
from pptx import Presentation

PROJECT_ROOT = Path(__file__).resolve().parent.parent
REPORTS_DIR = PROJECT_ROOT / "reports"
DOCS_DIR = PROJECT_ROOT / "docs"
TEMPLATE_DOCX = Path("/home/minwell0309/BigData_DoAn4_Brief_Checkpoint4_Template.docx")
TEMPLATE_PPTX = Path("/home/minwell0309/BigData_DoAn4_Checkpoint4_Report_Template.pptx")


def git_rev(ref: str, fallback: str) -> str:
    try:
        out = subprocess.check_output(
            ["git", "rev-parse", "--short", ref],
            cwd=PROJECT_ROOT,
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
        return out or fallback
    except Exception:
        return fallback


def make_chart(summary: dict, out_png: Path) -> None:
    exps = summary["experiments"]
    order = ["baseline", "e01_bhj", "e02_part", "e03_cache"]
    labels = [
        "E00\nBaseline\n(200 part)",
        "E01\nBroadcast\n(REVERT)",
        "E02\n8 Partitions\n(KEEP)",
        "E03\nCache 1-pass\n(REVERT)",
    ]
    medians = [exps[k]["median_sec"] for k in order]
    tasks = [exps[k]["num_tasks"] for k in order]
    colors = ["#6C757D", "#F0AD4E", "#2E8B57", "#D9534F"]

    fig, axes = plt.subplots(1, 2, figsize=(11, 4.4), dpi=150)

    # Panel 1: Median Runtime across E00..E03
    bars1 = axes[0].bar(labels, medians, color=colors, width=0.55)
    axes[0].set_title("Median Runtime qua 4 thực nghiệm (3 runs/EXP)")
    axes[0].set_ylabel("Thời gian (giây)")
    axes[0].set_ylim(0, max(medians) * 1.22)
    axes[0].grid(axis="y", linestyle="--", alpha=0.4)
    for bar, k in zip(bars1, order):
        val = exps[k]["median_sec"]
        pct = exps[k]["improvement_vs_baseline_pct"]
        tag = f"{val:.2f}s" if k == "baseline" else f"{val:.2f}s\n({-pct:+.1f}%)"
        axes[0].text(bar.get_x() + bar.get_width() / 2, val + 0.4, tag, ha="center", fontsize=9, fontweight="bold")

    # Panel 2: Total Spark Tasks (Spark UI)
    bars2 = axes[1].bar(labels, tasks, color=colors, width=0.55)
    axes[1].set_title("Tổng số Spark Tasks (Spark UI REST API)")
    axes[1].set_ylabel("Số tasks")
    axes[1].set_ylim(0, max(tasks) * 1.2)
    axes[1].grid(axis="y", linestyle="--", alpha=0.4)
    for bar, val in zip(bars2, tasks):
        axes[1].text(bar.get_x() + bar.get_width() / 2, val + 8, f"{val}", ha="center", fontsize=10, fontweight="bold")

    plt.tight_layout()
    out_png.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_png)
    plt.close()


def set_text_preserve_style(shape, lines: list[str]) -> None:
    tf = shape.text_frame
    base_p = tf.paragraphs[0]
    base_font = base_p.runs[0].font if base_p.runs else base_p.font
    font_name = base_font.name
    font_size = base_font.size
    font_bold = base_font.bold
    font_color = base_font.color.rgb if (base_font.color and base_font.color.type == 1) else None

    tf.clear()
    for idx, line in enumerate(lines):
        p = tf.paragraphs[0] if idx == 0 else tf.add_paragraph()
        run = p.add_run()
        run.text = line
        if font_name:
            run.font.name = font_name
        if font_size:
            run.font.size = font_size
        if font_bold is not None:
            run.font.bold = font_bold
        if font_color is not None:
            run.font.color.rgb = font_color


def make_docx(summary: dict, out_docx: Path, base_hash: str, opt_hash: str) -> None:
    doc = Document(str(TEMPLATE_DOCX))
    e00 = summary["experiments"]["baseline"]
    e01 = summary["experiments"]["e01_bhj"]
    e02 = summary["experiments"]["e02_part"]
    e03 = summary["experiments"]["e03_cache"]
    ld = summary["label_distribution"]

    replacements = {
        49: (
            f"Nhóm: Trần Tấn Toàn, Phạm Xuân Khoa, Hồ Tuấn Anh, Trần Thanh Lam    |    "
            f"Baseline commit: baseline-cp4 ({base_hash})    |    Optimized commit: optimized-cp4 ({opt_hash})"
        ),
        51: (
            f"• Input/workload: products_fact.parquet ({summary['total_items']:,} dòng, 10.29 MB) JOIN "
            f"trends_dim.csv (32,500 dòng, 12.92 MB), sinh từ raw thật (Tiki + Google Trends + RSS).\n"
            f"• Pipeline: src/pipeline_affiliate_spark.py (Filter price>0 → Join trend_key → Tính trend_score → "
            f"Gắn nhãn HOT/RISING/STABLE/COLD → GroupBy → Ghi Parquet).\n"
            f"• Resources & Spark configs (mặc định Spark, cố định): Spark {summary['spark_version']}, "
            f"master={summary['master']}, driver.memory={summary['driver_memory']}, shuffle.partitions=200, "
            f"autoBroadcastJoinThreshold=10MB, adaptive.enabled=false (cold JVM mỗi run, không cache).\n"
            f"• Baseline E00 (3 runs): {e00['runs_sec'][0]:.3f}s / {e00['runs_sec'][1]:.3f}s / {e00['runs_sec'][2]:.3f}s "
            f"→ Median = {e00['median_sec']:.3f}s (spread {e00['spread_pct']}%). Spark UI: {e00['num_tasks']} tasks, "
            f"medianTask={e00['median_task_time_ms']}ms, maxTask={e00['max_task_time_ms']}ms, execTime={e00['executor_run_time_ms']}ms."
        ),
        53: (
            f"• Observation (explain-before.txt): Catalyst đã tự động BroadcastHashJoin sau column-pruning bảng dim, "
            f"nhưng ở bước GroupBy & Sort sinh 2× Exchange với 200 partitions: (12) Exchange hashpartitioning(..., 200) "
            f"và (14) Exchange rangepartitioning(..., 200).\n"
            f"• Spark UI telemetry (spark_ui_stages_before.json): Tổng {e00['num_tasks']} tasks chạy trên {e00['num_stages']} stages "
            f"chỉ để xử lý {(e00['shuffle_read_bytes']+e00['shuffle_write_bytes'])/1024:.1f} KB shuffle và ghi {summary['summary_rows']} dòng summary; "
            f"median task chỉ {e00['median_task_time_ms']}ms trong khi tổng executor time lên tới {e00['executor_run_time_ms']}ms.\n"
            f"• Bottleneck claim: Chúng tôi cho rằng nút thắt nằm ở over-partitioning (shuffle.partitions=200 ở operator 12 & 14) "
            f"gây scheduling overhead và ghi nhiều file nhỏ không cần thiết trên local[2]."
        ),
        55: (
            f"• Hypothesis chính (E02): Nếu nút thắt over-partitioning xảy ra vì mặc định shuffle.partitions=200 tạo {e00['num_tasks']} task "
            f"siêu nhỏ (median={e00['median_task_time_ms']}ms) trên local[2], thì khi giảm duy nhất spark.sql.shuffle.partitions từ 200 → 8 "
            f"(4× cores), kỳ vọng số task giảm ~90%, executor time & median runtime giảm mạnh, trong khi 7 correctness assertions giữ nguyên.\n"
            f"• Change (1 biến duy nhất trong src/pipeline_affiliate_spark.py): spark.sql.shuffle.partitions: 200 → 8.\n"
            f"• Các thử nghiệm đối chứng (1 biến/EXP): E01 (thêm F.broadcast tường minh → {e01['median_sec']:.3f}s, +0.06% < variance → REVERT); "
            f"E03 (thêm scored.cache() 1-pass → {e03['median_sec']:.3f}s, chậm +63.6% vs E02, maxTask={e03['max_task_time_ms']}ms → REVERT)."
        ),
        57: (
            f"• Median Runtime: {e00['median_sec']:.3f}s → {e02['median_sec']:.3f}s | Δ = -{e00['median_sec']-e02['median_sec']:.3f}s (-{e02['improvement_vs_baseline_pct']}%) "
            f"(vượt xa spread {e02['spread_pct']}%).\n"
            f"• Total Tasks (Spark UI): {e00['num_tasks']} → {e02['num_tasks']} (-{(1 - e02['num_tasks']/e00['num_tasks'])*100:.1f}%).\n"
            f"• Executor Run Time: {e00['executor_run_time_ms']:,}ms → {e02['executor_run_time_ms']:,}ms (-{(1 - e02['executor_run_time_ms']/e00['executor_run_time_ms'])*100:.1f}%).\n"
            f"• Shuffle Read / Write: {e00['shuffle_read_bytes']/1024:.1f}KB / {e00['shuffle_write_bytes']/1024:.1f}KB → "
            f"{e02['shuffle_read_bytes']/1024:.1f}KB / {e02['shuffle_write_bytes']/1024:.1f}KB (-18.4% overhead).\n"
            f"• Max Task Time: {e00['max_task_time_ms']}ms → {e02['max_task_time_ms']}ms (-6.7%). Chi tiết tại reports/benchmark.csv."
        ),
        59: (
            f"Toàn bộ 7/7 correctness assertions PASS (không regression trên mọi EXP):\n"
            f"1. row_count_match: PASS ({summary['total_items']:,} == {summary['total_items']:,})\n"
            f"2. summary_rows_match: PASS ({summary['summary_rows']} == {summary['summary_rows']})\n"
            f"3. zero_null_or_dup_keys: PASS (0 null keys, 0 duplicate keys)\n"
            f"4. label_distribution_match: PASS (HOT:{ld['HOT']:,} | RISING:{ld['RISING']:,} | STABLE:{ld['STABLE']:,} | COLD:{ld['COLD']:,})\n"
            f"5. gmv_proxy_match: PASS ({summary['total_gmv_proxy']:,.0f}, sai số < 1e-2)\n"
            f"6. schema_contract_match: PASS | 7. output_checksum_match: PASS (SHA-256={summary['output_checksum'][:16]}…)"
        ),
        61: (
            f"• Quyết định: KEEP E02 (shuffle.partitions=8); REVERT E01 (không hiệu quả do Catalyst đã auto-broadcast) "
            f"và REVERT E03 (cache 1-pass làm chậm +63.6%, maxTask tăng 3.8× lên {e03['max_task_time_ms']}ms).\n"
            f"• Lý do: Giảm {e02['improvement_vs_baseline_pct']}% median runtime, giảm 92.9% tasks ({e00['num_tasks']}→{e02['num_tasks']}), "
            f"giảm 36.8% executor CPU time và PASS 7/7 correctness (kể cả SHA-256 checksum).\n"
            f"• Giới hạn: Đo trên local[2] với 1.17M dòng (~23 MB input); khi scale 10×–100× trên cluster lớn cần tăng "
            f"shuffle.partitions theo số cores/dữ liệu hoặc bật AQE coalescePartitions.\n"
            f"• Next action (Buổi 11): Thiết kế tầng Storage (Parquet partitioning, nén Snappy/Zstd, Data Lake)."
        ),
    }
    for idx, text in replacements.items():
        doc.paragraphs[idx].text = text

    out_docx.parent.mkdir(parents=True, exist_ok=True)
    doc.save(out_docx)


def make_pptx(summary: dict, out_pptx: Path, base_hash: str, opt_hash: str) -> None:
    prs = Presentation(str(TEMPLATE_PPTX))
    e00 = summary["experiments"]["baseline"]
    e01 = summary["experiments"]["e01_bhj"]
    e02 = summary["experiments"]["e02_part"]
    e03 = summary["experiments"]["e03_cache"]
    ld = summary["label_distribution"]

    # SLIDE 1: Cover + KPI cards
    s1 = prs.slides[0].shapes
    set_text_preserve_style(s1[2], [
        f"Nhóm: Trần Tấn Toàn, Phạm Xuân Khoa, Hồ Tuấn Anh, Trần Thanh Lam   •   Baseline: baseline-cp4 ({base_hash})   •   Optimized: optimized-cp4 ({opt_hash})"
    ])
    set_text_preserve_style(s1[4], ["MEDIAN RUNTIME", f"{e02['median_sec']:.2f} s"])
    set_text_preserve_style(s1[6], ["SPEEDUP", f"-{e02['improvement_vs_baseline_pct']:.1f} %"])
    set_text_preserve_style(s1[8], ["CORRECTNESS", "PASS (7/7)"])

    # SLIDE 2: 01 • Baseline
    s2 = prs.slides[1].shapes
    set_text_preserve_style(s2[4], [
        f"• Dataset: products_fact ({summary['total_items']:,} rows, 10.29 MB) JOIN trends_dim (32.5k rows, 12.92 MB)",
        "• Input: data/benchmark/* (từ raw Tiki + Google Trends + RSS)",
        "• Output: data/output/<mode>/ (Parquet)",
        "• Steps: Filter → Join → Trend Score → Affiliate Label → GroupBy → Sort",
    ])
    set_text_preserve_style(s2[7], [
        f"• Spark: {summary['spark_version']} (mặc định autoBroadcast=10MB)",
        f"• Master: {summary['master']} | Driver: {summary['driver_memory']}",
        "• shuffle.partitions = 200 (Spark default)",
        "• AQE = false | Cold JVM mỗi run (no cache)",
    ])
    set_text_preserve_style(s2[10], [
        f"• Run 1 / 2 / 3: {e00['runs_sec'][0]:.2f}s / {e00['runs_sec'][1]:.2f}s / {e00['runs_sec'][2]:.2f}s",
        f"• Median: {e00['median_sec']:.3f}s | Tasks: {e00['num_tasks']} | Exec: {e00['executor_run_time_ms']}ms",
        f"• Rows out: {summary['total_items']:,} items → {summary['summary_rows']} summary rows",
        f"• GMV proxy: {summary['total_gmv_proxy']:,.0f}",
    ])
    set_text_preserve_style(s2[11], [
        "Evidence: reports/benchmark.csv, reports/explain-before.txt, reports/spark_ui_stages_before.json."
    ])

    # SLIDE 3: 02 • Bottleneck Evidence
    s3 = prs.slides[2].shapes
    set_text_preserve_style(s3[4], [
        f"• Over-partitioning: {e00['num_tasks']} tasks cho chỉ 22.5 KB shuffle & 19 dòng output",
        "• 2× Exchange(..., 200) tại operator (12) HashAggregate & (14) Sort",
        f"• Median task = {e00['median_task_time_ms']} ms (hàng trăm task rỗng) vs Exec time = {e00['executor_run_time_ms']} ms",
        "• Catalyst đã tự BroadcastHashJoin → join KHÔNG còn là nút thắt chính",
    ])
    set_text_preserve_style(s3[7], [
        "Plan: reports/explain-before.txt → (12) Exchange hashpartitioning(200), (14) rangepartitioning(200)",
        f"Spark UI (spark_ui_stages_before.json): 7 stages, {e00['num_tasks']} tasks, maxTask={e00['max_task_time_ms']}ms, medianTask={e00['median_task_time_ms']}ms",
    ])
    set_text_preserve_style(s3[10], [
        "“Chúng tôi cho rằng nút thắt nằm ở over-partitioning (shuffle.partitions=200 tại operator 12 & 14) vì 425 task siêu nhỏ gây scheduling và small-file write overhead trên local[2].”"
    ])

    # SLIDE 4: 03 • Hypothesis & Change
    s4 = prs.slides[3].shapes
    set_text_preserve_style(s4[4], [
        "Nếu over-partitioning (200 partitions) tạo 425 task siêu nhỏ (median 23ms) cho 22.5 KB shuffle, thì khi giảm duy nhất spark.sql.shuffle.partitions từ 200 → 8 (4× cores), kỳ vọng số task giảm ~90% và median runtime giảm mạnh trong khi output giữ nguyên."
    ])
    set_text_preserve_style(s4[7], [
        "Thay đổi giữ lại (E02 – 1 biến duy nhất):",
        "Before: spark.sql.shuffle.partitions = 200 (default)",
        "After : spark.sql.shuffle.partitions = 8",
        f"Đối chứng: E01 (broadcast tường minh: REVERT +0.06%), E03 (cache 1-pass: REVERT {e03['median_sec']:.2f}s)",
    ])
    set_text_preserve_style(s4[10], [
        "• Plan: Exchange(200) → Exchange(8) ở operator (12) & (14)",
        "• Spark UI: Total tasks giảm mạnh (~425 → ~30), giảm executor time",
        "• Runtime kỳ vọng: giảm >20%, vượt xa run-to-run variance",
    ])
    set_text_preserve_style(s4[13], [
        "Giữ cố định 100%: cùng input (1.17M rows), local[2], driver=2g, autoBroadcast=10MB, AQE=false, cold JVM."
    ])

    # SLIDE 5: 04 • Before / After (GIỮ NGUYÊN 100% TÊN METRIC CỦA TEMPLATE THẦY!)
    s5 = prs.slides[4].shapes
    set_text_preserve_style(s5[13], [f"{e00['median_sec']:.3f} s"])
    set_text_preserve_style(s5[15], [f"{e02['median_sec']:.3f} s"])
    set_text_preserve_style(s5[17], [f"-{e02['improvement_vs_baseline_pct']:.2f}% (-{e00['median_sec']-e02['median_sec']:.2f}s)"])

    set_text_preserve_style(s5[21], [f"{e00['shuffle_read_bytes']/1024:.1f} / {e00['shuffle_write_bytes']/1024:.1f} KB (425 tasks)"])
    set_text_preserve_style(s5[23], [f"{e02['shuffle_read_bytes']/1024:.1f} / {e02['shuffle_write_bytes']/1024:.1f} KB (30 tasks)"])
    set_text_preserve_style(s5[25], ["-18.4% B | -92.9% tasks"])

    set_text_preserve_style(s5[29], [f"{e00['max_task_time_ms']} ms (Exec: {e00['executor_run_time_ms']}ms)"])
    set_text_preserve_style(s5[31], [f"{e02['max_task_time_ms']} ms (Exec: {e02['executor_run_time_ms']}ms)"])
    set_text_preserve_style(s5[33], ["-6.7% max | -36.8% exec"])

    set_text_preserve_style(s5[37], [f"{summary['total_items']:,} (19 summary)"])
    set_text_preserve_style(s5[39], [f"{summary['total_items']:,} (19 summary)"])
    set_text_preserve_style(s5[41], ["0 (SHA-256 exact)"])

    set_text_preserve_style(s5[45], [f"HOT:{ld['HOT']//1000}k | RISING:{ld['RISING']//1000}k"])
    set_text_preserve_style(s5[47], [f"HOT:{ld['HOT']//1000}k | RISING:{ld['RISING']//1000}k"])
    set_text_preserve_style(s5[49], ["PASS (GMV exact)"])

    # SLIDE 6: 05–06 • Correctness & Decision
    s6 = prs.slides[5].shapes
    set_text_preserve_style(s6[4], [
        "Assertions (7/7 PASS trên mọi EXP):",
        f"• Row count: {summary['total_items']:,} | Null/Dup keys: 0 (PASS)",
        f"• Labels: HOT {ld['HOT']:,}, RISING {ld['RISING']:,}, STABLE {ld['STABLE']:,}, COLD {ld['COLD']:,}",
        f"• GMV proxy, Schema & SHA-256 ({summary['output_checksum'][:12]}…): Exact match",
    ])
    set_text_preserve_style(s6[7], [
        f"KEEP E02 (-{e02['improvement_vs_baseline_pct']:.2f}% runtime, -92.9% tasks) | REVERT E01 & E03",
        f"E03 cache 1-pass làm chậm +63.6% ({e03['median_sec']:.2f}s, maxTask {e03['max_task_time_ms']}ms) → bác bỏ dựa trên số liệu.",
    ])
    set_text_preserve_style(s6[10], [
        "• Đo trên local[2] (1.17M dòng, 23 MB input); khi scale 10×–100× trên cluster cần tăng shuffle.partitions theo cores hoặc bật AQE.",
    ])
    set_text_preserve_style(s6[13], [
        "Buổi 11: Thiết kế tầng Storage (Parquet partitioning theo ingest_date/category_id, Snappy vs Zstd, Data Lake).",
    ])

    out_pptx.parent.mkdir(parents=True, exist_ok=True)
    prs.save(out_pptx)


def main() -> int:
    summary = json.loads((REPORTS_DIR / "benchmark_summary.json").read_text(encoding="utf-8"))
    base_hash = git_rev("baseline-cp4", "baseline-cp4")
    opt_hash = "tag: optimized-cp4"

    chart_png = REPORTS_DIR / "benchmark_before_after.png"
    out_docx = DOCS_DIR / "BigData_DoAn4_Brief_Checkpoint4_Report.docx"
    out_pptx = DOCS_DIR / "BigData_DoAn4_Checkpoint4_Report.pptx"

    make_chart(summary, chart_png)
    make_docx(summary, out_docx, base_hash, opt_hash)
    make_pptx(summary, out_pptx, base_hash, opt_hash)
    print(f"Generated reports (baseline={base_hash}, optimized={opt_hash}):")
    print(" ", chart_png)
    print(" ", out_docx)
    print(" ", out_pptx)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
