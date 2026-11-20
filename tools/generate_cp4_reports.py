"""Sinh biểu đồ benchmark + báo cáo Checkpoint 4 (DOCX & 6-slide PPTX)."""
from __future__ import annotations

import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from docx import Document
from pptx import Presentation
from pptx.util import Inches, Pt
from pptx.dml.color import RGBColor

PROJECT_ROOT = Path(__file__).resolve().parent.parent
REPORTS_DIR = PROJECT_ROOT / "reports"
DOCS_DIR = PROJECT_ROOT / "docs"


def make_chart(summary: dict, out_png: Path) -> None:
    b_runs = summary["baseline_runs_sec"]
    o_runs = summary["optimized_runs_sec"]
    b_med = summary["baseline_median_sec"]
    o_med = summary["optimized_median_sec"]

    fig, axes = plt.subplots(1, 2, figsize=(10, 4.2), dpi=150)

    # Panel 1: Per-run runtime
    x = [1, 2, 3]
    axes[0].plot(x, b_runs, marker="o", linewidth=2.2, color="#D9534F", label="Baseline (SortMergeJoin)")
    axes[0].plot(x, o_runs, marker="s", linewidth=2.2, color="#2E8B57", label="Optimized (BroadcastHashJoin)")
    axes[0].set_title("Runtime theo từng run (3 runs)")
    axes[0].set_xlabel("Run #")
    axes[0].set_ylabel("Thời gian (giây)")
    axes[0].set_xticks(x)
    axes[0].set_ylim(0, max(b_runs) * 1.2)
    axes[0].grid(True, linestyle="--", alpha=0.4)
    axes[0].legend()

    # Panel 2: Median comparison
    bars = axes[1].bar(
        ["Baseline\n(SortMergeJoin)", "Optimized\n(BroadcastHashJoin)"],
        [b_med, o_med],
        color=["#D9534F", "#2E8B57"],
        width=0.5,
    )
    axes[1].set_title(f"Median Runtime (-{summary['improvement_pct']}%)")
    axes[1].set_ylabel("Thời gian (giây)")
    axes[1].set_ylim(0, b_med * 1.25)
    axes[1].grid(axis="y", linestyle="--", alpha=0.4)
    for bar, val in zip(bars, [b_med, o_med]):
        axes[1].text(bar.get_x() + bar.get_width() / 2, val + 0.4, f"{val:.3f}s", ha="center", fontweight="bold")

    plt.tight_layout()
    out_png.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_png)
    plt.close()


def make_docx(summary: dict, chart_png: Path, out_docx: Path) -> None:
    template_docx = Path("/home/minwell0309/BigData_DoAn4_Brief_Checkpoint4_Template.docx")
    doc = Document(str(template_docx))

    b_r = summary["baseline_runs_sec"]
    o_r = summary["optimized_runs_sec"]
    b_med = summary["baseline_median_sec"]
    o_med = summary["optimized_median_sec"]
    imp = summary["improvement_pct"]

    replacements = {
        49: (
            "Nhóm: Trần Tấn Toàn, Phạm Xuân Khoa, Hồ Tuấn Anh, Trần Thanh Lam    |    "
            "Baseline commit: baseline-cp4 (4089894)    |    Optimized commit: checkpoint-4 (cb6d99f)"
        ),
        51: (
            f"• Input/workload: products_fact.parquet ({summary['total_items']:,} dòng, 5.74 MB) JOIN "
            f"category_trends.parquet (5 dòng, 3.85 KB), sinh từ dữ liệu raw thật (data/raw/ Tiki + Google Trends).\n"
            f"• Pipeline: src/pipeline_affiliate_spark.py (Filter price>0 → Join category_id → Tính trend_score → "
            f"Gắn nhãn affiliate HOT/RISING/STABLE/COLD → GroupBy → Ghi Parquet).\n"
            f"• Resources & Spark configs (cố định): Spark {summary['spark_version']}, master={summary['master']}, "
            f"driver.memory={summary['driver_memory']}, shuffle.partitions={summary['shuffle_partitions']}, "
            f"autoBroadcastJoinThreshold=-1, adaptive.enabled=false (cold JVM mỗi run, không cache).\n"
            f"• Kết quả Baseline (3 runs): Run 1 = {b_r[0]:.3f}s | Run 2 = {b_r[1]:.3f}s | Run 3 = {b_r[2]:.3f}s "
            f"→ Median = {b_med:.3f}s (GMV proxy = {summary['total_gmv_proxy']:,.0f})."
        ),
        53: (
            "• Observation: Execution plan baseline (reports/explain-before.txt, explain('formatted')) cho thấy "
            "toán tử join thực thi bằng SortMergeJoin [category_id], [category_id], Inner (operator 11).\n"
            "• Plan evidence: Trước khi join, Spark phát sinh 2× Exchange hashpartitioning(category_id, 64) "
            "(operator 4 & 9) và 2× Sort [category_id ASC] (operator 5 & 10) trên cả bảng fact lớn (879,000 dòng) "
            "lẫn bảng dim nhỏ (5 dòng).\n"
            "• Bottleneck claim: Chúng tôi cho rằng nút thắt nằm ở SortMergeJoin (operator 4–5–11) vì Spark phải "
            "shuffle và sort toàn bộ 879,000 dòng bảng fact chỉ để join với bảng dimension 5 dòng (3.85 KB)."
        ),
        55: (
            "• Hypothesis (If–Then): Nếu nút thắt SortMergeJoin xảy ra vì Spark phải shuffle và sort toàn bộ "
            "879,000 dòng bảng fact khi join với bảng category_trends rất nhỏ (5 dòng, 3.85 KB), thì khi áp dụng "
            "một thay đổi duy nhất F.broadcast(dim_df), chúng tôi kỳ vọng execution plan chuyển sang BroadcastHashJoin "
            "(loại bỏ Exchange & Sort trên bảng fact) và giảm median runtime, trong khi mọi correctness metrics giữ nguyên.\n"
            "• Change (1 biến duy nhất trong src/pipeline_affiliate_spark.py):\n"
            "   - Before: fact_df.join(dim_df, on='category_id', how='inner')\n"
            "   - After : fact_df.join(F.broadcast(dim_df), on='category_id', how='inner')\n"
            "• Control: Giữ cố định 100% input, tài nguyên (local[2], 2g), shuffle.partitions=64, AQE=false, cold JVM."
        ),
        57: (
            f"• Runtime Baseline (3 runs): {b_r[0]:.3f}s, {b_r[1]:.3f}s, {b_r[2]:.3f}s → Median: {b_med:.3f}s (spread ~3.8%)\n"
            f"• Runtime After    (3 runs): {o_r[0]:.3f}s, {o_r[1]:.3f}s, {o_r[2]:.3f}s → Median: {o_med:.3f}s (spread ~1.2%)\n"
            f"• Cải thiện (Δ): -{b_med - o_med:.3f}s (-{imp:.2f}%), vượt xa độ biến động giữa các run.\n"
            f"• Execution signal (reports/explain-after.txt): SortMergeJoin (2× Exchange + 2× Sort) → "
            f"BroadcastHashJoin + BroadcastExchange (0 Exchange/Sort trên bảng fact).\n"
            f"• Chi tiết lưu tại: reports/benchmark.csv, reports/experiment-log.md, reports/benchmark_before_after.png."
        ),
        59: (
            f"Bộ 5 correctness assertions đều PASS (không regression):\n"
            f"1. row_count_match: PASS ({summary['total_items']:,} == {summary['total_items']:,})\n"
            f"2. summary_rows_match: PASS (19 == 19 dòng bảng tổng hợp)\n"
            f"3. label_distribution_match: PASS (HOT: 267,000 | RISING: 285,000 | STABLE: 123,000 | COLD: 204,000)\n"
            f"4. gmv_proxy_match: PASS ({summary['total_gmv_proxy']:,.0f}, chênh lệch < 1e-2)\n"
            f"5. schema_contract_match: PASS (schema output giữ nguyên hoàn toàn)"
        ),
        61: (
            f"• Quyết định: KEEP thay đổi E01 (F.broadcast(dim_df)).\n"
            f"• Lý do: Giảm median runtime {imp:.2f}% (ổn định qua 3 runs, vượt xa variance), xác nhận đúng tín hiệu "
            f"BroadcastHashJoin trên explain-after.txt và PASS 5/5 correctness assertions.\n"
            f"• Giới hạn: Đo trên local[2] với 879k dòng (5.74 MB) nên thời gian khởi tạo JVM chiếm tỷ trọng lớn; "
            f"chỉ áp dụng broadcast() khi bảng dimension đủ nhỏ so với bộ nhớ driver/executor để tránh OOM.\n"
            f"• Next action (Buổi 11): Thiết kế tầng Storage (Parquet partitioning theo category_id/ingest_date, Data Lake)."
        ),
    }

    for idx, text in replacements.items():
        doc.paragraphs[idx].text = text

    out_docx.parent.mkdir(parents=True, exist_ok=True)
    doc.save(out_docx)


def set_text_preserve_style(shape, lines: list[str]) -> None:
    """Thay nội dung text_frame nhưng giữ nguyên font/color/size của template."""
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


def make_pptx(summary: dict, chart_png: Path, out_pptx: Path) -> None:
    template_path = Path("/home/minwell0309/BigData_DoAn4_Checkpoint4_Report_Template.pptx")
    prs = Presentation(str(template_path))

    b_r = summary["baseline_runs_sec"]
    o_r = summary["optimized_runs_sec"]
    b_med = summary["baseline_median_sec"]
    o_med = summary["optimized_median_sec"]
    imp = summary["improvement_pct"]

    # SLIDE 1: Cover + KPI cards
    s1 = prs.slides[0].shapes
    set_text_preserve_style(s1[2], [
        "Nhóm: Trần Tấn Toàn, Phạm Xuân Khoa, Hồ Tuấn Anh, Trần Thanh Lam   •   Baseline: baseline-cp4   •   Optimized: checkpoint-4 (4089894)"
    ])
    set_text_preserve_style(s1[4], ["MEDIAN RUNTIME", f"{o_med:.2f} s"])
    set_text_preserve_style(s1[6], ["SPEEDUP", f"-{imp:.2f} %"])
    set_text_preserve_style(s1[8], ["CORRECTNESS", "PASS (5/5)"])

    # SLIDE 2: 01 • Baseline
    s2 = prs.slides[1].shapes
    set_text_preserve_style(s2[4], [
        "• Dataset: products_fact (879,000 rows, 5.74 MB) JOIN category_trends (5 rows, 3.85 KB)",
        "• Input: data/benchmark/*.parquet (từ data/raw/ Tiki + Google Trends)",
        "• Output: data/output/<mode>/ (Parquet)",
        "• Steps: Filter(price>0) → Join(category_id) → Trend Score → Affiliate Label → GroupBy",
    ])
    set_text_preserve_style(s2[7], [
        f"• Spark: {summary['spark_version']} (PySpark)",
        f"• Master: {summary['master']} | Driver: {summary['driver_memory']}",
        f"• shuffle.partitions = {summary['shuffle_partitions']}",
        "• autoBroadcastJoinThreshold = -1 | AQE = false (cold JVM mỗi run)",
    ])
    set_text_preserve_style(s2[10], [
        f"• Run 1 / 2 / 3: {b_r[0]:.3f}s / {b_r[1]:.3f}s / {b_r[2]:.3f}s",
        f"• Median runtime: {b_med:.3f}s",
        f"• Rows processed: {summary['total_items']:,} (Summary: 19 rows)",
        f"• GMV proxy: {summary['total_gmv_proxy']:,.0f}",
    ])
    set_text_preserve_style(s2[11], [
        "Evidence: reports/benchmark.csv, reports/explain-before.txt, tag baseline-cp4."
    ])

    # SLIDE 3: 02 • Bottleneck Evidence
    s3 = prs.slides[2].shapes
    set_text_preserve_style(s3[4], [
        "• Physical Join là SortMergeJoin [category_id]",
        "• 2× Exchange hashpartitioning(category_id, 64)",
        "• 2× Sort [category_id ASC] trước khi join",
        "• Shuffle toàn bộ 879,000 dòng fact chỉ để join bảng dim 5 dòng (3.85 KB)",
    ])
    set_text_preserve_style(s3[7], [
        "File: reports/explain-before.txt (explain('formatted'))",
        "Stage/Operator: (4) Exchange hashpartitioning(64) → (5) Sort → (11) SortMergeJoin Inner",
        "Cả 2 nhánh products_fact (879k) và category_trends (5) đều bị shuffle & sort.",
    ])
    set_text_preserve_style(s3[10], [
        "“Chúng tôi cho rằng nút thắt nằm ở SortMergeJoin (operator 4-5-11) vì Spark phải shuffle và sort toàn bộ 879,000 dòng bảng fact thay vì broadcast bảng dim 5 dòng.”"
    ])

    # SLIDE 4: 03 • Hypothesis & Change
    s4 = prs.slides[3].shapes
    set_text_preserve_style(s4[4], [
        "Nếu SortMergeJoin gây shuffle + sort toàn bộ 879,000 dòng bảng fact vì không broadcast bảng dim nhỏ (5 dòng), thì khi bọc F.broadcast(dim_df), chúng tôi kỳ vọng plan chuyển sang BroadcastHashJoin và giảm median runtime trong khi output giữ nguyên."
    ])
    set_text_preserve_style(s4[7], [
        "File: src/pipeline_affiliate_spark.py (1 thay đổi duy nhất)",
        "Before: fact_df.join(dim_df, on='category_id', how='inner')",
        "After : fact_df.join(F.broadcast(dim_df), on='category_id', how='inner')",
    ])
    set_text_preserve_style(s4[10], [
        "• Plan: SortMergeJoin → BroadcastHashJoin + BroadcastExchange",
        "• Loại bỏ Exchange (4) và Sort (5) trên bảng fact 879k dòng",
        "• Runtime kỳ vọng: giảm rõ rệt (>10%), vượt run-to-run variance",
    ])
    set_text_preserve_style(s4[13], [
        "Giữ cố định 100%: cùng input (879k rows), local[2], driver=2g, shuffle.partitions=64, AQE=false, cold JVM."
    ])

    # SLIDE 5: 04 • Before / After
    s5 = prs.slides[4].shapes
    set_text_preserve_style(s5[13], [f"{b_med:.3f} s"])
    set_text_preserve_style(s5[15], [f"{o_med:.3f} s"])
    set_text_preserve_style(s5[17], [f"-{imp:.2f}% (-{b_med - o_med:.3f}s)"])

    set_text_preserve_style(s5[19], ["Join Strategy / Shuffle"])
    set_text_preserve_style(s5[21], ["SortMergeJoin (2× Exchange)"])
    set_text_preserve_style(s5[23], ["BroadcastHashJoin (0 fact Exchange)"])
    set_text_preserve_style(s5[25], ["Bỏ shuffle & sort fact"])

    set_text_preserve_style(s5[27], ["Run-to-run spread"])
    set_text_preserve_style(s5[29], ["15.831s – 16.445s (~3.8%)"])
    set_text_preserve_style(s5[31], ["13.302s – 13.467s (~1.2%)"])
    set_text_preserve_style(s5[33], ["Ổn định hơn"])

    set_text_preserve_style(s5[37], [f"{summary['total_items']:,}"])
    set_text_preserve_style(s5[39], [f"{summary['total_items']:,}"])
    set_text_preserve_style(s5[41], ["0 (Exact match)"])

    set_text_preserve_style(s5[45], ["HOT:267k | RISING:285k"])
    set_text_preserve_style(s5[47], ["HOT:267k | RISING:285k"])
    set_text_preserve_style(s5[49], ["PASS (GMV exact)"])

    # SLIDE 6: 05–06 • Correctness & Decision
    s6 = prs.slides[5].shapes
    set_text_preserve_style(s6[4], [
        "Assertions (5/5 PASS):",
        f"• Row count: {summary['total_items']:,} == {summary['total_items']:,} (PASS)",
        "• Label dist: HOT 267k, RISING 285k, STABLE 123k, COLD 204k (PASS)",
        "• GMV proxy & Schema contract: Exact match (PASS)",
    ])
    set_text_preserve_style(s6[7], [
        f"KEEP (E01 – broadcast dim_df)",
        f"Giảm median runtime {imp:.2f}% (vượt xa variance ~1.2–3.8%), plan xác nhận BroadcastHashJoin, 5/5 correctness PASS.",
    ])
    set_text_preserve_style(s6[10], [
        "• Chạy local[2], dataset 5.74 MB nên JVM startup chiếm phần lớn runtime.",
        "• Chỉ dùng broadcast() khi bảng dim nhỏ hơn ngưỡng bộ nhớ để tránh OOM.",
    ])
    set_text_preserve_style(s6[13], [
        "Buổi 11: Thiết kế tầng Storage (Parquet partitioning theo category_id/ingest_date, Data Lake) để tối ưu đọc/ghi end-to-end.",
    ])

    out_pptx.parent.mkdir(parents=True, exist_ok=True)
    prs.save(out_pptx)


def main() -> int:
    summary = json.loads((REPORTS_DIR / "benchmark_summary.json").read_text(encoding="utf-8"))
    chart_png = REPORTS_DIR / "benchmark_before_after.png"
    out_docx = DOCS_DIR / "BigData_DoAn4_Brief_Checkpoint4_Report.docx"
    out_pptx = DOCS_DIR / "BigData_DoAn4_Checkpoint4_Report.pptx"

    make_chart(summary, chart_png)
    make_docx(summary, chart_png, out_docx)
    make_pptx(summary, chart_png, out_pptx)
    print("Generated:")
    print(" ", chart_png)
    print(" ", out_docx)
    print(" ", out_pptx)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
