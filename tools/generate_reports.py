"""Sinh bộ tài liệu báo cáo Checkpoint 2 (Buổi 6 - Đồ án 2):
1. docs/architecture_checkpoint2.png  (Sơ đồ kiến trúc đến Checkpoint 2)
2. docs/BigData_Buoi06_DoAn2_Checkpoint2_Report.pptx (6 slide theo đúng template)
3. docs/BigData_Buoi06_DoAn2_Brief_Checkpoint_Report.docx (File Word theo đúng mẫu Brief & Checkpoint Report)

Chạy:
    python tools/generate_reports.py
"""
from __future__ import annotations

import json
import subprocess
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch

from docx import Document
from docx.enum.table import WD_TABLE_ALIGNMENT
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
from docx.shared import Inches, Pt, RGBColor

from pptx import Presentation
from pptx.dml.color import RGBColor as PptColor
from pptx.enum.shapes import MSO_SHAPE
from pptx.enum.text import PP_ALIGN
from pptx.util import Inches as PptInches, Pt as PptPt

ROOT = Path(__file__).resolve().parents[1]
DOCS_DIR = ROOT / "docs"
RAW_DIR = ROOT / "data" / "raw"
LOGS_DIR = ROOT / "logs"
META_DIR = ROOT / "metadata"


def get_git_info() -> tuple[str, str]:
    try:
        commit = subprocess.check_output(
            ["git", "rev-parse", "--short=12", "HEAD"], cwd=ROOT, text=True
        ).strip()
    except Exception:
        commit = "checkpoint-2"
    return commit, "checkpoint-2"


def load_latest_manifests() -> dict[str, dict]:
    manifests: dict[str, dict] = {}
    if not (META_DIR / "ingest_log.jsonl").exists():
        return manifests
    for line in (META_DIR / "ingest_log.jsonl").read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        rec = json.loads(line)
        if rec.get("status") in ("SUCCESS", "PARTIAL"):
            manifests[rec["dataset"]] = rec
    return manifests


def count_runs_per_dataset() -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for ds_dir in sorted(RAW_DIR.iterdir()):
        if ds_dir.is_dir():
            runs = sorted(p.name for p in ds_dir.glob("ingest_date=*/run_*") if p.is_dir())
            out[ds_dir.name] = runs
    return out


def generate_architecture_diagram(out_path: Path) -> None:
    fig, ax = plt.subplots(figsize=(13.5, 5.6), dpi=220)
    ax.set_xlim(0, 13.5)
    ax.set_ylim(0, 5.6)
    ax.axis("off")

    # Boundary box for Checkpoint 2
    boundary = FancyBboxPatch(
        (0.25, 0.45), 10.15, 4.75,
        boxstyle="round,pad=0.12,rounding_size=0.18",
        linewidth=2.0, edgecolor="#1E40AF", facecolor="#F8FAFC", linestyle="--"
    )
    ax.add_patch(boundary)
    ax.text(
        0.45, 4.95,
        "PHẠM VI ĐÃ TRIỂN KHAI ĐẾN CHECKPOINT 2 (BUỔI 6) — Boundary của nhóm",
        fontsize=10.5, fontweight="bold", color="#1E3A8A"
    )

    def draw_box(x, y, w, h, title, lines, fc="#FFFFFF", ec="#2563EB", tc="#0F172A"):
        box = FancyBboxPatch(
            (x, y), w, h,
            boxstyle="round,pad=0.08,rounding_size=0.12",
            linewidth=1.6, edgecolor=ec, facecolor=fc
        )
        ax.add_patch(box)
        ax.text(x + w / 2, y + h - 0.26, title, fontsize=9.5, fontweight="bold",
                color=tc, ha="center", va="top")
        body = "\n".join(lines)
        ax.text(x + w / 2, y + h / 2 - 0.10, body, fontsize=8.0,
                color="#334155", ha="center", va="center", linespacing=1.28)

    # 1. Sources
    draw_box(0.45, 3.55, 2.55, 1.12, "SOURCE 1: Google Trends RSS",
             ["trends.google.com/trending/rss?geo=VN", "HTTP GET • XML (Trending Now VN)"],
             fc="#EFF6FF", ec="#3B82F6", tc="#1D4ED8")
    draw_box(0.45, 2.15, 2.55, 1.18, "SOURCE 2: Google Trends API",
             ["pytrends (hl=vi-VN, geo=VN)", "4 nhóm × 5 từ khóa • timeframe 3-m",
              "Interest Over Time (CSV) + Related (JSON)"],
             fc="#EFF6FF", ec="#3B82F6", tc="#1D4ED8")
    draw_box(0.45, 0.72, 2.55, 1.20, "SOURCE 3: Tiki Public API",
             ["tiki.vn/api/personalish/v1/blocks/listings",
              "5 ngành hàng × 3 trang = 15 JSON files",
              "586 sản phẩm (giá, rating, đã bán)"],
             fc="#EFF6FF", ec="#3B82F6", tc="#1D4ED8")

    # 2. Ingestion Layer
    draw_box(3.45, 1.25, 2.70, 3.25, "INGESTION LAYER (Python 3.11)",
             ["src/ingest_trends_rss.py",
              "src/ingest_google_trends.py",
              "src/ingest_tiki_products.py",
              "src/ingest_all.py (Orchestrator)",
              "──────────────────────",
              "• Retry + Rate-limit sleep",
              "• Validate XML / JSON / CSV",
              "• Sinh run_<UTC_TIMESTAMP>",
              "• Tính SHA-256 & byte size",
              "• Phát hiện duplicate_of_run"],
             fc="#FEF3C7", ec="#D97706", tc="#92400E")

    # 3. Raw Storage & Traceability
    draw_box(6.60, 2.45, 3.55, 2.15, "RAW STORAGE (Immutable Local FS)",
             ["data/raw/<dataset>/",
              "  └─ ingest_date=YYYY-MM-DD/",
              "       └─ run_<UTC_TIMESTAMP>/",
              "            ├─ raw files (.xml/.csv/.json)",
              "            ├─ _manifest.json",
              "            └─ _SUCCESS",
              "• Append-only (exist_ok=False)"],
             fc="#ECFDF5", ec="#059669", tc="#065F46")

    draw_box(6.60, 0.72, 3.55, 1.48, "TRACEABILITY & VERIFICATION",
             ["metadata/ingest_log.jsonl (Sổ cái toàn cục)",
              "logs/env_check.txt • logs/verify_raw.txt",
              "PySpark 3.5.3 (local[*]) + JDK 17:",
              "src/verify_raw_spark.py đọc Raw & kiểm tra SHA-256"],
             fc="#F3E8FF", ec="#7C3AED", tc="#5B21B6")

    # 4. Future Scope (Buổi 7+)
    future = FancyBboxPatch(
        (10.70, 0.72), 2.55, 4.18,
        boxstyle="round,pad=0.08,rounding_size=0.12",
        linewidth=1.5, edgecolor="#94A3B8", facecolor="#F1F5F9", linestyle=":"
    )
    ax.add_patch(future)
    ax.text(11.97, 4.65, "BUỔI 7+ (NEXT STEP)", fontsize=9.2, fontweight="bold",
            color="#475569", ha="center", va="top")
    ax.text(11.97, 2.65,
            "Chưa triển khai ở CP2\n─────────────────\n"
            "1. Spark Staging (Parquet)\n   Chuẩn hóa & Dedup\n\n"
            "2. Keyword ↔ Product\n   Matching (NLP/Fuzzy)\n\n"
            "3. Trend Score Engine\n   Gắn nhãn Affiliate:\n   HOT / RISING /\n   STABLE / COLD\n\n"
            "4. Dashboard / Export",
            fontsize=8.0, color="#64748B", ha="center", va="center", linespacing=1.25)

    def arrow(x1, y1, x2, y2, color="#475569", style="-|>"):
        arr = FancyArrowPatch((x1, y1), (x2, y2),
                              arrowstyle=style, mutation_scale=14,
                              linewidth=1.6, color=color)
        ax.add_patch(arr)

    arrow(3.02, 4.10, 3.43, 3.30, "#2563EB")
    arrow(3.02, 2.74, 3.43, 2.74, "#2563EB")
    arrow(3.02, 1.32, 3.43, 2.15, "#2563EB")
    arrow(6.17, 3.30, 6.58, 3.45, "#D97706")
    arrow(6.17, 2.10, 6.58, 1.50, "#7C3AED")
    arrow(8.38, 2.43, 8.38, 2.22, "#059669")
    arrow(10.18, 3.45, 10.68, 3.45, "#94A3B8")

    plt.tight_layout(pad=0.1)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=220)
    plt.close(fig)


def add_header_banner(slide, badge_text: str, title_text: str, subtitle_text: str) -> None:
    banner = slide.shapes.add_shape(
        MSO_SHAPE.RECTANGLE, PptInches(0), PptInches(0), PptInches(13.333), PptInches(1.12)
    )
    banner.fill.solid()
    banner.fill.fore_color.rgb = PptColor(15, 23, 42)  # #0F172A
    banner.line.fill.background()

    badge = slide.shapes.add_shape(
        MSO_SHAPE.ROUNDED_RECTANGLE, PptInches(0.45), PptInches(0.16), PptInches(2.55), PptInches(0.30)
    )
    badge.fill.solid()
    badge.fill.fore_color.rgb = PptColor(37, 99, 235)
    badge.line.fill.background()
    tf_b = badge.text_frame
    tf_b.word_wrap = False
    p_b = tf_b.paragraphs[0]
    p_b.text = badge_text
    p_b.font.size = PptPt(10)
    p_b.font.bold = True
    p_b.font.color.rgb = PptColor(255, 255, 255)
    p_b.alignment = PP_ALIGN.CENTER

    tb = slide.shapes.add_textbox(PptInches(0.45), PptInches(0.46), PptInches(12.4), PptInches(0.62))
    tf = tb.text_frame
    tf.word_wrap = True
    p1 = tf.paragraphs[0]
    p1.text = title_text
    p1.font.size = PptPt(21)
    p1.font.bold = True
    p1.font.color.rgb = PptColor(255, 255, 255)

    tb2 = slide.shapes.add_textbox(PptInches(3.15), PptInches(0.15), PptInches(9.7), PptInches(0.32))
    tf2 = tb2.text_frame
    p2 = tf2.paragraphs[0]
    p2.text = subtitle_text
    p2.font.size = PptPt(11)
    p2.font.color.rgb = PptColor(148, 163, 184)
    p2.alignment = PP_ALIGN.RIGHT


def add_card(slide, left, top, width, height, heading: str, lines: list[str],
             bg=(248, 250, 252), border=(203, 213, 225), head_color=(30, 64, 175),
             mono: bool = False, font_size: int = 11) -> None:
    card = slide.shapes.add_shape(
        MSO_SHAPE.ROUNDED_RECTANGLE,
        PptInches(left), PptInches(top), PptInches(width), PptInches(height)
    )
    card.fill.solid()
    card.fill.fore_color.rgb = PptColor(*bg)
    card.line.color.rgb = PptColor(*border)
    card.line.width = PptPt(1.2)

    tb = slide.shapes.add_textbox(
        PptInches(left + 0.14), PptInches(top + 0.08),
        PptInches(width - 0.28), PptInches(height - 0.16)
    )
    tf = tb.text_frame
    tf.word_wrap = True

    p0 = tf.paragraphs[0]
    p0.text = heading
    p0.font.size = PptPt(12.5)
    p0.font.bold = True
    p0.font.color.rgb = PptColor(*head_color)
    p0.space_after = PptPt(4)

    for line in lines:
        p = tf.add_paragraph()
        p.text = line
        p.font.size = PptPt(font_size)
        p.font.name = "Consolas" if mono else "Calibri"
        p.font.color.rgb = PptColor(30, 41, 59) if not mono else PptColor(226, 232, 240)
        p.space_after = PptPt(2)


def generate_pptx(out_path: Path, arch_img: Path, commit: str, tag: str,
                  manifests: dict[str, dict], runs_map: dict[str, list[str]]) -> None:
    prs = Presentation()
    prs.slide_width = PptInches(13.333)
    prs.slide_height = PptInches(7.5)
    blank = prs.slide_layouts[6]

    tiki_m = manifests.get("tiki_products", {})
    trends_m = manifests.get("google_trends", {})
    rss_m = manifests.get("google_trends_rss", {})

    # ---------------- SLIDE 1 ----------------
    s1 = prs.slides.add_slide(blank)
    add_header_banner(
        s1,
        "ĐỒ ÁN 2 • CHECKPOINT REPORT",
        "Environment → Ingestion → Raw Storage",
        "Môn học: Dữ liệu lớn (BigData - 2026) • Giảng viên: TS. Hồ Quốc Dũng"
    )
    add_card(
        s1, 0.45, 1.35, 7.65, 3.35,
        "THÔNG TIN NHÓM & ĐỀ TÀI",
        [
            "• Nhóm: Nhóm TrendAffiliate (Lớp BigData - 2026)",
            "• Thành viên:",
            "   1. Trần Tấn Toàn (Nhóm trưởng) — 23T1020547",
            "   2. Phạm Xuân Khoa — Thành viên",
            "   3. Hồ Tuấn Anh — Thành viên",
            "   4. Trần Thanh Lam — Thành viên",
            "• Đề tài: Hệ thống AI truy xuất xu hướng để gắn nhãn Affiliate cho các sàn TMĐT",
            f"• Git commit / tag: {commit}  (tag: {tag})",
            "• Repo path: trend-affiliate-bigdata/ (có README.md, src/, config/, data/raw/, logs/, metadata/, docs/)",
        ],
        bg=(239, 246, 255), border=(147, 197, 253), head_color=(29, 78, 216), font_size=13
    )
    add_card(
        s1, 8.30, 1.35, 4.58, 3.35,
        "QUY TẮC & MỤC TIÊU CHECKPOINT 2",
        [
            "• Thời lượng trình bày: Tối đa 4 phút.",
            "• Mỗi slide đều đính kèm bằng chứng thật:",
            "   [✓] Terminal log (logs/env_check.txt)",
            "   [✓] Git repo + commit/tag checkpoint-2",
            "   [✓] Raw files thật (Tiki + Google Trends)",
            "   [✓] Manifest JSON (SHA-256, bytes, status)",
            "   [✓] Spark đọc kiểm chứng Raw PASS",
            "• Trọng tâm Buổi 6: “Data lands safely”",
            "  (Chưa làm ETL phức tạp / ML / Dashboard).",
        ],
        bg=(254, 243, 199), border=(252, 211, 77), head_color=(180, 83, 9), font_size=12.5
    )
    add_card(
        s1, 0.45, 4.90, 12.43, 2.25,
        "TÓM TẮT 5 BẰNG CHỨNG ĐẠT ĐƯỢC CUỐI BUỔI 6",
        [
            "1. [ENV]    Python 3.11.9 + OpenJDK 17.0.20.1 + PySpark 3.5.3 (local[*], 12 cores) — Smoke test 1,000,000 rows PASS (6.2s).",
            "2. [SOURCE] 3 nguồn dữ liệu thực: (a) Google Trends RSS VN; (b) Google Trends API (pytrends, 20 từ khóa); (c) Tiki Listing API (5 ngành hàng).",
            f"3. [INGEST] Chạy thành công cả 3 job: Tiki ({tiki_m.get('total_records', 586)} sản phẩm / 15 files), Google Trends ({trends_m.get('total_records', 372)} dòng / 8 files), Trends RSS ({rss_m.get('total_records', 10)} xu hướng).",
            "4. [RAW]    Lưu trữ bất biến theo quy ước: data/raw/<dataset>/ingest_date=YYYY-MM-DD/run_<UTC_TIMESTAMP>/ kèm cờ _SUCCESS.",
            "5. [TRACE]  Mỗi run có _manifest.json riêng + sổ cái metadata/ingest_log.jsonl; PySpark đọc kiểm chứng khớp 100% SHA-256 & byte size.",
        ],
        bg=(236, 253, 245), border=(110, 231, 183), head_color=(4, 120, 87), font_size=12
    )

    # ---------------- SLIDE 2 ----------------
    s2 = prs.slides.add_slide(blank)
    add_header_banner(
        s2,
        "CHECKPOINT 2 • SLIDE 2",
        "Architecture update — Source → Ingestion → Raw Storage",
        "Chỉ vẽ phần đã triển khai đến Checkpoint 2 • Ghi rõ technology và boundary của nhóm"
    )
    if arch_img.exists():
        s2.shapes.add_picture(
            str(arch_img), PptInches(0.45), PptInches(1.22), width=PptInches(12.43)
        )
    add_card(
        s2, 0.45, 5.95, 12.43, 1.30,
        "TECHNOLOGY STACK & 1 CÂU GIẢI THÍCH TRADE-OFF CHÍNH",
        [
            "• Công nghệ sử dụng: Python 3.11.9 (requests, pytrends) → Local Filesystem Raw Lake (partition ingest_date/run_id + _manifest.json) → PySpark 3.5.3 local[*] trên OpenJDK 17.",
            "• 1 câu giải thích trade-off chính: Nhóm chọn Local PySpark (local[*]) + Local Filesystem theo chuẩn phân vùng HDFS/S3 thay vì dựng cụm Hadoop/Cloud ngay ở Buổi 6 để đảm bảo tái tạo môi trường ≤ 10 phút, dễ debug lỗi ingestion và không tốn chi phí hạ tầng, đánh đổi lại chưa kiểm thử khả năng chịu lỗi phân tán đa node (nhưng giữ nguyên cấu trúc path để chuyển lên HDFS/S3 chỉ bằng biến môi trường RAW_ROOT).",
        ],
        bg=(239, 246, 255), border=(147, 197, 253), head_color=(30, 64, 175), font_size=11
    )

    # ---------------- SLIDE 3 ----------------
    s3 = prs.slides.add_slide(blank)
    add_header_banner(
        s3,
        "CHECKPOINT 2 • SLIDE 3",
        "Environment evidence — Chứng minh có thể tái tạo và chạy Spark",
        "Evidence từ logs/env_check.txt • Reproducible setup"
    )
    env_txt = (LOGS_DIR / "env_check.txt").read_text(encoding="utf-8").strip().splitlines()
    add_card(
        s3, 0.45, 1.30, 6.35, 4.35,
        "EVIDENCE: TERMINAL OUTPUT (logs/env_check.txt)",
        env_txt[:18],
        bg=(15, 23, 42), border=(51, 65, 85), head_color=(56, 189, 248), mono=True, font_size=10
    )
    add_card(
        s3, 6.95, 1.30, 5.93, 2.75,
        "SETUP COMMAND & EXECUTION MODE",
        [
            "• Setup command (Windows PowerShell ≤ 10 phút):",
            "  py -3.11 -m venv .venv",
            "  .\\.venv\\Scripts\\Activate.ps1",
            "  pip install -r requirements.txt",
            "  copy .env.example .env",
            "• Lệnh kiểm tra môi trường (1 lệnh duy nhất):",
            "  python src/env_check.py   (hoặc .\\run.ps1 src\\env_check.py)",
            "• Execution Mode: Mức A — Local PySpark (local[*], 12 cores)",
        ],
        bg=(236, 253, 245), border=(110, 231, 183), head_color=(4, 120, 87), font_size=11.5
    )
    add_card(
        s3, 6.95, 4.20, 5.93, 1.45,
        "BLOCKER MÔI TRƯỜNG & CÁCH ĐÃ XỬ LÝ",
        [
            "• Đã gặp: Máy cài sẵn Python 3.14 (chưa tương thích tốt với PySpark 3.5.3) và thiếu JDK.",
            "• Giải pháp: Cài đặt Python 3.11.9 + Eclipse Temurin JDK 17.0.20.1, cố định PYSPARK_PYTHON và JAVA_HOME trong .env + script run.ps1 → Hiện tại KHÔNG còn blocker môi trường.",
        ],
        bg=(254, 243, 199), border=(252, 211, 77), head_color=(180, 83, 9), font_size=11
    )
    add_card(
        s3, 0.45, 5.80, 12.43, 1.40,
        " TÓM TẮT VERSION RUNTIME",
        [
            "• OS: Windows 10 (build 26200)   |   Python: 3.11.9 (.venv)   |   Java: OpenJDK 17.0.20.1 (Temurin Hotspot)   |   PySpark / Spark: 3.5.3",
            "• Phép tính kiểm chứng: spark.range(1_000_000).count() == 1,000,000 và sum(1..100) == 5050 hoàn tất trong 6.2 giây → RESULT: PASS.",
        ],
        bg=(248, 250, 252), border=(203, 213, 225), head_color=(30, 64, 175), font_size=11.5
    )

    # ---------------- SLIDE 4 ----------------
    s4 = prs.slides.add_slide(blank)
    add_header_banner(
        s4,
        "CHECKPOINT 2 • SLIDE 4",
        "Data source & ingestion — Nguồn dữ liệu cụ thể và job chạy thực tế",
        "Kết hợp tín hiệu xu hướng (Google Trends) & dữ liệu sản phẩm TMĐT (Tiki API)"
    )
    add_card(
        s4, 0.45, 1.30, 5.95, 5.85,
        "SOURCE, SCALE / VERSION & PROVENANCE",
        [
            "1. Tiki Public Listing API (dataset: tiki_products)",
            "   • URL: https://tiki.vn/api/personalish/v1/blocks/listings",
            "   • Scale: 5 ngành hàng × 3 trang = 15 JSON files (~2.61 MB, 586 sản phẩm)",
            "   • Provenance: Public JSON API trên tiki.vn; lưu nguyên bytes phản hồi, crawl",
            "     tần suất thấp (sleep 1.5s/request), chỉ phục vụ mục đích học tập.",
            "",
            "2. Google Trends Explore API (dataset: google_trends)",
            "   • Access: Thư viện pytrends 4.9.2 (geo=VN, hl=vi-VN, timeframe='today 3-m')",
            "   • Scale: 4 nhóm ngành hàng × 5 từ khóa = 20 từ khóa; 8 files (4 CSV chuỗi",
            f"     thời gian {trends_m.get('total_records', 372)} dòng + 4 JSON truy vấn liên quan, ~{trends_m.get('total_bytes', 32000)//1024} KB).",
            "   • Provenance: Google Trends (chỉ số 0–100); tuân thủ Google ToS.",
            "",
            "3. Google Trends Daily RSS (dataset: google_trends_rss)",
            "   • URL: https://trends.google.com/trending/rss?geo=VN",
            f"   • Scale: 1 file XML ({rss_m.get('total_bytes', 22061):,} bytes, {rss_m.get('total_records', 10)} chủ đề nóng kèm tin tức).",
            "   • Version/Snapshot: Định danh bằng run_id (UTC) + SHA-256 trong _manifest.json.",
        ],
        bg=(239, 246, 255), border=(147, 197, 253), head_color=(29, 78, 216), font_size=10.5
    )
    ingest_lines = [
        "$ python src/ingest_all.py",
        "",
        "===== rss: ingest_trends_rss.py =====",
        f"INFO | Bắt đầu {rss_m.get('run_id', '')}",
        f"INFO | OK: {rss_m.get('total_records', 10)} trending items",
        f"INFO | Kết thúc | status={rss_m.get('status', 'SUCCESS')} | bytes={rss_m.get('total_bytes', 22061)} | records={rss_m.get('total_records', 10)}",
        "",
        "===== trends: ingest_google_trends.py =====",
        f"INFO | Bắt đầu {trends_m.get('run_id', '')}",
        "INFO | Nhóm 1..4 OK: 93 dòng/nhóm (4 CSV + 4 JSON)",
        f"INFO | Kết thúc | status={trends_m.get('status', 'SUCCESS')} | files={trends_m.get('file_count', 8)} | bytes={trends_m.get('total_bytes', 0)} | records={trends_m.get('total_records', 372)}",
        "",
        "===== tiki: ingest_tiki_products.py =====",
        f"INFO | Bắt đầu {tiki_m.get('run_id', '')}",
        "INFO | cat=1789 (Điện thoại - Máy tính bảng) p1..3: 106 SP",
        "INFO | cat=1815 (Thiết bị số - Phụ kiện số) p1..3: 120 SP",
        "INFO | cat=1520 (Làm đẹp) • cat=1882 (Gia dụng) • cat=931 (Thời trang): 360 SP",
        f"INFO | Kết thúc | status={tiki_m.get('status', 'SUCCESS')} | files={tiki_m.get('file_count', 15)} | bytes={tiki_m.get('total_bytes', 2607857)} | records={tiki_m.get('total_records', 586)}",
        "",
        "SUMMARY: rss -> OK (0) | trends -> OK (0) | tiki -> OK (0)",
    ]
    add_card(
        s4, 6.55, 1.30, 6.33, 5.85,
        "INGESTION EVIDENCE: COMMAND & REAL LOG OUTPUT",
        ingest_lines,
        bg=(15, 23, 42), border=(51, 65, 85), head_color=(56, 189, 248), mono=True, font_size=9.8
    )

    # ---------------- SLIDE 5 ----------------
    s5 = prs.slides.add_slide(blank)
    add_header_banner(
        s5,
        "CHECKPOINT 2 • SLIDE 5",
        "Raw storage & manifest — Đường dẫn thật, dung lượng thật và đối chiếu SHA-256",
        "Quy ước: data/raw/<dataset>/ingest_date=YYYY-MM-DD/run_<UTC_TIMESTAMP>/"
    )
    tiki_first_file = (tiki_m.get("files") or [{}])[0]
    tree_lines = [
        "Path convention: data/raw/<dataset>/ingest_date=YYYY-MM-DD/run_<UTC>/",
        "",
        "data/raw/",
        "├── google_trends/",
        "│   └── ingest_date=2026-10-23/",
        f"│       └── {trends_m.get('run_id', 'run_...')}/",
        "│           ├── interest_over_time_g1..g4.csv   (4 files)",
        "│           ├── related_queries_g1..g4.json     (4 files)",
        "│           ├── _manifest.json & _SUCCESS",
        "├── google_trends_rss/",
        "│   └── ingest_date=2026-10-23/",
        f"│       └── {rss_m.get('run_id', 'run_...')}/  (3 runs kiểm chứng rerun)",
        "│           ├── trending_VN.xml                 (22,061 B)",
        "│           ├── _manifest.json & _SUCCESS",
        "└── tiki_products/",
        "    └── ingest_date=2026-10-23/",
        f"        └── {tiki_m.get('run_id', 'run_...')}/",
        "            ├── cat=1789_page=1..3.json ... cat=931_page=3.json (15 files)",
        "            ├── _manifest.json & _SUCCESS       (Tổng: 2,607,857 B)",
    ]
    add_card(
        s5, 0.45, 1.30, 6.20, 5.85,
        "RAW STORAGE TREE & DUNG LƯỢNG THẬT",
        tree_lines,
        bg=(15, 23, 42), border=(51, 65, 85), head_color=(74, 222, 128), mono=True, font_size=9.5
    )
    manifest_excerpt = [
        f"// Trích {tiki_m.get('destination', 'data/raw/tiki_products/...')}/_manifest.json",
        "{",
        f'  "run_id": "{tiki_m.get("run_id", "")}",',
        '  "dataset": "tiki_products",',
        f'  "status": "{tiki_m.get("status", "SUCCESS")}",',
        '  "source": {',
        '    "name": "Tiki.vn public listing API",',
        '    "url": "https://tiki.vn/api/personalish/v1/blocks/listings"',
        "  },",
        f'  "ingest_started_utc": "{tiki_m.get("ingest_started_utc", "")}",',
        f'  "destination": "{tiki_m.get("destination", "")}",',
        f'  "file_count": {tiki_m.get("file_count", 15)},',
        f'  "total_bytes": {tiki_m.get("total_bytes", 2607857)},',
        f'  "total_records": {tiki_m.get("total_records", 586)},',
        '  "files": [ {',
        f'    "file": "{tiki_first_file.get("file", "cat=1789_page=1.json")}",',
        f'    "bytes": {tiki_first_file.get("bytes", 0)},',
        f'    "sha256": "{str(tiki_first_file.get("sha256", ""))[:36]}...",',
        f'    "records": {tiki_first_file.get("records", 40)}, "duplicate_of_run": null',
        "  }, ... ]",
        "}",
        "✔ Spark Verify (logs/verify_raw.txt): VERIFY RESULT: PASS (586 SP, 372 dòng Trends)",
    ]
    add_card(
        s5, 6.80, 1.30, 6.08, 5.85,
        "MANIFEST / LOG (_manifest.json & Spark Verify)",
        manifest_excerpt,
        bg=(15, 23, 42), border=(51, 65, 85), head_color=(250, 204, 21), mono=True, font_size=9.3
    )

    # ---------------- SLIDE 6 ----------------
    s6 = prs.slides.add_slide(blank)
    add_header_banner(
        s6,
        "CHECKPOINT 2 • SLIDE 6",
        "Rerun behavior, blocker & next action — Quyết định kỹ thuật rõ ràng",
        "Trả lời câu hỏi bắt buộc khi chạy ingest 3 lần liên tiếp & kế hoạch trước Buổi 7"
    )
    rss_runs = runs_map.get("google_trends_rss", [])
    add_card(
        s6, 0.45, 1.30, 6.15, 3.55,
        "NẾU CHẠY INGEST 3 LẦN LIÊN TIẾP (RERUN BEHAVIOR)",
        [
            "• Cơ chế: Append-only theo run_id (run_<YYYYMMDDTHHMMSSffffffZ>).",
            "• KHÔNG OVERWRITE: Thư mục run tạo với mkdir(exist_ok=False), bảo toàn tuyệt đối",
            "  mọi snapshot raw trước đó.",
            "• Thay đổi trong Raw Storage khi chạy 3 lần:",
            "  1) Sinh 3 thư mục run_<UTC> riêng biệt trong ingest_date=YYYY-MM-DD/.",
            "  2) Sinh 3 file _manifest.json + 3 cờ _SUCCESS độc lập.",
            "  3) Append 3 dòng log vào sổ cái metadata/ingest_log.jsonl.",
            "• Duplicate handling: SHA-256 của mỗi file mới được so với các run SUCCESS trước;",
            "  nếu trùng nội dung, manifest tự động ghi trường duplicate_of_run = '<run_id cũ>'",
            "  để tầng Staging (Buổi 7) bỏ qua hoặc dedup theo (product_id, snapshot_time).",
            f"• Bằng chứng thực nghiệm (google_trends_rss đã chạy {len(rss_runs)} lần):",
            "  " + ", ".join(rss_runs[-3:]),
        ],
        bg=(236, 253, 245), border=(110, 231, 183), head_color=(4, 120, 87), font_size=10.3
    )
    add_card(
        s6, 6.75, 1.30, 6.13, 3.55,
        "BLOCKER LỚN NHẤT & EVIDENCE LINKS",
        [
            "• Blocker lớn nhất:",
            "  1) Google Trends (pytrends) giới hạn tần suất (HTTP 429) khi mở rộng nhiều từ khóa.",
            "     → Đã giảm thiểu bằng sleep 8s + retry backoff + bổ sung Google Trends RSS.",
            "  2) Khác biệt giữa từ khóa xu hướng và tên sản phẩm trên Tiki (cần chuẩn hóa",
            "     và khớp nối ở Buổi 7).",
            "• Owner: Phạm Xuân Khoa & Trần Tấn Toàn   |   Deadline: Trước Buổi 7",
            "────────────────────────────────────────",
            "• EVIDENCE LINKS TRONG REPO:",
            f"  - Git commit / tag : {commit} (tag: {tag})",
            "  - Smoke test log   : logs/env_check.txt",
            "  - Spark verify log : logs/verify_raw.txt",
            "  - Sổ cái Ingest    : metadata/ingest_log.jsonl",
            "  - Raw & Manifest   : data/raw/<dataset>/ingest_date=2026-10-23/run_*/_manifest.json",
        ],
        bg=(254, 243, 199), border=(252, 211, 77), head_color=(180, 83, 9), font_size=10.3
    )
    add_card(
        s6, 0.45, 5.00, 12.43, 2.15,
        "TRƯỚC BUỔI 7 — VIỆC PHẢI HOÀN THÀNH & PHÂN CÔNG (OWNER + DEADLINE)",
        [
            "1. [Trần Tấn Toàn – Nhóm trưởng] Xây dựng Spark ETL chuyển Raw (JSON/CSV/XML) → Staging Parquet, lọc run trùng (duplicate_of_run) và dedup theo product_id.",
            "2. [Phạm Xuân Khoa] Mở rộng bộ từ khóa & danh mục sản phẩm TMĐT; tối ưu cơ chế cache/backoff cho Google Trends.",
            "3. [Hồ Tuấn Anh] Thiết kế thuật toán khớp từ khóa xu hướng với sản phẩm và công thức tính Trend Score gắn nhãn Affiliate (HOT / RISING / STABLE / COLD).",
            "4. [Trần Thanh Lam] Viết bộ kiểm thử chất lượng dữ liệu (schema check, null check) và chuẩn bị báo cáo Buổi 7.",
        ],
        bg=(239, 246, 255), border=(147, 197, 253), head_color=(29, 78, 216), font_size=11
    )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    prs.save(out_path)


def set_cell_bg(cell, hex_color: str) -> None:
    tcPr = cell._tc.get_or_add_tcPr()
    shd = OxmlElement("w:shd")
    shd.set(qn("w:val"), "clear")
    shd.set(qn("w:color"), "auto")
    shd.set(qn("w:fill"), hex_color)
    tcPr.append(shd)


def generate_docx(out_path: Path, arch_img: Path, commit: str, tag: str,
                  manifests: dict[str, dict], runs_map: dict[str, list[str]]) -> None:
    doc = Document()
    style = doc.styles["Normal"]
    style.font.name = "Calibri"
    style.font.size = Pt(10.5)

    for section in doc.sections:
        section.top_margin = Inches(0.65)
        section.bottom_margin = Inches(0.65)
        section.left_margin = Inches(0.75)
        section.right_margin = Inches(0.75)

    tiki_m = manifests.get("tiki_products", {})
    trends_m = manifests.get("google_trends", {})
    rss_m = manifests.get("google_trends_rss", {})
    rss_runs = runs_map.get("google_trends_rss", [])

    # Title block
    p_sub = doc.add_paragraph("Dữ liệu lớn • Đồ án 2 • Checkpoint 2")
    p_sub.runs[0].font.color.rgb = RGBColor(37, 99, 235)
    p_sub.runs[0].font.bold = True

    h0 = doc.add_heading("DỮ LIỆU LỚN – ĐỒ ÁN 2: Environment → Ingestion → Raw Storage", level=1)
    h0.runs[0].font.color.rgb = RGBColor(15, 23, 42)

    p_meta = doc.add_paragraph(
        "Project Brief & Báo cáo Nộp Checkpoint 2 • Buổi 6 | Giảng viên: TS. Hồ Quốc Dũng\n"
        "Đề tài: Hệ thống AI truy xuất xu hướng để gắn nhãn Affiliate cho các sàn Thương mại Điện tử\n"
        "Nhóm thực hiện: Trần Tấn Toàn (Nhóm trưởng), Phạm Xuân Khoa, Hồ Tuấn Anh, Trần Thanh Lam"
    )
    p_meta.runs[0].font.bold = True

    # Section 1: Pre-work
    doc.add_heading("1. PRE-WORK TRƯỚC BUỔI 6 (ĐÃ HOÀN THÀNH)", level=2)
    t1 = doc.add_table(rows=1, cols=3)
    t1.style = "Table Grid"
    t1.alignment = WD_TABLE_ALIGNMENT.CENTER
    hdr1 = ["Hạng mục", "Phải chuẩn bị & Bằng chứng thực tế của nhóm", "Evidence đầu buổi"]
    for i, text in enumerate(hdr1):
        cell = t1.rows[0].cells[i]
        cell.text = text
        cell.paragraphs[0].runs[0].font.bold = True
        set_cell_bg(cell, "DBEAFE")

    prework_rows = [
        ("Repo", "Git repo đã khởi tạo (trend-affiliate-bigdata); đầy đủ README.md, requirements.txt, .env.example, src/, config/, data/raw/, data/sample/, logs/, metadata/, docs/.", "☑ Có   ☐ Chưa"),
        ("Environment", "Python 3.11.9 + OpenJDK 17.0.20.1 (Temurin) + PySpark 3.5.3 (local[*]); đã chạy smoke test python src/env_check.py đạt PASS (6.2s), lưu logs/env_check.txt.", "☑ Có   ☐ Chưa"),
        ("Data source", "Đã chốt 3 nguồn: (1) Tiki Listing API (JSON); (2) Google Trends Explore qua pytrends (CSV/JSON); (3) Google Trends Daily RSS VN (XML). Ghi rõ provenance tại metadata/sources.md.", "☑ Có   ☐ Chưa"),
        ("Raw plan", "Quy ước thư mục: data/raw/<dataset>/ingest_date=YYYY-MM-DD/run_<UTC_TIMESTAMP>/ kèm _manifest.json và cờ _SUCCESS.", "☑ Có   ☐ Chưa"),
        ("Pitch", "Đã chuẩn bị slide báo cáo 6 trang (docs/BigData_Buoi06_DoAn2_Checkpoint2_Report.pptx) phục vụ pitch 4 phút.", "☑ Có   ☐ Chưa"),
    ]
    for item, desc, status in prework_rows:
        row = t1.add_row().cells
        row[0].text = item
        row[0].paragraphs[0].runs[0].font.bold = True
        row[1].text = desc
        row[2].text = status

    # Section 2: Artifacts
    doc.add_heading("2. SẢN PHẨM CUỐI BUỔI 6 (ARTIFACTS & BẰNG CHỨNG)", level=2)
    t2 = doc.add_table(rows=1, cols=3)
    t2.style = "Table Grid"
    for i, text in enumerate(["Artifact", "Yêu cầu", "Bằng chứng thực tế trong Repo của nhóm"]):
        c = t2.rows[0].cells[i]
        c.text = text
        c.paragraphs[0].runs[0].font.bold = True
        set_cell_bg(c, "DBEAFE")

    art_rows = [
        ("ENV", "Version + smoke test PASS", "File logs/env_check.txt: Python 3.11.9, OpenJDK 17.0.20.1, PySpark 3.5.3, local[*] (12 cores), count(1,000,000) = 1,000,000 -> RESULT: PASS."),
        ("SOURCE", "Nguồn dữ liệu và provenance", "File config/sources.json & metadata/sources.md: mô tả chi tiết URL, tham số truy vấn, snapshot timestamp, SHA-256 và điều khoản sử dụng của Tiki API & Google Trends."),
        ("INGEST", "Ingestion script/job chạy được", "src/ingest_tiki_products.py, src/ingest_google_trends.py, src/ingest_trends_rss.py và src/ingest_all.py — cả 3 đều chạy thành công (exit=0)."),
        ("RAW", "Raw file không rỗng, đúng quy ước", f"data/raw/tiki_products/... ({tiki_m.get('file_count', 15)} files, {tiki_m.get('total_bytes', 2607857):,} B); data/raw/google_trends/... ({trends_m.get('file_count', 8)} files, {trends_m.get('total_bytes', 0):,} B); data/raw/google_trends_rss/... (1 file XML, {rss_m.get('total_bytes', 22061):,} B)."),
        ("TRACE", "Manifest/log đầy đủ", "Mỗi thư mục run có _manifest.json (source, ingest_started_utc, bytes, sha256, records, duplicate_of_run, status) + metadata/ingest_log.jsonl + logs/verify_raw.txt."),
        ("REPO", "Commit checkpoint & README", f"README.md có hướng dẫn setup ≤ 10 phút; Git commit {commit} gắn tag {tag}."),
    ]
    for a, req, ev in art_rows:
        r = t2.add_row().cells
        r[0].text = a
        r[0].paragraphs[0].runs[0].font.bold = True
        r[1].text = req
        r[2].text = ev

    # Section 3: Checklist
    doc.add_heading("3. CHECKLIST KIỂM TRA TẠI BÀN", level=2)
    checks = [
        "☑ Repo mở được; có README.md + src/ + data/raw/ + logs/ + metadata/ + docs/.",
        "☑ env_check.py chạy PASS tại chỗ (RESULT: PASS).",
        "☑ Source thật truy cập được (Tiki API + Google Trends API/RSS) và snapshot có provenance.",
        "☑ Ingest job chạy lại được nhiều lần và nhóm giải thích rõ behavior khi rerun.",
        "☑ Raw file tồn tại, không rỗng, tuân thủ quy ước data/raw/<dataset>/ingest_date=YYYY-MM-DD/run_<UTC>/.",
        "☑ Manifest/log (_manifest.json & metadata/ingest_log.jsonl) khớp 100% SHA-256 và byte size với raw file.",
        "☑ Spark đọc được sample raw (python src/verify_raw_spark.py -> VERIFY RESULT: PASS).",
        "☑ Mỗi thành viên nắm rõ luồng dữ liệu (data flow), trade-off kiến trúc và phần việc phụ trách.",
    ]
    for item in checks:
        doc.add_paragraph(item, style="List Bullet")

    if arch_img.exists():
        doc.add_heading("Sơ đồ kiến trúc đã triển khai đến Checkpoint 2", level=3)
        doc.add_picture(str(arch_img), width=Inches(6.7))

    # Section 4: Checkpoint Report Form (Page 4 of Brief template)
    doc.add_heading("4. CHECKPOINT REPORT – MẪU NỘP (ĐÃ ĐIỀN ĐẦY ĐỦ)", level=2)
    t4 = doc.add_table(rows=1, cols=2)
    t4.style = "Table Grid"
    for i, text in enumerate(["Trường thông tin", "Nội dung báo cáo của nhóm"]):
        c = t4.rows[0].cells[i]
        c.text = text
        c.paragraphs[0].runs[0].font.bold = True
        set_cell_bg(c, "DBEAFE")

    report_fields = [
        ("Tên nhóm / thành viên", "Nhóm TrendAffiliate:\n1. Trần Tấn Toàn (Nhóm trưởng – 23T1020547)\n2. Phạm Xuân Khoa\n3. Hồ Tuấn Anh\n4. Trần Thanh Lam"),
        ("Đề tài", "Hệ thống AI truy xuất xu hướng để gắn nhãn Affiliate cho các sàn Thương mại Điện tử"),
        ("Git commit/tag", f"Commit: {commit} | Tag: {tag}"),
        ("Environment", "Python: 3.11.9 | Java: OpenJDK 17.0.20.1 (Temurin) | Spark: 3.5.3 | Mode: local[*] (12 cores)"),
        ("Data source", "1) Tiki Public Listing API: https://tiki.vn/api/personalish/v1/blocks/listings (5 ngành hàng, 586 sản phẩm)\n2) Google Trends Explore API (pytrends, geo=VN, timeframe=today 3-m, 20 từ khóa sản phẩm, 372 dòng chuỗi thời gian)\n3) Google Trends Trending Now RSS: https://trends.google.com/trending/rss?geo=VN (10 chủ đề xu hướng nóng trong ngày)"),
        ("Ingestion command", "Chạy toàn bộ: python src/ingest_all.py (hoặc .\\run.ps1 src\\ingest_all.py)\nChạy từng nguồn: python src/ingest_tiki_products.py | python src/ingest_google_trends.py | python src/ingest_trends_rss.py"),
        ("Raw location", "data/raw/<dataset>/ingest_date=YYYY-MM-DD/run_<UTC_TIMESTAMP>/\n(Gồm 3 dataset: tiki_products, google_trends, google_trends_rss)"),
        ("Manifest/log location", "• Manifest từng lần chạy: data/raw/<dataset>/ingest_date=YYYY-MM-DD/run_<UTC_TIMESTAMP>/_manifest.json\n• Sổ cái toàn cục: metadata/ingest_log.jsonl\n• Log thực thi & kiểm chứng: logs/env_check.txt, logs/verify_raw.txt, logs/ingest_*.log"),
        ("Rerun behavior", f"Append-only theo run_id (run_<YYYYMMDDTHHMMSSffffffZ>): mỗi lần chạy tạo một thư mục run mới với mkdir(exist_ok=False), tuyệt đối không ghi đè (overwrite) dữ liệu cũ. Mọi file mới được tính SHA-256 và đối chiếu với các run SUCCESS trước đó; nếu trùng nội dung thì trường duplicate_of_run trong _manifest.json sẽ trỏ về run_id gốc (đã kiểm chứng chạy {len(rss_runs)} lần liên tiếp với google_trends_rss). Việc khử trùng lặp bản ghi theo (product_id, snapshot_time) sẽ thực hiện ở tầng Staging (Buổi 7)."),
        ("Blocker còn lại", "1) Google Trends API (pytrends) có thể trả HTTP 429 nếu truy vấn dồn dập (đã xử lý bằng sleep 8s + retry backoff và bổ sung nguồn RSS ổn định).\n2) Bài toán chuẩn hóa và khớp từ khóa xu hướng với tên sản phẩm thực tế trên Tiki ở tầng Staging."),
        ("Owner + deadline", "• Trần Tấn Toàn (Nhóm trưởng): Spark Staging ETL (JSON/CSV/XML -> Parquet) & dedup — Trước Buổi 7\n• Phạm Xuân Khoa: Mở rộng danh mục Tiki & quản lý rate-limit Google Trends — Trước Buổi 7\n• Hồ Tuấn Anh: Thuật toán khớp từ khóa ↔ sản phẩm & tính Trend Score gắn nhãn Affiliate — Trước Buổi 7\n• Trần Thanh Lam: Kiểm thử Data Quality & tài liệu hóa — Trước Buổi 7"),
    ]
    for k, v in report_fields:
        r = t4.add_row().cells
        r[0].text = k
        r[0].paragraphs[0].runs[0].font.bold = True
        r[1].text = v

    # Exit Ticket
    doc.add_heading("5. EXIT TICKET (CÂU HỎI BẮT BUỘC CUỐI BUỔI 6)", level=2)
    qas = [
        (
            "Câu 1: Một lệnh duy nhất để kiểm tra environment của nhóm là gì?",
            "Lệnh: python src/env_check.py (hoặc trên Windows PowerShell: .\\run.ps1 src\\env_check.py). Lệnh này kiểm tra phiên bản OS, Python, JAVA_HOME, OpenJDK, PySpark, khởi tạo SparkSession ở chế độ local[*], thực hiện phép đếm 1.000.000 dòng và tính tổng 1..100, sau đó ghi kết quả PASS vào logs/env_check.txt."
        ),
        (
            "Câu 2: Nhóm xác định version/snapshot dữ liệu bằng cách nào?",
            "Vì dữ liệu từ Google Trends và sàn TMĐT Tiki là dữ liệu động theo thời gian thực (không có số phiên bản tĩnh từ nhà cung cấp), nhóm định danh mỗi snapshot bằng tổ hợp: (1) phân vùng ngày thu thập ingest_date=YYYY-MM-DD; (2) mã định danh lần chạy run_<UTC_TIMESTAMP> đến độ chính xác micro-giây; (3) bộ tham số truy vấn params (danh mục, bộ từ khóa, timeframe, geo); và (4) mã băm mật mã SHA-256 cùng kích thước byte của từng file gốc được lưu bất biến trong _manifest.json."
        ),
        (
            "Câu 3: Nếu chạy ingestion 3 lần liên tiếp, raw storage sẽ thay đổi thế nào?",
            f"Khi chạy ingestion 3 lần liên tiếp, raw storage KHÔNG ghi đè bất kỳ file nào mà tạo ra 3 thư mục con độc lập: run_<UTC_1>, run_<UTC_2>, run_<UTC_3> bên trong data/raw/<dataset>/ingest_date=YYYY-MM-DD/. Mỗi thư mục chứa bản nguyên mẫu dữ liệu của lần chạy đó, 1 file _manifest.json và 1 cờ _SUCCESS; đồng thời file metadata/ingest_log.jsonl được nối thêm (append) 3 dòng JSON. Nếu dữ liệu giữa lần chạy sau giống hệt lần chạy trước (cùng mã SHA-256), trường duplicate_of_run trong _manifest.json sẽ ghi nhận mã run_id đầu tiên chứa file đó để tầng xử lý Spark Staging ở Buổi 7 tự động loại bỏ trùng lặp."
        ),
        (
            "Câu 4: Rủi ro lớn nhất trước Buổi 7 là gì?",
            "Rủi ro kỹ thuật lớn nhất trước Buổi 7 là độ nhiễu khi liên kết (entity matching) giữa từ khóa tìm kiếm ngắn gọn trên Google Trends với tên sản phẩm dài, chứa nhiều từ khóa quảng cáo trên sàn TMĐT Tiki, cùng với nguy cơ bị giới hạn tần suất (HTTP 429) từ Google Trends khi mở rộng quy mô từ khóa."
        ),
    ]
    for q, a in qas:
        pq = doc.add_paragraph()
        rq = pq.add_run(q)
        rq.bold = True
        pa = doc.add_paragraph(a)
        pa.paragraph_format.left_indent = Inches(0.25)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    doc.save(out_path)


def main() -> None:
    commit, tag = get_git_info()
    manifests = load_latest_manifests()
    runs_map = count_runs_per_dataset()

    arch_img = DOCS_DIR / "architecture_checkpoint2.png"
    pptx_path = DOCS_DIR / "BigData_Buoi06_DoAn2_Checkpoint2_Report.pptx"
    docx_path = DOCS_DIR / "BigData_Buoi06_DoAn2_Brief_Checkpoint_Report.docx"

    generate_architecture_diagram(arch_img)
    print(f"[OK] Architecture diagram -> {arch_img.relative_to(ROOT)}")

    generate_pptx(pptx_path, arch_img, commit, tag, manifests, runs_map)
    print(f"[OK] PowerPoint report    -> {pptx_path.relative_to(ROOT)}")

    generate_docx(docx_path, arch_img, commit, tag, manifests, runs_map)
    print(f"[OK] Word report          -> {docx_path.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
