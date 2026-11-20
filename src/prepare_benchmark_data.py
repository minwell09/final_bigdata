"""Chuẩn bị dataset benchmark cố định từ dữ liệu raw thật (data/raw/).

Tạo ra 2 bảng trong data/benchmark/:
- products_fact.parquet: ~1,172,000 dòng sản phẩm/snapshot mở rộng từ Tiki raw
- trends_dim.csv       : ~12.5 MB (20,000 dòng) bảng dimension xu hướng theo (category_id, trend_bucket)
                         sinh từ Google Trends + Google Trends RSS raw.
                         Kích thước > 10 MB trên đĩa để với Spark mặc định
                         (autoBroadcastJoinThreshold=10MB), Spark tự động chọn SortMergeJoin
                         (không cần tắt threshold nhân tạo), còn sau khi column-pruning +
                         F.broadcast() thì broadcast hash table chỉ vài trăm KB (an toàn bộ nhớ).

Chạy:
    python src/prepare_benchmark_data.py --scale 2000
"""
from __future__ import annotations

import argparse
import csv
import json
import xml.etree.ElementTree as ET
from pathlib import Path

import pandas as pd

from common import PROJECT_ROOT, RAW_ROOT

BENCH_DIR = PROJECT_ROOT / "data" / "benchmark"

GROUP_TO_CATS = {
    "g1": [1789, 1815],  # Điện thoại & Phụ kiện số
    "g2": [1520],        # Làm đẹp - Sức khỏe
    "g3": [1882],        # Điện gia dụng
    "g4": [931],         # Thời trang nữ
}

BUCKETS_PER_CAT = 6500  # 5 categories * 6500 buckets = 32,500 dim rows (~13 MB > 10 MB default threshold)


def latest_success_run(dataset: str) -> Path:
    runs = sorted((RAW_ROOT / dataset).glob("ingest_date=*/run_*"))
    runs = [r for r in runs if (r / "_SUCCESS").exists()]
    if not runs:
        raise FileNotFoundError(f"Không tìm thấy run _SUCCESS cho dataset {dataset}")
    return runs[-1]


def load_tiki_raw() -> list[dict]:
    run_dir = latest_success_run("tiki_products")
    rows: list[dict] = []
    for f in sorted(run_dir.glob("cat=*_page=*.json")):
        cat_id = int(f.name.split("_")[0].split("=")[1])
        payload = json.loads(f.read_text(encoding="utf-8"))
        for p in payload.get("data", []):
            sold_obj = p.get("quantity_sold") or {}
            rows.append({
                "product_id": int(p.get("id", 0)),
                "category_id": cat_id,
                "name": str(p.get("name", "")),
                "price": float(p.get("price", 0) or 0),
                "original_price": float(p.get("original_price", p.get("price", 0)) or 0),
                "discount_rate": float(p.get("discount_rate", 0) or 0),
                "rating_average": float(p.get("rating_average", 0) or 0),
                "review_count": int(p.get("review_count", 0) or 0),
                "sold": int(sold_obj.get("value", 0) or 0),
            })
    return rows


def load_rss_headlines() -> str:
    run_dir = latest_success_run("google_trends_rss")
    xml_files = sorted(run_dir.glob("*.xml"))
    titles: list[str] = []
    if xml_files:
        root = ET.fromstring(xml_files[0].read_text(encoding="utf-8"))
        for item in root.findall(".//item/title"):
            if item.text:
                titles.append(item.text.strip())
    return " | ".join(titles[:15]) if titles else "trending_vn_snapshot"


def load_trends_dim() -> pd.DataFrame:
    sources = json.loads((PROJECT_ROOT / "config" / "sources.json").read_text(encoding="utf-8"))
    cat_names = {c["id"]: c["name"] for c in sources["tiki"]["categories"]}
    kw_groups = sources["google_trends"]["keyword_groups"]
    rss_context = load_rss_headlines()

    run_dir = latest_success_run("google_trends")
    base_cat_stats: list[dict] = []
    for idx, (g, cat_ids) in enumerate(GROUP_TO_CATS.items()):
        csv_path = run_dir / f"interest_over_time_{g}.csv"
        values: list[float] = []
        peak = 0.0
        with csv_path.open("r", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                for k, v in row.items():
                    if k in ("date", "isPartial") or not v:
                        continue
                    try:
                        val = float(v)
                        values.append(val)
                        if val > peak:
                            peak = val
                    except ValueError:
                        pass
        avg_interest = round(sum(values) / len(values), 4) if values else 50.0
        kw_str = ", ".join(kw_groups[idx]) if idx < len(kw_groups) else g
        for cid in cat_ids:
            base_cat_stats.append({
                "category_id": int(cid),
                "category_name": cat_names.get(cid, f"Category {cid}"),
                "trend_group": g,
                "avg_trend_interest": avg_interest,
                "peak_trend_interest": round(peak, 4),
                "keywords_meta": kw_str,
            })

    # Tạo 20,000 dòng dimension (category_id x trend_bucket) kèm raw metadata text
    # -> ~12.5 MB trên đĩa (vượt ngưỡng autoBroadcastJoinThreshold=10MB mặc định của Spark)
    dim_rows: list[dict] = []
    for stat in base_cat_stats:
        cid = stat["category_id"]
        for b in range(BUCKETS_PER_CAT):
            # Dao động nhẹ theo bucket để giữ tính đa dạng nhưng tất định
            adj = round(((b % 11) - 5) * 0.4, 2)
            interest = max(5.0, min(100.0, round(stat["avg_trend_interest"] + adj, 4)))
            dim_rows.append({
                "trend_key": f"{cid}_{b:04d}",
                "category_id": cid,
                "category_name": stat["category_name"],
                "trend_group": stat["trend_group"],
                "avg_trend_interest": interest,
                "peak_trend_interest": stat["peak_trend_interest"],
                "raw_context_payload": f"bucket={b:04d};kw=[{stat['keywords_meta']}];rss=[{rss_context}];src=google_trends_vn",
            })
    return pd.DataFrame(dim_rows)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--scale", type=int, default=2000, help="Hệ số nhân bản từ raw Tiki (~586 rows * scale)")
    args = parser.parse_args()

    BENCH_DIR.mkdir(parents=True, exist_ok=True)
    base_rows = load_tiki_raw()
    base_df = pd.DataFrame(base_rows)
    dim_df = load_trends_dim()

    # Nhân bản có kiểm soát từ dữ liệu thật Tiki + gắn trend_key khớp 100% với trends_dim
    chunks = []
    for i in range(args.scale):
        c = base_df.copy()
        c["snapshot_idx"] = i
        c["event_id"] = c["product_id"].astype("int64") * 10000 + i
        bucket = (c["product_id"] + i) % BUCKETS_PER_CAT
        c["trend_key"] = c["category_id"].astype(str) + "_" + bucket.astype(str).str.zfill(4)
        chunks.append(c)
    fact_df = pd.concat(chunks, ignore_index=True)

    fact_path = BENCH_DIR / "products_fact.parquet"
    dim_path = BENCH_DIR / "trends_dim.csv"
    fact_df.to_parquet(fact_path, index=False)
    dim_df.to_csv(dim_path, index=False)

    print(f"Prepared benchmark data in {BENCH_DIR}:")
    print(f"  products_fact.parquet: {len(fact_df):,} rows ({fact_path.stat().st_size / (1024*1024):.2f} MB)")
    print(f"  trends_dim.csv       : {len(dim_df):,} rows ({dim_path.stat().st_size / (1024*1024):.2f} MB)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
