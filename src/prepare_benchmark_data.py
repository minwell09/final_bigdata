"""Chuẩn bị dataset benchmark cố định từ dữ liệu raw thật (data/raw/).

Tạo ra 2 bảng Parquet trong data/benchmark/:
- products_fact.parquet  : ~600,000 dòng sản phẩm/snapshot mở rộng có kiểm soát từ Tiki raw
- category_trends.parquet: bảng dimension xu hướng theo category từ Google Trends raw

Chạy:
    python src/prepare_benchmark_data.py --scale 1000
"""
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

import pandas as pd

from common import PROJECT_ROOT, RAW_ROOT

BENCH_DIR = PROJECT_ROOT / "data" / "benchmark"

# Ánh xạ nhóm từ khoá Google Trends (g1..g4) -> Tiki category_id trong config/sources.json
GROUP_TO_CATS = {
    "g1": [1789, 1815],  # Điện thoại & Phụ kiện số
    "g2": [1520],        # Làm đẹp - Sức khỏe
    "g3": [1882],        # Điện gia dụng
    "g4": [931],         # Thời trang nữ
}


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


def load_trends_dim() -> pd.DataFrame:
    sources = json.loads((PROJECT_ROOT / "config" / "sources.json").read_text(encoding="utf-8"))
    cat_names = {c["id"]: c["name"] for c in sources["tiki"]["categories"]}

    run_dir = latest_success_run("google_trends")
    dim_rows = []
    for g, cat_ids in GROUP_TO_CATS.items():
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
        for cid in cat_ids:
            dim_rows.append({
                "category_id": int(cid),
                "category_name": cat_names.get(cid, f"Category {cid}"),
                "trend_group": g,
                "avg_trend_interest": avg_interest,
                "peak_trend_interest": round(peak, 4),
            })
    return pd.DataFrame(dim_rows)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--scale", type=int, default=1000, help="Hệ số nhân bản từ raw Tiki (~600 rows * scale)")
    args = parser.parse_args()

    BENCH_DIR.mkdir(parents=True, exist_ok=True)
    base_rows = load_tiki_raw()
    base_df = pd.DataFrame(base_rows)
    dim_df = load_trends_dim()

    # Nhân bản có kiểm soát (mô phỏng nhiều snapshot/ngày thu thập)
    chunks = []
    for i in range(args.scale):
        c = base_df.copy()
        c["snapshot_idx"] = i
        c["event_id"] = c["product_id"].astype("int64") * 10000 + i
        chunks.append(c)
    fact_df = pd.concat(chunks, ignore_index=True)

    fact_path = BENCH_DIR / "products_fact.parquet"
    dim_path = BENCH_DIR / "category_trends.parquet"
    fact_df.to_parquet(fact_path, index=False)
    dim_df.to_parquet(dim_path, index=False)

    print(f"Prepared benchmark data in {BENCH_DIR}:")
    print(f"  products_fact.parquet  : {len(fact_df):,} rows ({fact_path.stat().st_size / (1024*1024):.2f} MB)")
    print(f"  category_trends.parquet: {len(dim_df):,} rows ({dim_path.stat().st_size / 1024:.2f} KB)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
