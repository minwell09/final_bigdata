"""Spark Pipeline Đồ án 4: Tính Trend Score & Gắn nhãn Affiliate (HOT/RISING/STABLE/COLD).

Hỗ trợ 2 chế độ với CÙNG workload, CÙNG Spark config, chỉ khác ĐÚNG 1 thay đổi:
- baseline : join thông thường (SortMergeJoin, shuffle cả bảng fact lớn và dim)
- optimized: dùng F.broadcast(dim_df) (BroadcastHashJoin, loại bỏ shuffle bảng fact)

Chạy:
    python src/pipeline_affiliate_spark.py --mode baseline
    python src/pipeline_affiliate_spark.py --mode optimized
"""
from __future__ import annotations

import argparse
import io
import json
import time
from contextlib import redirect_stdout
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common import PROJECT_ROOT

BENCH_DIR = PROJECT_ROOT / "data" / "benchmark"
OUT_ROOT = PROJECT_ROOT / "data" / "output"

# Cấu hình tài nguyên cố định cho cả baseline và optimized
SPARK_MASTER = "local[2]"
DRIVER_MEMORY = "2g"
SHUFFLE_PARTITIONS = "64"


def build_spark(app_name: str) -> SparkSession:
    spark = (
        SparkSession.builder
        .master(SPARK_MASTER)
        .appName(app_name)
        .config("spark.driver.memory", DRIVER_MEMORY)
        .config("spark.sql.shuffle.partitions", SHUFFLE_PARTITIONS)
        # Cố định autoBroadcastJoinThreshold=-1 ở CẢ baseline & optimized
        # để tách biệt hoàn toàn tác động của 1 thay đổi duy nhất: F.broadcast(dim_df)
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def run_pipeline(mode: str, save_explain: Path | None = None) -> dict:
    spark = build_spark(f"affiliate-pipeline-{mode}")
    t0 = time.perf_counter()

    fact_path = (BENCH_DIR / "products_fact.parquet").as_posix()
    dim_path = (BENCH_DIR / "category_trends.parquet").as_posix()
    out_dir = (OUT_ROOT / mode).as_posix()

    fact_df = spark.read.parquet(fact_path).filter(F.col("price") > 0)
    dim_df = spark.read.parquet(dim_path)

    # --- ĐIỂM THAY ĐỔI DUY NHẤT (ONE CHANGE) ---
    if mode == "optimized":
        joined = fact_df.join(F.broadcast(dim_df), on="category_id", how="inner")
    else:
        joined = fact_df.join(dim_df, on="category_id", how="inner")
    # -------------------------------------------

    scored = (
        joined
        .withColumn("sold_score", F.least(F.lit(40.0), F.log1p(F.col("sold")) * F.lit(5.0)))
        .withColumn("rating_score", F.col("rating_average") * F.lit(4.0))
        .withColumn("discount_score", F.least(F.lit(15.0), F.col("discount_rate") * F.lit(0.3)))
        .withColumn("trend_boost", F.col("avg_trend_interest") * F.lit(0.25))
        .withColumn(
            "trend_score",
            F.round(
                F.col("sold_score") + F.col("rating_score") + F.col("discount_score") + F.col("trend_boost"),
                4,
            ),
        )
        .withColumn(
            "affiliate_label",
            F.when(F.col("trend_score") >= 65.0, F.lit("HOT"))
            .when(F.col("trend_score") >= 45.0, F.lit("RISING"))
            .when(F.col("trend_score") >= 25.0, F.lit("STABLE"))
            .otherwise(F.lit("COLD")),
        )
    )

    summary = (
        scored
        .groupBy("category_id", "category_name", "affiliate_label")
        .agg(
            F.count("*").alias("item_count"),
            F.round(F.avg("trend_score"), 4).alias("avg_trend_score"),
            F.sum(F.col("price") * F.col("sold")).alias("gmv_proxy"),
            F.min("trend_score").alias("min_score"),
            F.max("trend_score").alias("max_score"),
        )
        .orderBy("category_id", "affiliate_label")
    )

    if save_explain is not None:
        buf = io.StringIO()
        with redirect_stdout(buf):
            summary.explain("formatted")
        save_explain.parent.mkdir(parents=True, exist_ok=True)
        save_explain.write_text(buf.getvalue(), encoding="utf-8")

    summary.write.mode("overwrite").parquet(out_dir)

    # Thu thập correctness metrics từ output đã ghi
    out_df = spark.read.parquet(out_dir)
    rows = [r.asDict() for r in out_df.orderBy("category_id", "affiliate_label").collect()]
    elapsed = time.perf_counter() - t0

    total_items = sum(int(r["item_count"]) for r in rows)
    total_gmv = sum(float(r["gmv_proxy"]) for r in rows)
    label_dist: dict[str, int] = {}
    for r in rows:
        lbl = r["affiliate_label"]
        label_dist[lbl] = label_dist.get(lbl, 0) + int(r["item_count"])

    result = {
        "mode": mode,
        "spark_version": spark.version,
        "master": SPARK_MASTER,
        "driver_memory": DRIVER_MEMORY,
        "shuffle_partitions": SHUFFLE_PARTITIONS,
        "runtime_sec": round(elapsed, 4),
        "summary_rows": len(rows),
        "total_items": total_items,
        "total_gmv_proxy": round(total_gmv, 2),
        "label_distribution": label_dist,
        "schema": [(f.name, f.dataType.simpleString()) for f in out_df.schema.fields],
    }
    spark.stop()
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["baseline", "optimized"], required=True)
    parser.add_argument("--save-explain", type=Path, default=None)
    args = parser.parse_args()

    res = run_pipeline(args.mode, args.save_explain)
    print(json.dumps(res, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
