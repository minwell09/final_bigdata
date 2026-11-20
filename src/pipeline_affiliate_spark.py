"""Spark Pipeline Đồ án 4: Tính Trend Score & Gắn nhãn Affiliate (HOT/RISING/STABLE/COLD).

Thu thập trực tiếp Spark UI Stage/Task Metrics (shuffleReadBytes, shuffleWriteBytes,
maxTaskTimeMs, medianTaskTimeMs, totalTasks, stages, spill) qua Spark REST API
trước khi dừng SparkSession.

Hỗ trợ các chế độ thực nghiệm có kiểm soát (mỗi bước thay đúng 1 biến):
- baseline : Spark mặc định (shuffle.partitions=200, SortMergeJoin tự nhiên do trends_dim.csv > 10MB, no cache)
- e01_bhj  : + F.broadcast(dim_df) sau khi chọn 3 cột cần thiết (shuffle.partitions=200 giữ nguyên)
- e02_part : + giảm spark.sql.shuffle.partitions từ 200 -> 8 (giữ e01_bhj)
- e03_cache: + scored.cache() trên pipeline 1-pass (kiểm chứng negative control / anti-pattern)
"""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import statistics
import time
import urllib.request
from contextlib import redirect_stdout
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common import PROJECT_ROOT

BENCH_DIR = PROJECT_ROOT / "data" / "benchmark"
OUT_ROOT = PROJECT_ROOT / "data" / "output"

SPARK_MASTER = "local[2]"
DRIVER_MEMORY = "2g"

MODE_CONFIGS = {
    "baseline":  {"broadcast": False, "shuffle_partitions": 200, "cache_scored": False},
    "e01_bhj":   {"broadcast": True,  "shuffle_partitions": 200, "cache_scored": False},
    "e02_part":  {"broadcast": True,  "shuffle_partitions": 8,   "cache_scored": False},
    "e03_cache": {"broadcast": True,  "shuffle_partitions": 8,   "cache_scored": True},
}


def build_spark(app_name: str, shuffle_partitions: int) -> SparkSession:
    spark = (
        SparkSession.builder
        .master(SPARK_MASTER)
        .appName(app_name)
        .config("spark.driver.memory", DRIVER_MEMORY)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        # Giữ nguyên autoBroadcastJoinThreshold mặc định của Spark (10 MB = 10485760 B)
        # Tắt AQE để cô lập rõ từng thay đổi thực nghiệm (1 biến / experiment)
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.ui.enabled", "true")
        .config("spark.ui.port", "0")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def fetch_spark_ui_metrics(spark: SparkSession) -> tuple[dict, list[dict]]:
    """Đọc trực tiếp stage & task metrics từ Spark UI REST API."""
    ui_url = spark.sparkContext.uiWebUrl
    app_id = spark.sparkContext.applicationId
    if not ui_url:
        return {}, []
    url = f"{ui_url}/api/v1/applications/{app_id}/stages?details=true"
    with urllib.request.urlopen(url, timeout=10) as resp:
        stages = json.loads(resp.read().decode("utf-8"))

    completed = [s for s in stages if s.get("status") == "COMPLETE"]
    shuffle_read_bytes = sum(int(s.get("shuffleReadBytes", 0)) for s in completed)
    shuffle_write_bytes = sum(int(s.get("shuffleWriteBytes", 0)) for s in completed)
    input_bytes = sum(int(s.get("inputBytes", 0)) for s in completed)
    executor_run_ms = sum(int(s.get("executorRunTime", 0)) for s in completed)
    disk_spill_bytes = sum(int(s.get("diskBytesSpilled", 0)) for s in completed)
    mem_spill_bytes = sum(int(s.get("memoryBytesSpilled", 0)) for s in completed)

    task_durations: list[int] = []
    stage_summaries: list[dict] = []
    for s in completed:
        tasks_dict = s.get("tasks") or {}
        stage_durs = [int(t.get("duration", 0)) for t in tasks_dict.values()]
        task_durations.extend(stage_durs)
        stage_summaries.append({
            "stageId": s.get("stageId"),
            "name": s.get("name"),
            "numTasks": s.get("numTasks"),
            "executorRunTimeMs": s.get("executorRunTime", 0),
            "shuffleReadBytes": s.get("shuffleReadBytes", 0),
            "shuffleWriteBytes": s.get("shuffleWriteBytes", 0),
            "maxTaskMs": max(stage_durs) if stage_durs else 0,
            "medianTaskMs": int(statistics.median(stage_durs)) if stage_durs else 0,
        })

    agg = {
        "num_stages": len(completed),
        "num_tasks": len(task_durations),
        "shuffle_read_bytes": shuffle_read_bytes,
        "shuffle_write_bytes": shuffle_write_bytes,
        "shuffle_total_mb": round((shuffle_read_bytes + shuffle_write_bytes) / (1024 * 1024), 3),
        "input_mb": round(input_bytes / (1024 * 1024), 3),
        "executor_run_time_ms": executor_run_ms,
        "max_task_time_ms": max(task_durations) if task_durations else 0,
        "median_task_time_ms": round(statistics.median(task_durations), 2) if task_durations else 0.0,
        "disk_bytes_spilled": disk_spill_bytes,
        "memory_bytes_spilled": mem_spill_bytes,
    }
    return agg, stage_summaries


def run_pipeline(
    mode: str,
    save_explain: Path | None = None,
    save_stages: Path | None = None,
) -> dict:
    cfg = MODE_CONFIGS[mode]
    spark = build_spark(f"affiliate-cp4-{mode}", cfg["shuffle_partitions"])

    fact_path = (BENCH_DIR / "products_fact.parquet").as_posix()
    dim_path = (BENCH_DIR / "trends_dim.csv").as_posix()
    out_dir = (OUT_ROOT / mode).as_posix()

    t0 = time.perf_counter()

    fact_df = spark.read.parquet(fact_path).filter(F.col("price") > 0)
    dim_df = (
        spark.read.option("header", True)
        .option("inferSchema", False)
        .csv(dim_path)
        .select(
            F.col("trend_key"),
            F.col("category_name"),
            F.col("avg_trend_interest").cast("double").alias("avg_trend_interest"),
        )
    )

    # --- E01: Broadcast Join vs SortMergeJoin mặc định ---
    if cfg["broadcast"]:
        joined = fact_df.join(F.broadcast(dim_df), on="trend_key", how="inner")
    else:
        joined = fact_df.join(dim_df, on="trend_key", how="inner")

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

    # --- E03: Negative control (cache DataFrame chỉ dùng 1 lần) ---
    if cfg["cache_scored"]:
        scored = scored.cache()
        scored.count()

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
    elapsed = time.perf_counter() - t0

    # Thu thập Spark UI metrics của chính job pipeline (trước khi chạy bước verify)
    ui_metrics, stage_summaries = fetch_spark_ui_metrics(spark)
    if save_stages is not None:
        save_stages.parent.mkdir(parents=True, exist_ok=True)
        save_stages.write_text(json.dumps(stage_summaries, indent=2), encoding="utf-8")

    # Bước verify correctness (đọc lại output đã ghi)
    out_df = spark.read.parquet(out_dir)
    rows = [r.asDict() for r in out_df.orderBy("category_id", "affiliate_label").collect()]
    null_keys = out_df.filter(
        F.col("category_id").isNull() | F.col("affiliate_label").isNull()
    ).count()
    dup_keys = len(rows) - out_df.select("category_id", "affiliate_label").distinct().count()

    canonical = json.dumps(rows, sort_keys=True, ensure_ascii=False)
    output_checksum = hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    total_items = sum(int(r["item_count"]) for r in rows)
    total_gmv = sum(float(r["gmv_proxy"]) for r in rows)
    min_score = min(float(r["min_score"]) for r in rows)
    max_score = max(float(r["max_score"]) for r in rows)
    label_dist: dict[str, int] = {}
    for r in rows:
        lbl = r["affiliate_label"]
        label_dist[lbl] = label_dist.get(lbl, 0) + int(r["item_count"])

    result = {
        "mode": mode,
        "config": cfg,
        "spark_version": spark.version,
        "master": SPARK_MASTER,
        "driver_memory": DRIVER_MEMORY,
        "shuffle_partitions": cfg["shuffle_partitions"],
        "runtime_sec": round(elapsed, 4),
        "ui_metrics": ui_metrics,
        "summary_rows": len(rows),
        "total_items": total_items,
        "null_keys": null_keys,
        "duplicate_keys": dup_keys,
        "total_gmv_proxy": round(total_gmv, 2),
        "min_score": min_score,
        "max_score": max_score,
        "label_distribution": label_dist,
        "output_checksum": output_checksum,
        "schema": [(f.name, f.dataType.simpleString()) for f in out_df.schema.fields],
    }
    spark.stop()
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=list(MODE_CONFIGS.keys()), required=True)
    parser.add_argument("--save-explain", type=Path, default=None)
    parser.add_argument("--save-stages", type=Path, default=None)
    args = parser.parse_args()

    res = run_pipeline(args.mode, args.save_explain, args.save_stages)
    print(json.dumps(res, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
