"""Harness chạy benchmark Đồ án 4 v2: 4 thí nghiệm có kiểm soát (E00..E03), 3 runs/thí nghiệm.

Ghi đầy đủ dấu vết khoa học:
- reports/benchmark.csv                (12 runs + Spark UI metrics)
- reports/explain-before.txt           (E00 baseline)
- reports/explain-e01.txt              (E01 broadcast join)
- reports/explain-after.txt            (E02 broadcast + shuffle.partitions=8)
- reports/spark_ui_stages_before.json  (Stage/task telemetry E00)
- reports/spark_ui_stages_after.json   (Stage/task telemetry E02)
- reports/benchmark_summary.json       (Tổng hợp median, variance, speedup, 7 correctness assertions)
"""
from __future__ import annotations

import csv
import json
import statistics
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from common import PROJECT_ROOT

REPORTS_DIR = PROJECT_ROOT / "reports"
PIPELINE = PROJECT_ROOT / "src" / "pipeline_affiliate_spark.py"

EXPERIMENTS = [
    ("baseline",  "E00", REPORTS_DIR / "explain-before.txt", REPORTS_DIR / "spark_ui_stages_before.json"),
    ("e01_bhj",   "E01", REPORTS_DIR / "explain-e01.txt",    None),
    ("e02_part",  "E02", REPORTS_DIR / "explain-after.txt",  REPORTS_DIR / "spark_ui_stages_after.json"),
    ("e03_cache", "E03", None,                               None),
]


def invoke_run(mode: str, save_explain: Path | None, save_stages: Path | None) -> dict:
    cmd = [sys.executable, str(PIPELINE), "--mode", mode]
    if save_explain is not None:
        cmd.extend(["--save-explain", str(save_explain)])
    if save_stages is not None:
        cmd.extend(["--save-stages", str(save_stages)])
    proc = subprocess.run(cmd, capture_output=True, text=True, check=True)
    return json.loads(proc.stdout)


def check_correctness(base: dict, cand: dict) -> dict[str, bool]:
    return {
        "row_count_match": base["total_items"] == cand["total_items"] and cand["total_items"] > 0,
        "summary_rows_match": base["summary_rows"] == cand["summary_rows"],
        "zero_null_or_dup_keys": cand["null_keys"] == 0 and cand["duplicate_keys"] == 0,
        "label_distribution_match": base["label_distribution"] == cand["label_distribution"],
        "gmv_proxy_match": abs(base["total_gmv_proxy"] - cand["total_gmv_proxy"]) < 1e-2,
        "schema_contract_match": base["schema"] == cand["schema"],
        "output_checksum_match": base["output_checksum"] == cand["output_checksum"],
    }


def main() -> int:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    runs_per_exp = 3

    records = []
    results_by_mode: dict[str, list[dict]] = {}

    for mode, exp_id, explain_file, stages_file in EXPERIMENTS:
        results_by_mode[mode] = []
        for run_idx in range(1, runs_per_exp + 1):
            ep = explain_file if run_idx == 1 else None
            sp = stages_file if run_idx == 1 else None
            print(f"[{exp_id}] Running {mode} #{run_idx}/{runs_per_exp}...", flush=True)
            res = invoke_run(mode, ep, sp)
            results_by_mode[mode].append(res)
            ui = res["ui_metrics"]
            records.append({
                "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                "exp_id": exp_id,
                "mode": mode,
                "run_id": run_idx,
                "runtime_sec": res["runtime_sec"],
                "shuffle_read_bytes": ui.get("shuffle_read_bytes", 0),
                "shuffle_write_bytes": ui.get("shuffle_write_bytes", 0),
                "shuffle_total_mb": ui.get("shuffle_total_mb", 0),
                "num_stages": ui.get("num_stages", 0),
                "num_tasks": ui.get("num_tasks", 0),
                "max_task_time_ms": ui.get("max_task_time_ms", 0),
                "median_task_time_ms": ui.get("median_task_time_ms", 0),
                "executor_run_time_ms": ui.get("executor_run_time_ms", 0),
                "disk_bytes_spilled": ui.get("disk_bytes_spilled", 0),
                "total_items": res["total_items"],
                "summary_rows": res["summary_rows"],
                "output_checksum": res["output_checksum"][:16],
                "spark_version": res["spark_version"],
                "master": res["master"],
                "driver_memory": res["driver_memory"],
                "shuffle_partitions": res["shuffle_partitions"],
            })
            print(
                f"  -> {res['runtime_sec']:.3f}s | shuffle={ui.get('shuffle_total_mb', 0):.2f}MB "
                f"| tasks={ui.get('num_tasks', 0)} | maxTask={ui.get('max_task_time_ms', 0)}ms",
                flush=True,
            )

    csv_path = REPORTS_DIR / "benchmark.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(records[0].keys()))
        writer.writeheader()
        writer.writerows(records)

    base_ref = results_by_mode["baseline"][0]
    base_med = statistics.median([r["runtime_sec"] for r in results_by_mode["baseline"]])

    exp_summaries: dict[str, dict] = {}
    all_correct = True
    for mode, exp_id, _, _ in EXPERIMENTS:
        runs = results_by_mode[mode]
        times = [r["runtime_sec"] for r in runs]
        med = statistics.median(times)
        spread_pct = round((max(times) - min(times)) / med * 100.0, 2) if med > 0 else 0.0
        imp_vs_base = round((base_med - med) / base_med * 100.0, 2) if base_med > 0 else 0.0

        assertions = check_correctness(base_ref, runs[0])
        passed = all(assertions.values())
        all_correct &= passed

        # Lấy median các Spark UI metrics
        med_shuf_read = int(statistics.median([r["ui_metrics"]["shuffle_read_bytes"] for r in runs]))
        med_shuf_write = int(statistics.median([r["ui_metrics"]["shuffle_write_bytes"] for r in runs]))
        med_max_task = int(statistics.median([r["ui_metrics"]["max_task_time_ms"] for r in runs]))
        med_median_task = round(statistics.median([r["ui_metrics"]["median_task_time_ms"] for r in runs]), 2)
        med_exec_ms = int(statistics.median([r["ui_metrics"]["executor_run_time_ms"] for r in runs]))

        exp_summaries[mode] = {
            "exp_id": exp_id,
            "runs_sec": times,
            "median_sec": round(med, 4),
            "spread_pct": spread_pct,
            "improvement_vs_baseline_pct": imp_vs_base,
            "shuffle_read_bytes": med_shuf_read,
            "shuffle_write_bytes": med_shuf_write,
            "shuffle_total_mb": round((med_shuf_read + med_shuf_write) / (1024 * 1024), 3),
            "num_stages": runs[0]["ui_metrics"]["num_stages"],
            "num_tasks": runs[0]["ui_metrics"]["num_tasks"],
            "max_task_time_ms": med_max_task,
            "median_task_time_ms": med_median_task,
            "executor_run_time_ms": med_exec_ms,
            "correctness_assertions": assertions,
            "correctness_pass": passed,
        }

    summary = {
        "spark_version": base_ref["spark_version"],
        "master": base_ref["master"],
        "driver_memory": base_ref["driver_memory"],
        "total_items": base_ref["total_items"],
        "summary_rows": base_ref["summary_rows"],
        "total_gmv_proxy": base_ref["total_gmv_proxy"],
        "min_score": base_ref["min_score"],
        "max_score": base_ref["max_score"],
        "label_distribution": base_ref["label_distribution"],
        "output_checksum": base_ref["output_checksum"],
        "experiments": exp_summaries,
        "all_correctness_pass": all_correct,
    }
    (REPORTS_DIR / "benchmark_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    print("\n=== BENCHMARK SUMMARY v2 ===")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if all_correct else 1


if __name__ == "__main__":
    raise SystemExit(main())
