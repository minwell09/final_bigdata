"""Harness chạy benchmark Đồ án 4 (≥3 runs baseline vs ≥3 runs optimized + correctness).

Ghi kết quả ra:
- reports/benchmark.csv
- reports/explain-before.txt
- reports/explain-after.txt
- reports/benchmark_summary.json
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


def invoke_run(mode: str, save_explain: Path | None = None) -> dict:
    cmd = [sys.executable, str(PIPELINE), "--mode", mode]
    if save_explain is not None:
        cmd.extend(["--save-explain", str(save_explain)])
    proc = subprocess.run(cmd, capture_output=True, text=True, check=True)
    return json.loads(proc.stdout)


def main() -> int:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    runs_per_mode = 3

    records = []
    results_by_mode: dict[str, list[dict]] = {"baseline": [], "optimized": []}

    for mode, explain_file in [
        ("baseline", REPORTS_DIR / "explain-before.txt"),
        ("optimized", REPORTS_DIR / "explain-after.txt"),
    ]:
        for run_idx in range(1, runs_per_mode + 1):
            exp_path = explain_file if run_idx == 1 else None
            print(f"Running {mode} #{run_idx}/{runs_per_mode}...", flush=True)
            res = invoke_run(mode, exp_path)
            results_by_mode[mode].append(res)
            records.append({
                "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                "mode": mode,
                "run_id": run_idx,
                "runtime_sec": res["runtime_sec"],
                "total_items": res["total_items"],
                "summary_rows": res["summary_rows"],
                "total_gmv_proxy": res["total_gmv_proxy"],
                "spark_version": res["spark_version"],
                "master": res["master"],
                "driver_memory": res["driver_memory"],
                "shuffle_partitions": res["shuffle_partitions"],
            })
            print(f"  -> {res['runtime_sec']:.3f}s (items={res['total_items']:,})", flush=True)

    # Ghi reports/benchmark.csv
    csv_path = REPORTS_DIR / "benchmark.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(records[0].keys()))
        writer.writeheader()
        writer.writerows(records)

    # Kiểm tra Correctness Assertions (Baseline vs Optimized)
    b0 = results_by_mode["baseline"][0]
    o0 = results_by_mode["optimized"][0]
    assertions = {
        "row_count_match": b0["total_items"] == o0["total_items"] and b0["total_items"] > 0,
        "summary_rows_match": b0["summary_rows"] == o0["summary_rows"],
        "label_distribution_match": b0["label_distribution"] == o0["label_distribution"],
        "gmv_proxy_match": abs(b0["total_gmv_proxy"] - o0["total_gmv_proxy"]) < 1e-2,
        "schema_contract_match": b0["schema"] == o0["schema"],
    }
    all_pass = all(assertions.values())

    b_times = [r["runtime_sec"] for r in results_by_mode["baseline"]]
    o_times = [r["runtime_sec"] for r in results_by_mode["optimized"]]
    b_med = statistics.median(b_times)
    o_med = statistics.median(o_times)
    speedup_pct = round((b_med - o_med) / b_med * 100.0, 2) if b_med > 0 else 0.0

    summary = {
        "baseline_runs_sec": b_times,
        "baseline_median_sec": round(b_med, 4),
        "optimized_runs_sec": o_times,
        "optimized_median_sec": round(o_med, 4),
        "improvement_pct": speedup_pct,
        "correctness_assertions": assertions,
        "correctness_pass": all_pass,
        "label_distribution": o0["label_distribution"],
        "total_items": o0["total_items"],
        "total_gmv_proxy": o0["total_gmv_proxy"],
        "spark_version": o0["spark_version"],
        "master": o0["master"],
        "driver_memory": o0["driver_memory"],
        "shuffle_partitions": o0["shuffle_partitions"],
    }
    (REPORTS_DIR / "benchmark_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    print("\n=== BENCHMARK SUMMARY ===")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if all_pass else 1


if __name__ == "__main__":
    raise SystemExit(main())
