"""Kiểm tra Spark đọc được raw data + đối chiếu manifest (checklist giảng viên).

    python src/verify_raw_spark.py

- Với mỗi dataset, lấy run SUCCESS mới nhất trong data/raw/.
- So sánh bytes + sha256 trong _manifest.json với file thật.
- Dùng Spark đọc sample (JSON Tiki, CSV Trends, XML RSS dạng text) và in schema/số dòng.
Kết quả lưu logs/verify_raw.txt
"""
from __future__ import annotations

import io
import json
import sys
from contextlib import redirect_stdout
from pathlib import Path

from common import PROJECT_ROOT, RAW_ROOT, sha256_file

OUT = PROJECT_ROOT / "logs" / "verify_raw.txt"


def latest_run(dataset: str) -> Path | None:
    runs = sorted((RAW_ROOT / dataset).glob("ingest_date=*/run_*"))
    runs = [r for r in runs if (r / "_SUCCESS").exists()]
    return runs[-1] if runs else None


def check_manifest(run_dir: Path) -> bool:
    m = json.loads((run_dir / "_manifest.json").read_text(encoding="utf-8"))
    ok = True
    for f in m["files"]:
        p = run_dir / f["file"]
        good = p.exists() and p.stat().st_size == f["bytes"] and sha256_file(p) == f["sha256"]
        ok &= good
        print(f"  [{'OK ' if good else 'BAD'}] {f['file']:40s} {f['bytes']:>10,d} B  sha256={f['sha256'][:12]}…")
    print(f"  manifest: status={m['status']} files={m['file_count']} records={m['total_records']}")
    return ok


def main() -> int:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    buf = io.StringIO()

    class Tee(io.TextIOBase):
        def write(self, s):
            sys.__stdout__.write(s)
            buf.write(s)
            return len(s)

    spark = (SparkSession.builder.master("local[*]").appName("verify-raw")
             .config("spark.ui.showConsoleProgress", "false").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    all_ok = True
    with redirect_stdout(Tee()):
        for ds in ("tiki_products", "google_trends", "google_trends_rss"):
            run = latest_run(ds)
            print(f"\n=== {ds} ===")
            if run is None:
                print("  (chưa có run SUCCESS)")
                continue
            print("  run:", run.relative_to(PROJECT_ROOT).as_posix())
            all_ok &= check_manifest(run)
            if ds == "tiki_products":
                files = [p.as_posix() for p in sorted(run.glob("cat=*_page=*.json"))]
                df = spark.read.option("multiLine", True).json(files)
                items = df.select(F.explode("data").alias("p")).select(
                    "p.id", "p.name", "p.price", "p.rating_average",
                    F.col("p.quantity_sold.value").alias("sold"))
                print("  Spark rows (products):", items.count())
                items.orderBy(F.desc("sold")).show(5, truncate=40)
            elif ds == "google_trends":
                files = [p.as_posix() for p in sorted(run.glob("interest_over_time_g*.csv"))]
                df = spark.read.option("header", True).option("inferSchema", True).csv(files)
                print("  Spark rows:", df.count(), "| columns:", len(df.columns))
                df.select(df.columns[:4]).show(3)
            else:
                files = [p.as_posix() for p in sorted(run.glob("*.xml"))]
                df = spark.read.text(files)
                titles = df.filter(F.col("value").contains("<title>")).count()
                print("  Spark lines:", df.count(), "| <title> lines:", titles)
        print("\nVERIFY RESULT:", "PASS" if all_ok else "FAIL")
    spark.stop()
    OUT.write_text(buf.getvalue(), encoding="utf-8")
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
