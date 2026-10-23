"""Smoke test môi trường: Python / Java / PySpark + một phép tính Spark.

    python src/env_check.py

Output in ra màn hình và lưu vào logs/env_check.txt (evidence cho Checkpoint 2).
"""
from __future__ import annotations

import io
import os
import platform
import subprocess
import sys
import time
from contextlib import redirect_stdout
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "logs" / "env_check.txt"

try:
    from dotenv import load_dotenv

    load_dotenv(ROOT / ".env")
except ImportError:
    pass
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


def java_version() -> str:
    try:
        p = subprocess.run(["java", "-version"], capture_output=True, text=True, timeout=30)
        return (p.stderr or p.stdout).strip().splitlines()[0]
    except Exception as e:
        return f"NOT FOUND ({e})"


def run_checks() -> bool:
    print("=" * 60)
    print("ENV CHECK -", datetime.now(timezone.utc).isoformat())
    print("=" * 60)
    print("OS        :", platform.platform())
    print("Python    :", sys.version.split()[0], "|", sys.executable)
    print("JAVA_HOME :", os.environ.get("JAVA_HOME", "(not set)"))
    print("Java      :", java_version())
    try:
        import pyspark
        from pyspark.sql import SparkSession
    except ImportError as e:
        print("PySpark   : NOT INSTALLED", e)
        print("RESULT    : FAIL")
        return False
    print("PySpark   :", pyspark.__version__)

    t0 = time.time()
    spark = (SparkSession.builder
             .master("local[*]")
             .appName("env-check")
             .config("spark.ui.showConsoleProgress", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    print("Spark     :", spark.version)
    print("Master    :", spark.sparkContext.master)
    print("Cores     :", spark.sparkContext.defaultParallelism)
    count = spark.range(1_000_000).count()
    total = spark.range(1, 101).selectExpr("sum(id) AS s").first()["s"]
    spark.stop()
    ok = count == 1_000_000 and total == 5050
    print("Count     :", count)
    print("Sum 1..100:", total)
    print(f"Elapsed   : {time.time() - t0:.1f}s")
    print("RESULT    :", "PASS" if ok else "FAIL")
    return ok


def main() -> int:
    buf = io.StringIO()

    class Tee(io.TextIOBase):
        def write(self, s):
            sys.__stdout__.write(s)
            buf.write(s)
            return len(s)

    with redirect_stdout(Tee()):
        ok = run_checks()
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(buf.getvalue(), encoding="utf-8")
    print(f"\n[saved] {OUT.relative_to(ROOT)}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
