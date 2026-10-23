"""Chạy toàn bộ ingestion (3 nguồn) trong một lệnh.

    python src/ingest_all.py
    python src/ingest_all.py --only tiki rss
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

SRC = Path(__file__).resolve().parent
JOBS = {
    "rss": "ingest_trends_rss.py",
    "trends": "ingest_google_trends.py",
    "tiki": "ingest_tiki_products.py",
}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", nargs="*", choices=list(JOBS), default=list(JOBS))
    args = ap.parse_args()
    results = {}
    for key in args.only:
        print(f"\n===== {key}: {JOBS[key]} =====", flush=True)
        results[key] = subprocess.call([sys.executable, str(SRC / JOBS[key])])
    print("\n===== SUMMARY =====")
    for k, rc in results.items():
        print(f"{k:8s} -> {'OK' if rc == 0 else 'FAILED'} (exit={rc})")
    return 0 if all(rc == 0 for rc in results.values()) else 1


if __name__ == "__main__":
    sys.exit(main())
