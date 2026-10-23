"""Ingest danh sách sản phẩm theo danh mục từ Tiki (public listing API).

Raw lưu nguyên JSON response của từng trang (không parse/sửa):
    data/raw/tiki_products/ingest_date=YYYY-MM-DD/run_<UTC>/
        cat=<category_id>_page=<n>.json
        _manifest.json

Chạy:
    python src/ingest_tiki_products.py [--pages 3] [--limit 40]
"""
from __future__ import annotations

import argparse
import json
import sys
import time

import requests

from common import IngestRun, get_logger, load_config

log = get_logger("ingest_tiki_products")
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "vi-VN,vi;q=0.9",
    "Referer": "https://tiki.vn/",
}


def fetch_page(session: requests.Session, base_url: str, cat: dict, page: int, limit: int):
    params = {
        "limit": limit,
        "include": "advertisement",
        "aggregations": 2,
        "category": cat["id"],
        "page": page,
        "urlKey": cat["url_key"],
    }
    for attempt in range(1, 4):
        r = session.get(base_url, params=params, headers=HEADERS, timeout=30)
        if r.status_code == 429:
            time.sleep(5 * attempt)
            continue
        r.raise_for_status()
        return r
    r.raise_for_status()
    return r


def main() -> int:
    cfg = load_config()["tiki"]
    ap = argparse.ArgumentParser()
    ap.add_argument("--pages", type=int, default=cfg["pages_per_category"])
    ap.add_argument("--limit", type=int, default=cfg["limit"])
    args = ap.parse_args()

    run = IngestRun(
        dataset="tiki_products",
        source={
            "name": "Tiki.vn public listing API",
            "url": cfg["base_url"],
            "access": "HTTP GET public JSON (không cần đăng nhập)",
            "terms": "Dữ liệu công khai trên tiki.vn; thu thập tần suất thấp, chỉ dùng học tập",
        },
        params={"pages": args.pages, "limit": args.limit, "categories": cfg["categories"]},
    )
    log.info("Bắt đầu %s -> %s", run.run_id, run.dest_dir)

    with requests.Session() as s:
        for cat in cfg["categories"]:
            for page in range(1, args.pages + 1):
                try:
                    r = fetch_page(s, cfg["base_url"], cat, page, args.limit)
                    body = r.json()  # validate JSON hợp lệ, nhưng vẫn lưu bytes gốc
                    n = len(body.get("data", []))
                    if n == 0:
                        log.info("cat=%s page=%d rỗng -> dừng danh mục", cat["id"], page)
                        break
                    run.write_bytes(f"cat={cat['id']}_page={page}.json", r.content, records=n,
                                    category_id=cat["id"], category_name=cat["name"], page=page,
                                    http_status=r.status_code)
                    log.info("cat=%s (%s) page=%d: %d sản phẩm", cat["id"], cat["name"], page, n)
                except (requests.RequestException, json.JSONDecodeError, ValueError) as e:
                    msg = f"cat={cat['id']} page={page}: {e}"
                    run.errors.append(msg)
                    log.error(msg)
                time.sleep(cfg["sleep_between_requests_sec"])

    m = run.finish()
    log.info("Kết thúc %s | status=%s | files=%d | bytes=%d | records=%d",
             m["run_id"], m["status"], m["file_count"], m["total_bytes"], m["total_records"])
    return 0 if m["status"] != "FAILED" else 1


if __name__ == "__main__":
    sys.exit(main())
