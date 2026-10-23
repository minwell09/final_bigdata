"""Ingest danh sách tìm kiếm thịnh hành hằng ngày (Trending Now RSS) của Google Trends tại VN.

Nguồn ổn định, không cần API key -> dùng làm tín hiệu "xu hướng nóng" theo ngày.
Raw lưu nguyên bytes XML trả về:
    data/raw/google_trends_rss/ingest_date=YYYY-MM-DD/run_<UTC>/trending_VN.xml

Chạy:
    python src/ingest_trends_rss.py
"""
from __future__ import annotations

import sys
import xml.etree.ElementTree as ET

import requests

from common import IngestRun, get_logger, load_config

log = get_logger("ingest_trends_rss")
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) BigData-Student-Project/1.0"


def main() -> int:
    cfg = load_config()["google_trends_rss"]
    run = IngestRun(
        dataset="google_trends_rss",
        source={
            "name": "Google Trends - Trending Now RSS (VN)",
            "url": cfg["url"],
            "access": "HTTP GET public RSS",
            "terms": "Google Terms of Service; snapshot theo thời điểm ingest",
        },
        params={"url": cfg["url"]},
    )
    log.info("Bắt đầu %s -> %s", run.run_id, run.dest_dir)
    try:
        r = requests.get(cfg["url"], headers={"User-Agent": UA}, timeout=30)
        r.raise_for_status()
        items = len(ET.fromstring(r.content).findall("./channel/item"))  # validate XML hợp lệ
        if items == 0:
            raise RuntimeError("RSS không có item")
        run.write_bytes("trending_VN.xml", r.content, records=items,
                        http_status=r.status_code, content_type=r.headers.get("Content-Type"))
        log.info("OK: %d trending items", items)
    except Exception as e:
        run.errors.append(str(e))
        log.error("Lỗi: %s", e)
    m = run.finish()
    log.info("Kết thúc %s | status=%s | bytes=%d | records=%d",
             m["run_id"], m["status"], m["total_bytes"], m["total_records"])
    return 0 if m["status"] != "FAILED" else 1


if __name__ == "__main__":
    sys.exit(main())
