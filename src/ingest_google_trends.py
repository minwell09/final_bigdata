"""Ingest Google Trends (interest over time + related queries) cho các từ khoá sản phẩm.

Nguồn: Google Trends qua thư viện pytrends (unofficial API).
Raw lưu nguyên dạng output của pytrends, KHÔNG biến đổi:
    data/raw/google_trends/ingest_date=YYYY-MM-DD/run_<UTC>/
        interest_over_time_g<k>.csv
        related_queries_g<k>.json
        _manifest.json

Chạy:
    python src/ingest_google_trends.py [--timeframe "today 3-m"] [--geo VN]
"""
from __future__ import annotations

import argparse
import json
import sys
import time

from common import IngestRun, get_logger, load_config

log = get_logger("ingest_google_trends")


def build_client(cfg: dict):
    from pytrends.request import TrendReq

    return TrendReq(hl=cfg["hl"], tz=cfg["tz"], timeout=(10, 30))


def fetch_group(client, keywords: list[str], timeframe: str, geo: str, max_retries: int):
    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            client.build_payload(keywords, timeframe=timeframe, geo=geo)
            iot = client.interest_over_time()
            try:
                rq = client.related_queries()
            except Exception as e:  # related_queries hay lỗi độc lập, không chặn cả nhóm
                log.warning("related_queries lỗi cho %s: %s", keywords, e)
                rq = {}
            return iot, rq
        except Exception as e:
            last_err = e
            wait = 20 * attempt
            log.warning("Lần %d/%d lỗi (%s). Đợi %ds rồi thử lại...", attempt, max_retries, e, wait)
            time.sleep(wait)
    raise RuntimeError(f"Hết retry cho {keywords}: {last_err}")


def related_to_json(rq: dict) -> dict:
    out = {}
    for kw, parts in (rq or {}).items():
        out[kw] = {}
        for kind in ("top", "rising"):
            df = (parts or {}).get(kind)
            out[kw][kind] = None if df is None else df.to_dict(orient="records")
    return out


def main() -> int:
    cfg_all = load_config()
    cfg = cfg_all["google_trends"]
    ap = argparse.ArgumentParser()
    ap.add_argument("--timeframe", default=cfg["timeframe"])
    ap.add_argument("--geo", default=cfg["geo"])
    args = ap.parse_args()

    run = IngestRun(
        dataset="google_trends",
        source={
            "name": "Google Trends",
            "url": "https://trends.google.com/trends/explore",
            "access": "pytrends (unofficial API wrapper)",
            "terms": "Google Terms of Service; dữ liệu chỉ số tương đối 0-100, dùng cho mục đích học tập",
        },
        params={"timeframe": args.timeframe, "geo": args.geo, "keyword_groups": cfg["keyword_groups"]},
    )
    log.info("Bắt đầu %s -> %s", run.run_id, run.dest_dir)

    client = build_client(cfg)
    for i, group in enumerate(cfg["keyword_groups"], start=1):
        try:
            iot, rq = fetch_group(client, group, args.timeframe, args.geo, cfg["max_retries"])
            if iot is None or iot.empty:
                raise RuntimeError("interest_over_time rỗng")
            csv_bytes = iot.reset_index().to_csv(index=False).encode("utf-8")
            run.write_bytes(f"interest_over_time_g{i}.csv", csv_bytes, records=len(iot), keywords=group)
            rq_bytes = json.dumps(related_to_json(rq), ensure_ascii=False, indent=2, default=str).encode("utf-8")
            run.write_bytes(f"related_queries_g{i}.json", rq_bytes, records=0, keywords=group)
            log.info("Nhóm %d OK: %d dòng", i, len(iot))
        except Exception as e:
            msg = f"group {i} {group}: {e}"
            run.errors.append(msg)
            log.error(msg)
        time.sleep(cfg["sleep_between_requests_sec"])

    m = run.finish()
    log.info("Kết thúc %s | status=%s | files=%d | bytes=%d | records=%d",
             m["run_id"], m["status"], m["file_count"], m["total_bytes"], m["total_records"])
    return 0 if m["status"] != "FAILED" else 1


if __name__ == "__main__":
    sys.exit(main())
