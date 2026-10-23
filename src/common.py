"""Tiện ích dùng chung cho các ingestion job.

- Sinh run_id theo UTC timestamp.
- Tạo đường dẫn raw theo quy ước:
      data/raw/<dataset>/ingest_date=YYYY-MM-DD/run_<UTC_TIMESTAMP>/
- Tính sha256 / bytes cho file raw.
- Ghi manifest.json trong thư mục run và append vào metadata/ingest_log.jsonl.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import platform
import socket
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

try:  # nạp .env nếu có (RAW_ROOT, JAVA_HOME...)
    from dotenv import load_dotenv

    load_dotenv(PROJECT_ROOT / ".env")
except ImportError:
    pass
# Spark worker trên Windows phải dùng đúng python của venv
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
RAW_ROOT = Path(os.getenv("RAW_ROOT", "data/raw"))
if not RAW_ROOT.is_absolute():
    RAW_ROOT = (PROJECT_ROOT / RAW_ROOT).resolve()
LOG_DIR = PROJECT_ROOT / "logs"
METADATA_DIR = PROJECT_ROOT / "metadata"
INGEST_LOG = METADATA_DIR / "ingest_log.jsonl"
CONFIG_PATH = PROJECT_ROOT / "config" / "sources.json"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def load_config() -> dict:
    with open(CONFIG_PATH, encoding="utf-8") as f:
        return json.load(f)


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def get_logger(name: str) -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    fh = logging.FileHandler(LOG_DIR / f"{name}.log", encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(sh)
    logger.addHandler(fh)
    return logger


def previous_hashes(dataset: str) -> dict[str, str]:
    """Trả về {sha256: run_id} của các lần ingest SUCCESS trước đó cho dataset."""
    out: dict[str, str] = {}
    if not INGEST_LOG.exists():
        return out
    with open(INGEST_LOG, encoding="utf-8") as f:
        for line in f:
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            if rec.get("dataset") != dataset or rec.get("status") != "SUCCESS":
                continue
            for fi in rec.get("files", []):
                out.setdefault(fi["sha256"], rec["run_id"])
    return out


@dataclass
class IngestRun:
    """Một lần chạy ingest = một thư mục run_<UTC> bất biến + một manifest."""

    dataset: str
    source: dict
    params: dict = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.started_at = utc_now()
        self.run_id = "run_" + self.started_at.strftime("%Y%m%dT%H%M%S%fZ")
        self.ingest_date = self.started_at.strftime("%Y-%m-%d")
        self.dest_dir = RAW_ROOT / self.dataset / f"ingest_date={self.ingest_date}" / self.run_id
        self.dest_dir.mkdir(parents=True, exist_ok=False)  # không bao giờ ghi đè run cũ
        self.files: list[dict] = []
        self.errors: list[str] = []
        self.record_count = 0
        self._known = previous_hashes(self.dataset)

    def write_bytes(self, filename: str, data: bytes, records: int = 0, **extra) -> Path:
        path = self.dest_dir / filename
        with open(path, "wb") as f:
            f.write(data)
        digest = sha256_file(path)
        info = {
            "file": filename,
            "path": str(path.relative_to(PROJECT_ROOT)).replace("\\", "/"),
            "bytes": path.stat().st_size,
            "sha256": digest,
            "records": records,
            "duplicate_of_run": self._known.get(digest),
            **extra,
        }
        self.files.append(info)
        self.record_count += records
        return path

    def finish(self, status: str | None = None) -> dict:
        ended = utc_now()
        if status is None:
            if not self.files:
                status = "FAILED"
            elif self.errors:
                status = "PARTIAL"
            else:
                status = "SUCCESS"
        manifest = {
            "run_id": self.run_id,
            "dataset": self.dataset,
            "status": status,
            "source": self.source,
            "params": self.params,
            "ingest_started_utc": self.started_at.isoformat(),
            "ingest_ended_utc": ended.isoformat(),
            "duration_sec": round((ended - self.started_at).total_seconds(), 3),
            "destination": str(self.dest_dir.relative_to(PROJECT_ROOT)).replace("\\", "/"),
            "file_count": len(self.files),
            "total_bytes": sum(f["bytes"] for f in self.files),
            "total_records": self.record_count,
            "files": self.files,
            "errors": self.errors,
            "job": {
                "script": Path(sys.argv[0]).name,
                "host": socket.gethostname(),
                "python": platform.python_version(),
                "git_commit": _git_commit(),
            },
        }
        with open(self.dest_dir / "_manifest.json", "w", encoding="utf-8") as f:
            json.dump(manifest, f, ensure_ascii=False, indent=2)
        if status in ("SUCCESS", "PARTIAL"):
            (self.dest_dir / "_SUCCESS").touch()
        METADATA_DIR.mkdir(parents=True, exist_ok=True)
        with open(INGEST_LOG, "a", encoding="utf-8") as f:
            f.write(json.dumps(manifest, ensure_ascii=False) + "\n")
        return manifest


def _git_commit() -> str | None:
    head = PROJECT_ROOT / ".git" / "HEAD"
    try:
        ref = head.read_text().strip()
        if ref.startswith("ref:"):
            ref_path = PROJECT_ROOT / ".git" / ref.split(" ", 1)[1]
            if ref_path.exists():
                return ref_path.read_text().strip()[:12]
            packed = PROJECT_ROOT / ".git" / "packed-refs"
            if packed.exists():
                for line in packed.read_text().splitlines():
                    if line.endswith(ref.split(" ", 1)[1]):
                        return line.split(" ")[0][:12]
            return None
        return ref[:12]
    except OSError:
        return None
