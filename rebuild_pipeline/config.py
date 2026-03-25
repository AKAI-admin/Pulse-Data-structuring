"""
Central configuration — reads from environment variables with sensible defaults.
Copy .env.example to .env and fill in values before running.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

_pkg_dir = Path(__file__).resolve().parent
_repo_root = _pkg_dir.parent
# Load .env from repo root first, then optional rebuild_pipeline/.env (override)
for _env_candidate in (_repo_root / ".env", _pkg_dir / ".env"):
    if _env_candidate.is_file():
        load_dotenv(_env_candidate, override=True)


def _env_str(key: str) -> str:
    v = os.environ.get(key, "") or ""
    v = v.strip()
    if len(v) >= 2 and v[0] in "\"'" and v[0] == v[-1]:
        v = v[1:-1]
    return v.strip()

# ── Source filesystem ────────────────────────────────────────────────────────
ROOT_DIR = Path(os.environ.get("ROOT_DIR", r"D:\report\PET-CT Reports"))

# ── AWS / S3 ─────────────────────────────────────────────────────────────────
S3_BUCKET = _env_str("S3_BUCKET_NAME_V2") or _env_str("S3_BUCKET_NAME") or _env_str("AWS_S3_BUCKET")
AWS_REGION = _env_str("AWS_REGION") or "ap-south-1"
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "")

S3_KEY_PREFIX = "reports"
S3_UPLOAD_WORKERS = int(os.environ.get("S3_UPLOAD_WORKERS", "12"))

# ── MongoDB ──────────────────────────────────────────────────────────────────
MONGO_URI = os.environ.get("MONGO_URI", "")
MONGO_STANDARD_URI = (
    "mongodb://pulselabs13_db_user:tDZwThFhIber30yY@"
    "ac-mmcizf1-shard-00-00.hcdmbzo.mongodb.net:27017,"
    "ac-mmcizf1-shard-00-01.hcdmbzo.mongodb.net:27017,"
    "ac-mmcizf1-shard-00-02.hcdmbzo.mongodb.net:27017/pulse"
    "?ssl=true&replicaSet=atlas-z2o3xu-shard-0&authSource=admin&appName=Cluster0"
)
MONGO_DB_NAME = os.environ.get("MONGO_DB_NAME", "pulse")
PATIENTS_COLLECTION = "patients_v2"
SCANS_COLLECTION = "scans_v2"
MONGO_BATCH_SIZE = int(os.environ.get("MONGO_BATCH_SIZE", "1000"))

# ── Pipeline tuning ──────────────────────────────────────────────────────────
HASH_WORKERS = int(os.environ.get("HASH_WORKERS", "6"))

# ── Filesystem exclusions (carried over from old script) ─────────────────────
EXCLUDED_DIRS = {"images", "ravi"}

# ── Artifacts ────────────────────────────────────────────────────────────────
ARTIFACTS_DIR = Path(__file__).parent / "artifacts"
MANIFEST_PATH = ARTIFACTS_DIR / "manifest.jsonl"
PROGRESS_PATH = ARTIFACTS_DIR / "progress.jsonl"
REPORT_PATH = ARTIFACTS_DIR / "ingestion_report.json"
