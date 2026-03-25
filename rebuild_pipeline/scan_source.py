"""
Phase 1: Walk the source filesystem and build a manifest of all .docx files
with extracted metadata (patient name, study date, year, relative path, SHA-256).
"""

import hashlib
import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from pathlib import Path

from config import (
    ARTIFACTS_DIR,
    EXCLUDED_DIRS,
    HASH_WORKERS,
    MANIFEST_PATH,
    ROOT_DIR,
)
from normalize import (
    clean_patient_name,
    extract_study_date_and_year,
    normalize_name_key,
)


@dataclass
class ManifestEntry:
    abs_path: str
    relative_path: str
    filename: str
    patient_name: str
    normalized_name: str
    study_date: str | None  # YYYY-MM-DD or None
    year: str | None
    content_sha256: str
    s3_key: str
    skip_reason: str | None = None


def _is_excluded(dirname: str) -> bool:
    lower = dirname.lower()
    if lower in EXCLUDED_DIRS:
        return True
    if "form" in lower:
        return True
    return False


def walk_docx_files(root: Path) -> list[Path]:
    results: list[Path] = []
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if not _is_excluded(d)]
        for fn in filenames:
            if fn.lower().endswith(".docx"):
                results.append(Path(dirpath) / fn)
    return results


def _hash_file(filepath: Path) -> str:
    h = hashlib.sha256()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _build_s3_key(relative_path: str) -> str:
    """Collision-proof key derived from the full relative source path."""
    normalized = relative_path.replace("\\", "/")
    return "reports/" + normalized


def build_manifest_entry(abs_path: Path, root: Path) -> ManifestEntry:
    relative = str(abs_path.relative_to(root))
    filename = abs_path.name
    patient_name = clean_patient_name(filename)
    normalized = normalize_name_key(patient_name) if patient_name else ""
    study_date, year = extract_study_date_and_year(relative)

    skip_reason = None
    if not patient_name:
        skip_reason = "empty_name_after_cleaning"
    elif not year:
        skip_reason = "no_year_in_path"

    content_sha256 = _hash_file(abs_path) if not skip_reason else ""
    s3_key = _build_s3_key(relative) if not skip_reason else ""

    return ManifestEntry(
        abs_path=str(abs_path),
        relative_path=relative,
        filename=filename,
        patient_name=patient_name,
        normalized_name=normalized,
        study_date=study_date.isoformat() if study_date else None,
        year=year,
        content_sha256=content_sha256,
        s3_key=s3_key,
        skip_reason=skip_reason,
    )


def scan_and_build_manifest() -> list[ManifestEntry]:
    root = ROOT_DIR
    print(f"Scanning {root} ...")
    all_files = walk_docx_files(root)
    print(f"Found {len(all_files)} .docx files")

    entries: list[ManifestEntry] = []
    errors = 0

    print(f"Building manifest (hashing with {HASH_WORKERS} workers) ...")
    with ThreadPoolExecutor(max_workers=HASH_WORKERS) as pool:
        futures = {
            pool.submit(build_manifest_entry, fp, root): fp
            for fp in all_files
        }
        for i, future in enumerate(as_completed(futures), 1):
            try:
                entry = future.result()
                entries.append(entry)
            except Exception as exc:
                fp = futures[future]
                errors += 1
                print(f"  [ERROR] {fp}: {exc}", file=sys.stderr)

            if i % 2000 == 0 or i == len(all_files):
                print(f"  manifest progress: {i}/{len(all_files)}", end="\r")

    print()

    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    with open(MANIFEST_PATH, "w", encoding="utf-8") as f:
        for entry in entries:
            f.write(json.dumps(asdict(entry), ensure_ascii=False) + "\n")

    skipped = sum(1 for e in entries if e.skip_reason)
    valid = len(entries) - skipped
    unique_patients = len({e.normalized_name for e in entries if not e.skip_reason})
    unique_hashes = len({e.content_sha256 for e in entries if not e.skip_reason})

    print(f"\nManifest built: {MANIFEST_PATH}")
    print(f"  total entries:    {len(entries)}")
    print(f"  valid:            {valid}")
    print(f"  skipped:          {skipped}")
    print(f"  errors:           {errors}")
    print(f"  unique patients:  {unique_patients}")
    print(f"  unique files:     {unique_hashes}")

    return entries


def load_manifest() -> list[ManifestEntry]:
    entries = []
    with open(MANIFEST_PATH, "r", encoding="utf-8") as f:
        for line in f:
            d = json.loads(line)
            entries.append(ManifestEntry(**d))
    return entries


if __name__ == "__main__":
    scan_and_build_manifest()
