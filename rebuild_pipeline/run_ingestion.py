#!/usr/bin/env python3
"""
Main orchestrator for the rebuild pipeline.

Usage:
    python run_ingestion.py --dry-run              # scan manifest, preview stats
    python run_ingestion.py --apply                # full: S3 upload + Mongo
    python run_ingestion.py --apply --skip-scan --mongo-only   # Mongo only (S3 already done)

The pipeline runs in stages:
  1. Scan source filesystem and build manifest (with file hashing)
  2. Upload to S3 concurrently
  3. Write patients + scans to new Mongo collections in batch
  4. Print summary report
"""

import argparse
import json
import time
from pathlib import Path

from config import ARTIFACTS_DIR, MANIFEST_PATH, REPORT_PATH, S3_BUCKET
from scan_source import load_manifest, scan_and_build_manifest
from upload_s3 import build_report_url_map, upload_files
from write_mongo import write_patients_and_scans


def main():
    parser = argparse.ArgumentParser(description="Rebuild PET-CT report dataset")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true", help="Preview only, no S3/Mongo writes")
    mode.add_argument("--apply", action="store_true", help="Full rebuild: upload + write")
    parser.add_argument(
        "--skip-scan",
        action="store_true",
        help="Skip filesystem scan; reuse existing manifest",
    )
    parser.add_argument(
        "--mongo-only",
        action="store_true",
        help="With --apply: skip S3 upload; only write Mongo (use after files are already in S3). Implies building report URLs from manifest.",
    )
    args = parser.parse_args()

    if args.mongo_only and not args.apply:
        parser.error("--mongo-only requires --apply")

    dry_run = args.dry_run
    mongo_only = args.mongo_only

    if mongo_only:
        label = "APPLY — MONGO ONLY"
    elif dry_run:
        label = "DRY-RUN"
    else:
        label = "APPLY"
    print(f"{'=' * 60}")
    print(f"  Rebuild Pipeline — {label}")
    print(f"{'=' * 60}\n")

    t0 = time.time()

    # ── Phase 1: Scan & Manifest ─────────────────────────────────────────────
    if args.skip_scan and MANIFEST_PATH.exists():
        print(f"Reusing existing manifest: {MANIFEST_PATH}\n")
        entries = load_manifest()
        print(f"Loaded {len(entries)} entries from manifest")
    else:
        entries = scan_and_build_manifest()

    valid = [e for e in entries if not e.skip_reason]
    skipped = [e for e in entries if e.skip_reason]
    t1 = time.time()
    print(f"\n[Phase 1 done in {t1 - t0:.1f}s]\n")

    # ── Phase 2: S3 Upload (optional) ──────────────────────────────────────────
    if mongo_only:
        print("[Phase 2 skipped — mongo-only: building report URLs from manifest]\n")
        url_map = build_report_url_map(valid)
        print(f"Built {len(url_map)} report URLs for bucket s3://{S3_BUCKET}/\n")
        t2 = time.time()
    else:
        url_map = upload_files(valid, dry_run=dry_run)
        t2 = time.time()
        print(f"\n[Phase 2 done in {t2 - t1:.1f}s]\n")

    # ── Phase 3: MongoDB Write ───────────────────────────────────────────────
    mongo_stats = write_patients_and_scans(entries, url_map, dry_run=dry_run)
    t3 = time.time()
    print(f"\n[Phase 3 done in {t3 - t2:.1f}s]\n")

    # ── Summary ──────────────────────────────────────────────────────────────
    elapsed = t3 - t0

    unique_patients = len({e.normalized_name for e in valid})
    unique_hashes = len({e.content_sha256 for e in valid})

    # Check for S3 key collisions (should be zero with path-based keys)
    all_keys = [e.s3_key for e in valid]
    key_collisions = len(all_keys) - len(set(all_keys))

    report = {
        "mode": label,
        "mongo_only": mongo_only,
        "elapsed_seconds": round(elapsed, 1),
        "total_files_found": len(entries),
        "valid_files": len(valid),
        "skipped_files": len(skipped),
        "skip_reasons": {},
        "unique_patients_from_manifest": unique_patients,
        "unique_content_hashes": unique_hashes,
        "s3_key_collisions": key_collisions,
        "s3_files_uploaded": len(url_map),
        **mongo_stats,
    }

    for e in skipped:
        reason = e.skip_reason or "unknown"
        report["skip_reasons"][reason] = report["skip_reasons"].get(reason, 0) + 1

    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print(f"{'=' * 60}")
    print(f"  SUMMARY ({label})")
    print(f"{'=' * 60}")
    for k, v in report.items():
        if isinstance(v, dict):
            print(f"  {k}:")
            for sk, sv in v.items():
                print(f"    {sk}: {sv}")
        else:
            print(f"  {k}: {v}")
    print(f"\nFull report saved to: {REPORT_PATH}")

    if key_collisions > 0:
        print(f"\n  WARNING: {key_collisions} S3 key collisions detected!")
    else:
        print(f"\n  S3 key collisions: 0 (good)")

    if dry_run:
        print(f"\nDry-run complete. Re-run with --apply to execute.")


if __name__ == "__main__":
    main()
