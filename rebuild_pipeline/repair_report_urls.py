#!/usr/bin/env python3
"""
Fix report_url on scans_v2 when ingestion ran with an empty S3 bucket in env.
Rebuilds URLs from source_relative_path + current S3_BUCKET / AWS_REGION in .env.

Usage (from rebuild_pipeline/):
  python repair_report_urls.py
"""

import sys

from pymongo import UpdateOne

from config import AWS_REGION, MONGO_DB_NAME, S3_BUCKET, SCANS_COLLECTION
from mongo_client import connect_mongo
from upload_s3 import _build_report_url


def main():
    if not S3_BUCKET:
        print(
            "ERROR: S3 bucket is empty. Set S3_BUCKET_NAME or S3_BUCKET_NAME_V2 in .env",
            file=sys.stderr,
        )
        sys.exit(1)

    print(f"Bucket: {S3_BUCKET!r}, region: {AWS_REGION!r}")

    client = connect_mongo()
    db = client[MONGO_DB_NAME]
    col = db[SCANS_COLLECTION]

    total = col.count_documents({})
    print(f"Total scans in {SCANS_COLLECTION}: {total}")

    ops = []
    fixed = 0
    for doc in col.find({}, {"_id": 1, "source_relative_path": 1, "report_url": 1}):
        rel = doc.get("source_relative_path") or ""
        if not rel:
            continue
        s3_key = "reports/" + rel.replace("\\", "/")
        new_url = _build_report_url(S3_BUCKET, AWS_REGION, s3_key)
        if doc.get("report_url") != new_url:
            ops.append(UpdateOne({"_id": doc["_id"]}, {"$set": {"report_url": new_url}}))
            fixed += 1
        if len(ops) >= 500:
            col.bulk_write(ops, ordered=False)
            ops = []
            print(f"  updated {fixed} ...", end="\r")

    if ops:
        col.bulk_write(ops, ordered=False)

    print(f"\nDone. Documents needing URL change: {fixed}")
    client.close()


if __name__ == "__main__":
    main()
