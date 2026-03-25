#!/usr/bin/env python3
"""
Post-ingestion validation: compare new collections against the manifest
and flag known problem patterns.
"""

import json
import sys
from collections import Counter

from pymongo import MongoClient

from config import (
    MANIFEST_PATH,
    MONGO_DB_NAME,
    MONGO_STANDARD_URI,
    MONGO_URI,
    PATIENTS_COLLECTION,
    SCANS_COLLECTION,
)
from scan_source import load_manifest


def connect_mongo() -> MongoClient:
    uri = MONGO_URI.strip() if MONGO_URI else ""
    if uri:
        try:
            client = MongoClient(uri, serverSelectionTimeoutMS=10000)
            client.admin.command("ping")
            return client
        except Exception:
            pass
    client = MongoClient(MONGO_STANDARD_URI, serverSelectionTimeoutMS=10000)
    client.admin.command("ping")
    return client


def main():
    if not MANIFEST_PATH.exists():
        print("No manifest found. Run the ingestion pipeline first.")
        sys.exit(1)

    entries = load_manifest()
    valid = [e for e in entries if not e.skip_reason]
    manifest_patients = {e.normalized_name for e in valid}
    manifest_hashes = Counter(e.content_sha256 for e in valid)

    client = connect_mongo()
    db = client[MONGO_DB_NAME]
    patients_col = db[PATIENTS_COLLECTION]
    scans_col = db[SCANS_COLLECTION]

    db_patient_count = patients_col.count_documents({})
    db_scan_count = scans_col.count_documents({})

    # Check patient count match
    print(f"Manifest unique patients:   {len(manifest_patients)}")
    print(f"DB patients ({PATIENTS_COLLECTION}): {db_patient_count}")
    patient_match = len(manifest_patients) == db_patient_count
    print(f"  Match: {'YES' if patient_match else 'NO'}")

    # Check scan count match
    print(f"\nManifest valid files:       {len(valid)}")
    print(f"DB scans ({SCANS_COLLECTION}):    {db_scan_count}")
    scan_match = len(valid) == db_scan_count
    print(f"  Match: {'YES' if scan_match else 'NO'}")

    # Check for S3 key uniqueness in DB
    pipeline = [
        {"$group": {"_id": "$report_url", "count": {"$sum": 1}}},
        {"$match": {"count": {"$gt": 1}}},
        {"$count": "duplicate_url_groups"},
    ]
    dup_result = list(scans_col.aggregate(pipeline))
    dup_url_groups = dup_result[0]["duplicate_url_groups"] if dup_result else 0
    print(f"\nDuplicate report_url groups in DB: {dup_url_groups}")

    # Check known problem names
    test_names = ["asha shah", "varsha shah", "raksha shah", "usha shah"]
    print("\nSpot-check known problem names:")
    for name in test_names:
        patient = patients_col.find_one({"normalized_name": name})
        if patient:
            scan_count = scans_col.count_documents({"patient": patient["_id"]})
            sample_urls = [
                s["report_url"]
                for s in scans_col.find({"patient": patient["_id"]}, {"report_url": 1}).limit(5)
            ]
            print(f"  {name}: {scan_count} scans")
            for url in sample_urls:
                print(f"    {url}")
        else:
            print(f"  {name}: NOT FOUND")

    # Duplicate content stats
    dup_content = scans_col.count_documents({"is_duplicate_content": True})
    print(f"\nScans flagged as duplicate content: {dup_content}")

    # Patients with multiple distinct dates
    multi_date_pipeline = [
        {"$match": {"studyDate": {"$type": "date"}}},
        {"$group": {"_id": {"patient": "$patient", "day": {"$dateToString": {"format": "%Y-%m-%d", "date": "$studyDate"}}}}},
        {"$group": {"_id": "$_id.patient", "distinct_dates": {"$sum": 1}}},
        {"$group": {
            "_id": None,
            "with_2_plus": {"$sum": {"$cond": [{"$gte": ["$distinct_dates", 2]}, 1, 0]}},
            "with_3_plus": {"$sum": {"$cond": [{"$gte": ["$distinct_dates", 3]}, 1, 0]}},
        }},
    ]
    date_result = list(scans_col.aggregate(multi_date_pipeline))
    if date_result:
        r = date_result[0]
        print(f"\nPatients with 2+ distinct dates: {r.get('with_2_plus', 0)}")
        print(f"Patients with 3+ distinct dates: {r.get('with_3_plus', 0)}")

    all_ok = patient_match and scan_match and dup_url_groups == 0
    print(f"\n{'=' * 40}")
    print(f"Overall: {'ALL CHECKS PASSED' if all_ok else 'SOME CHECKS FAILED'}")
    print(f"{'=' * 40}")

    client.close()


if __name__ == "__main__":
    main()
