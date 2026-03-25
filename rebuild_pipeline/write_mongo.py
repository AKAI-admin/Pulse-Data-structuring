"""
Phase 3: Write patients and scans to new MongoDB collections in batches.
"""

from datetime import datetime, timezone

from config import (
    MONGO_BATCH_SIZE,
    MONGO_DB_NAME,
    PATIENTS_COLLECTION,
    SCANS_COLLECTION,
)
from mongo_client import connect_mongo


def write_patients_and_scans(
    entries,
    url_map: dict[str, str],
    *,
    dry_run: bool = False,
) -> dict:
    """
    Build patient index from manifest entries, then write patients and scans.
    Returns summary stats dict.
    """
    valid = [e for e in entries if not e.skip_reason]

    # Build in-memory patient index from manifest
    patient_names: dict[str, str] = {}  # normalized_name -> display_name
    for e in valid:
        key = e.normalized_name
        if key and key not in patient_names:
            patient_names[key] = e.patient_name

    print(f"Unique patients from manifest: {len(patient_names)}")
    print(f"Valid scan entries: {len(valid)}")

    if dry_run:
        seen_hashes: dict[str, int] = {}
        dup_content_count = 0
        for e in valid:
            combo = f"{e.normalized_name}|{e.content_sha256}"
            seen_hashes[combo] = seen_hashes.get(combo, 0) + 1
        for v in seen_hashes.values():
            if v > 1:
                dup_content_count += v - 1

        print(f"[DRY-RUN] Would create {len(patient_names)} patients in '{PATIENTS_COLLECTION}'")
        print(f"[DRY-RUN] Would create {len(valid)} scans in '{SCANS_COLLECTION}'")
        print(f"[DRY-RUN] Duplicate-content scans (same patient + same hash): {dup_content_count}")
        return {
            "patients_created": len(patient_names),
            "scans_created": len(valid),
            "duplicate_content_scans": dup_content_count,
            "dry_run": True,
        }

    client = connect_mongo()
    db = client[MONGO_DB_NAME]
    patients_col = db[PATIENTS_COLLECTION]
    scans_col = db[SCANS_COLLECTION]

    print("Connected to MongoDB")

    # Drop old v2 collections if they exist (fresh rebuild)
    patients_col.drop()
    scans_col.drop()
    print(f"Dropped old '{PATIENTS_COLLECTION}' and '{SCANS_COLLECTION}' collections (if any)")

    # Insert patients in batch
    patient_docs = []
    for norm_name, display_name in patient_names.items():
        patient_docs.append({
            "name": display_name,
            "normalized_name": norm_name,
        })

    patient_id_map: dict[str, object] = {}  # normalized_name -> _id

    for batch_start in range(0, len(patient_docs), MONGO_BATCH_SIZE):
        batch = patient_docs[batch_start: batch_start + MONGO_BATCH_SIZE]
        result = patients_col.insert_many(batch)
        for doc, inserted_id in zip(batch, result.inserted_ids):
            patient_id_map[doc["normalized_name"]] = inserted_id
        print(f"  patients: {min(batch_start + MONGO_BATCH_SIZE, len(patient_docs))}/{len(patient_docs)}", end="\r")

    print(f"\nInserted {len(patient_id_map)} patients")

    # Create indexes
    patients_col.create_index("normalized_name", unique=True)
    scans_col.create_index("patient")
    scans_col.create_index("content_sha256")

    # Track duplicates by (patient + content hash)
    seen_content: set[str] = set()

    # Insert scans in batch
    scan_docs = []
    duplicate_content_count = 0

    for e in valid:
        patient_id = patient_id_map.get(e.normalized_name)
        if not patient_id:
            continue

        report_url = url_map.get(e.s3_key, "")
        study_date = None
        if e.study_date:
            parts = e.study_date.split("-")
            study_date = datetime(int(parts[0]), int(parts[1]), int(parts[2]), tzinfo=timezone.utc)

        content_key = f"{e.normalized_name}|{e.content_sha256}"
        is_dup = content_key in seen_content
        if is_dup:
            duplicate_content_count += 1
        seen_content.add(content_key)

        scan_docs.append({
            "patient": patient_id,
            "studyDate": study_date,
            "report_url": report_url,
            "source_relative_path": e.relative_path,
            "source_filename": e.filename,
            "content_sha256": e.content_sha256,
            "is_duplicate_content": is_dup,
        })

    for batch_start in range(0, len(scan_docs), MONGO_BATCH_SIZE):
        batch = scan_docs[batch_start: batch_start + MONGO_BATCH_SIZE]
        scans_col.insert_many(batch)
        print(f"  scans: {min(batch_start + MONGO_BATCH_SIZE, len(scan_docs))}/{len(scan_docs)}", end="\r")

    print(f"\nInserted {len(scan_docs)} scans (duplicate-content flagged: {duplicate_content_count})")

    client.close()

    return {
        "patients_created": len(patient_id_map),
        "scans_created": len(scan_docs),
        "duplicate_content_scans": duplicate_content_count,
        "dry_run": False,
    }
