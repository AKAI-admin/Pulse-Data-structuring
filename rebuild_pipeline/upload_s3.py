"""
Phase 2: Upload report files to S3 with concurrent workers.
Uses the manifest to determine which files to upload and their target keys.
"""

import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import boto3

from config import (
    AWS_ACCESS_KEY_ID,
    AWS_REGION,
    AWS_SECRET_ACCESS_KEY,
    S3_BUCKET,
    S3_UPLOAD_WORKERS,
)

DOCX_CONTENT_TYPE = (
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
)


def _make_s3_client():
    return boto3.client(
        "s3",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )


def _build_report_url(bucket: str, region: str, key: str) -> str:
    from urllib.parse import quote

    encoded_key = "/".join(quote(part, safe="") for part in key.split("/"))
    return f"https://{bucket}.s3.{region}.amazonaws.com/{encoded_key}"


def _upload_one(s3_client, bucket: str, abs_path: str, s3_key: str) -> str:
    with open(abs_path, "rb") as f:
        s3_client.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=f,
            ContentType=DOCX_CONTENT_TYPE,
        )
    return _build_report_url(bucket, AWS_REGION, s3_key)


def upload_files(entries, *, dry_run: bool = False) -> dict[str, str]:
    """
    Upload manifest entries to S3 concurrently.
    Returns a dict mapping s3_key -> report_url for each uploaded file.
    """
    to_upload = [e for e in entries if not e.skip_reason]

    if dry_run:
        print(f"[DRY-RUN] Would upload {len(to_upload)} files to s3://{S3_BUCKET}/")
        result = {}
        for e in to_upload:
            result[e.s3_key] = _build_report_url(S3_BUCKET, AWS_REGION, e.s3_key)
        return result

    print(f"Uploading {len(to_upload)} files to s3://{S3_BUCKET}/ ({S3_UPLOAD_WORKERS} workers) ...")

    uploaded: dict[str, str] = {}
    errors = 0

    def _worker(entry):
        client = _make_s3_client()
        url = _upload_one(client, S3_BUCKET, entry.abs_path, entry.s3_key)
        return entry.s3_key, url

    with ThreadPoolExecutor(max_workers=S3_UPLOAD_WORKERS) as pool:
        futures = {pool.submit(_worker, e): e for e in to_upload}
        for i, future in enumerate(as_completed(futures), 1):
            try:
                key, url = future.result()
                uploaded[key] = url
            except Exception as exc:
                entry = futures[future]
                errors += 1
                print(f"  [S3 ERROR] {entry.s3_key}: {exc}", file=sys.stderr)

            if i % 500 == 0 or i == len(to_upload):
                print(f"  upload progress: {i}/{len(to_upload)} (errors: {errors})", end="\r")

    print()
    print(f"Uploaded: {len(uploaded)}, Errors: {errors}")
    return uploaded
