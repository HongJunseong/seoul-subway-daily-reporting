from __future__ import annotations

import os
import json
from pathlib import Path
from datetime import datetime

import boto3
from dotenv import load_dotenv


# 프로젝트 루트의 .env 로드 (test)
PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")

S3_BUCKET = os.getenv("SSDR_S3_BUCKET")
S3_PREFIX = os.getenv("SSDR_S3_PREFIX")

# AWS ENV LOAD
AWS_REGION = os.getenv("AWS_DEFAULT_REGION")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

if not S3_BUCKET:
    raise RuntimeError("SSDR_S3_BUCKET이 .env에 없습니다.")
if not S3_PREFIX:
    raise RuntimeError("SSDR_S3_PREFIX가 .env에 없습니다. 예: bronze/subway_usage/use_ymd=20260116/")
if not AWS_REGION:
    raise RuntimeError("SSDR_AWS_REGION 또는 AWS_DEFAULT_REGION이 .env에 없습니다.")
if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
    raise RuntimeError("AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY(.env) 설정이 필요합니다.")

OUT_DIR = Path(__file__).resolve().parent / "output"
OUT_DIR.mkdir(parents=True, exist_ok=True)

MANIFEST_PATH = Path(__file__).resolve().parent / "manifest.json"


def main():
    # .env의 키로 boto3 세션 생성
    session = boto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )
    s3 = session.client("s3")

    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_PREFIX)

    keys: list[str] = []
    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("/"):
                continue
            keys.append(key)

    if not keys:
        raise RuntimeError(f"No objects found: s3://{S3_BUCKET}/{S3_PREFIX}")

    downloaded = []
    for key in keys:
        local_path = OUT_DIR / key.replace("/", "__")
        s3.download_file(S3_BUCKET, key, str(local_path))
        downloaded.append(str(local_path))

    manifest = {
        "exported_at": datetime.now().isoformat(),
        "bucket": S3_BUCKET,
        "prefix": S3_PREFIX,
        "region": AWS_REGION,
        "num_files": len(downloaded),
        "files_local_preview": downloaded[:50],
    }
    MANIFEST_PATH.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print(f"[OK] Downloaded {len(downloaded)} files to: {OUT_DIR}")
    print(f"[OK] Wrote manifest: {MANIFEST_PATH}")


if __name__ == "__main__":
    main()
