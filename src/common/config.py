# src/common/config.py
from __future__ import annotations

import os
from dataclasses import dataclass

@dataclass(frozen=True)
class S3Config:
    bucket: str
    region: str | None = None

def load_s3_config() -> S3Config:
    bucket = os.getenv("SSDR_S3_BUCKET", "").strip()
    if not bucket:
        raise RuntimeError("SSDR_S3_BUCKET 환경변수가 비어있습니다. (.env에 설정)")
    region = os.getenv("SSDR_AWS_REGION", "ap-northeast-2").strip()
    return S3Config(bucket=bucket, region=region)