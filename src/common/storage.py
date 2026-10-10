"""Versioned paths; local mode never requires cloud credentials."""
from __future__ import annotations
import os
from pathlib import Path
from src.common.config import load_s3_config


def storage_prefix() -> str:
    prefix = os.getenv("SSDR_STORAGE_PREFIX", "v2").strip("/")
    if any(part in (".", "..") for part in prefix.split("/")):
        raise ValueError("SSDR_STORAGE_PREFIX must not contain relative path segments")
    return prefix


def table_path(key: str) -> str:
    relative = "/".join(p for p in (storage_prefix(), key.strip("/")) if p)
    if ".." in relative.split("/"):
        raise ValueError("invalid table key")
    local_root = os.getenv("SSDR_DATA_ROOT")
    if local_root:
        return str(Path(local_root).resolve() / relative)
    return f"s3a://{load_s3_config().bucket}/{relative}"


def public_uri(path: str) -> str:
    return path.replace("s3a://", "s3://", 1)
