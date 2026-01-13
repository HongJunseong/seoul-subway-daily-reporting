from __future__ import annotations

import os
from pyspark.sql import SparkSession


def build_spark(app_name: str) -> SparkSession:
    region = os.getenv("AWS_DEFAULT_REGION") or os.getenv("AWS_REGION") or os.getenv("SSDR_AWS_REGION") or "ap-northeast-2"

    return (
        SparkSession.builder
        .appName(app_name)

        # S3A 설정
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.EnvironmentVariableCredentialsProvider,"
            "com.amazonaws.auth.InstanceProfileCredentialsProvider,"
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
        )
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{region}.amazonaws.com")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")

        .getOrCreate()
    )


def get_bucket() -> str:
    b = os.getenv("SSDR_S3_BUCKET", "seoul-subway-daily-reporting").strip()
    if not b:
        raise RuntimeError("SSDR_S3_BUCKET is empty")
    return b


def s3a(bucket: str, key: str) -> str:
    return f"s3a://{bucket}/{key.lstrip('/')}"
