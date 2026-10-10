from __future__ import annotations
import os
from pyspark.sql import SparkSession
from src.common.config import load_s3_config


def build_spark(app_name: str) -> SparkSession:
    region = os.getenv("AWS_DEFAULT_REGION") or os.getenv("SSDR_AWS_REGION") or "ap-northeast-2"
    builder = (SparkSession.builder.appName(app_name)
               .config("spark.sql.session.timeZone", "Asia/Seoul")
               .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
               .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
               .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
               .config("spark.sql.shuffle.partitions", os.getenv("SSDR_SHUFFLE_PARTITIONS", "8")))
    if not os.getenv("SSDR_DATA_ROOT"):
        builder = (builder.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                   .config("spark.hadoop.fs.s3a.endpoint", f"s3.{region}.amazonaws.com")
                   .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                           "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"))
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def get_bucket() -> str:
    return load_s3_config().bucket


def s3a(bucket: str, key: str) -> str:
    return f"s3a://{bucket}/{key.lstrip('/')}"
