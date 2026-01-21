from pyspark.sql import SparkSession
from pathlib import Path

INPUT_DIR = "databricks/sample_export/output"
OUTPUT_DIR = "databricks/sample_export/merged"

spark = (
    SparkSession.builder
    .appName("merge-parquet")
    .getOrCreate()
)

df = spark.read.parquet(INPUT_DIR)

# 파일 병합
df = df.coalesce(1)

Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

df.write.mode("overwrite").parquet(OUTPUT_DIR)

print("Merged parquet written to:", OUTPUT_DIR)

spark.stop()
