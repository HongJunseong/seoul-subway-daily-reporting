"""Shared local Spark configuration for isolated fixture experiments."""
import os
import sys
from pathlib import Path


def local_spark(name):
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession
    root = Path(__file__).resolve().parents[1]
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
    os.environ["PYSPARK_PYTHON"] = sys.executable
    builder = (SparkSession.builder.master("local[2]").appName(name)
               .config("spark.sql.session.timeZone", "Asia/Seoul")
               .config("spark.sql.shuffle.partitions", "2")
               .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
               .config("spark.databricks.delta.snapshotPartitions", "2")
               .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
               .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
               .config("spark.jars.ivy", str(root / ".work/ivy"))
               .config("spark.jars.ivySettings", str(root / "tests/ivysettings.xml")))
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark
