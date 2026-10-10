import os
import sys
from pathlib import Path
import pytest


@pytest.fixture(scope="session")
def spark():
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    builder = (SparkSession.builder.master("local[2]").appName("ssdr-regression")
               .config("spark.driver.host", "127.0.0.1")
               .config("spark.driver.bindAddress", "127.0.0.1")
               .config("spark.sql.shuffle.partitions", "2")
               .config("spark.databricks.delta.snapshotPartitions", "2")
               .config("spark.sql.session.timeZone", "Asia/Seoul")
               .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
               .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
               .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
               .config("spark.jars.ivy", str(Path(".work/ivy").resolve()))
               .config("spark.jars.ivySettings", str(Path("tests/ivysettings.xml").resolve())))
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def storage(tmp_path, monkeypatch):
    monkeypatch.setenv("SSDR_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("SSDR_STORAGE_PREFIX", "v2")
    return tmp_path
