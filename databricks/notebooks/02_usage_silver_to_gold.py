# Databricks notebook source
from pathlib import Path
import sys
dbutils.widgets.text("project_root", "")
dbutils.widgets.text("namespace", "workspace.ssdr_v2_fixture")
dbutils.widgets.text("use_ymd", "20260128")
project_root = Path(dbutils.widgets.get("project_root"))
if not project_root.is_absolute() or not (project_root / "src/platforms/managed_usage.py").is_file():
    raise ValueError("project_root must be the absolute uploaded project/workspace directory")
sys.path.insert(0, str(project_root))
spark.conf.set("spark.sql.session.timeZone", "Asia/Seoul")
from src.platforms.managed_usage import UsageTables
tables = UsageTables(dbutils.widgets.get("namespace"))
use_ymd = dbutils.widgets.get("use_ymd")

# COMMAND ----------
from src.platforms.managed_usage import publish_gold
import json
evidence = publish_gold(spark, tables, use_ymd)
print(json.dumps(evidence, sort_keys=True))
display(spark.table(tables.name("station")).where(f"use_ymd='{use_ymd}'"))
