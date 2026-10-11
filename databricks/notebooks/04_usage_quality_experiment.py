# Databricks notebook source
from pathlib import Path
import sys
import json
dbutils.widgets.text("project_root", "")
dbutils.widgets.text("namespace", "workspace.ssdr_v2_fixture_quality_demo")
project_root = Path(dbutils.widgets.get("project_root"))
if not project_root.is_absolute() or not (project_root / "src/platforms/usage_portfolio.py").is_file():
    raise ValueError("project_root must be the uploaded absolute source directory")
sys.path.insert(0, str(project_root))
spark.conf.set("spark.sql.session.timeZone", "Asia/Seoul")
spark.conf.set("spark.sql.ansi.enabled", "true")
from src.platforms.managed_usage import UsageTables
tables = UsageTables(dbutils.widgets.get("namespace"))

# COMMAND ----------
from src.platforms.usage_portfolio import quality_experiment
evidence = quality_experiment(spark, tables, project_root / "fixtures/usage.json",
                              "databricks-serverless")
dbutils.notebook.exit(json.dumps(evidence, ensure_ascii=False, sort_keys=True))
