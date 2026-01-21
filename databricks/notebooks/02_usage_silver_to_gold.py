# Databricks notebook source
from pyspark.sql.functions import sum as _sum, col

silver_df = spark.table("silver_subway_usage")

gold_df = (
    silver_df
    .groupBy("use_ymd", "station_name")   # 네 silver에서 표준화한 컬럼 기준
    .agg(_sum("total_cnt").alias("daily_total"))
    .orderBy(col("daily_total").desc())
)

display(gold_df.limit(20))

gold_df.write.format("delta").mode("overwrite").saveAsTable(
    "gold_subway_usage_daily_station"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold_subway_usage_daily_station ORDER BY daily_total DESC LIMIT 20;

# COMMAND ----------

# 노선별 TOP

gold_line_df = (
    silver_df
    .groupBy("use_ymd", "line_name")
    .agg(_sum("total_cnt").alias("daily_total"))
    .orderBy(col("daily_total").desc())
)

display(gold_line_df.limit(20))

gold_line_df.write.format("delta").mode("overwrite").saveAsTable(
    "gold_subway_usage_daily_line"
)
