# Databricks notebook source
from pyspark.sql.functions import col, coalesce, lit
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

bronze_df = spark.table("bronze_subway_usage")

# 1) 최신 REG_YMD만 유지 (정확한 중복 제거)
w = Window.partitionBy("USE_YMD", "SBWY_STNS_NM", "SBWY_ROUT_LN_NM").orderBy(col("REG_YMD").desc())
dedup_df = (
    bronze_df
    .withColumn("rn", row_number().over(w))
    .filter(col("rn") == 1)
    .drop("rn")
)

# 2) 타입/컬럼 표준화 + 파생지표
silver_df = (
    dedup_df
    .selectExpr(
        "USE_YMD as use_ymd",
        "SBWY_ROUT_LN_NM as line_name",
        "SBWY_STNS_NM as station_name",
        "REG_YMD as reg_ymd",
        "cast(GTON_TNOPE as int) as board_cnt",
        "cast(GTOFF_TNOPE as int) as alight_cnt"
    )
    .withColumn("board_cnt", coalesce(col("board_cnt"), lit(0)))
    .withColumn("alight_cnt", coalesce(col("alight_cnt"), lit(0)))
    .withColumn("total_cnt", col("board_cnt") + col("alight_cnt"))
    .filter((col("board_cnt") >= 0) & (col("alight_cnt") >= 0))
)

silver_df.write.format("delta").mode("overwrite").saveAsTable("silver_subway_usage")
