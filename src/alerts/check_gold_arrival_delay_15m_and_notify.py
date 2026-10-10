"""Compatibility CLI name; alerts describe sampled ETA, not measured service delay."""
from __future__ import annotations
import argparse
from pyspark.sql import functions as F
from src.common.dates import parse_as_of, window_bounds
from src.common.storage import table_path
from src.jobs.spark_common import build_spark
from src.quality.checks import require_nonempty
from src.alerts.notify_pipeline import discord_notify


def summarize_alert(rows, min_total_n=200, min_group_n=20):
    valid_total = sum(int(r["valid_eta_n"]) for r in rows)
    if valid_total == 0:
        return {"should_alert": False, "valid_eta_n": 0, "long_ratio_10m": None, "severe_group_ratio": None}
    ratio = sum(r["valid_eta_n"] * (r["long_ratio_10m"] or 0) for r in rows) / valid_total
    eligible = [r for r in rows if r["valid_eta_n"] >= min_group_n]
    severe_fraction = (sum((r["p90_eta_sec"] or 0) >= 600 for r in eligible) / len(eligible)
                       if eligible else 0.0)
    return {"should_alert": valid_total >= min_total_n and (ratio >= 0.25 or severe_fraction >= 0.25),
            "valid_eta_n": valid_total, "long_ratio_10m": ratio, "severe_group_ratio": severe_fraction}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--as_of", required=True, type=parse_as_of)
    args = parser.parse_args()
    start, end = window_bounds(args.as_of)
    spark = build_spark("arrival-eta-quality")
    try:
        df = (spark.read.format("delta").load(table_path("gold/arrival_metrics_15m_delta"))
              .where(F.col("window_end_ymdhm") == end.strftime("%Y%m%d%H%M")))
        require_nonempty(df, "arrival Gold window")
        rows = [r.asDict() for r in df.collect()]
        result = summarize_alert(rows)
        print(f"[QA] window={start}~{end} {result}")
        if result["should_alert"]:
            discord_notify(f"Subway sampled ETA alert: {start}~{end} KST. "
                           f"valid={result['valid_eta_n']}, ETA>=10m={result['long_ratio_10m']:.1%}. "
                           "This is not a timetable-delay measurement.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
