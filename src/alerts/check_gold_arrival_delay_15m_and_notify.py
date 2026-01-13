from __future__ import annotations

import os
import sys
import argparse
from pathlib import Path
from datetime import datetime, timedelta

import requests
from pyspark.sql import functions as F

# 공통 경로 설정
CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.jobs.spark_common import build_spark, get_bucket, s3a

GOLD_PREFIX = "gold/arrival_metrics_15m_delta/"

WINDOW_MINUTES = 15

# 알림 임계치
MIN_TOTAL_N = 200                 # 표본 너무 적을 땐 알림 스킵(노이즈 방지)
ALERT_DELAY_RATIO = 0.25          # 전체 10분+ 비율이 25% 넘으면 알림
ALERT_P90_SEC = 600               # p90이 10분 넘는 역이 많이 나오면 알림
TOPK = 10

def floor_to_5min(ts: datetime) -> datetime:
    minute = (ts.minute // 5) * 5
    return ts.replace(minute=minute, second=0, microsecond=0)

def discord_notify(webhook: str, content: str) -> None:
    if not webhook:
        print("[WARN] DISCORD_WEBHOOK_URL not set -> skip notify")
        return
    r = requests.post(webhook, json={"content": content}, timeout=10)
    if r.status_code >= 300:
        print("[WARN] Discord notify failed:", r.status_code, r.text)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--as_of", type=str, required=True, help="KST 'YYYY-MM-DD HH:MM:SS'")
    args = parser.parse_args()

    webhook = os.getenv("DISCORD_WEBHOOK_URL", "")
    bucket = get_bucket()

    now = datetime.strptime(args.as_of, "%Y-%m-%d %H:%M:%S")
    window_end = floor_to_5min(now)
    window_start = window_end - timedelta(minutes=WINDOW_MINUTES)

    yi, mi, di, hi = window_end.year, window_end.month, window_end.day, window_end.hour
    window_end_ymdhm = window_end.strftime("%Y%m%d%H%M")

    gold_path = s3a(bucket, GOLD_PREFIX)

    spark = build_spark(f"ssdr_qa_arrival_15m_{window_end_ymdhm}")
    spark.sparkContext.setLogLevel("WARN")

    # 최신 윈도우 파티션만 읽기
    df = (
        spark.read.format("delta").load(gold_path)
        .where(
            (F.col("year") == F.lit(int(yi))) &
            (F.col("month") == F.lit(int(mi))) &
            (F.col("day") == F.lit(int(di))) &
            (F.col("hour") == F.lit(int(hi))) &
            (F.col("window_end_ymdhm") == F.lit(window_end_ymdhm))
        )
    )

    if df.rdd.isEmpty():
        msg = (
            f"⚠️ [Subway Arrival QA] GOLD EMPTY\n"
            f"- window: {window_start} ~ {window_end} (KST)\n"
            f"- partition: {yi}-{mi:02d}-{di:02d} {hi:02d} / {window_end_ymdhm}\n"
        )
        discord_notify(webhook, msg)
        spark.stop()
        return

    # 전체 지연율(가중 평균): sum(n * long_ratio_10m) / sum(n)
    agg = df.agg(
        F.sum("n").alias("total_n"),
        (F.sum(F.col("n") * F.col("long_ratio_10m")) / F.sum("n")).alias("delay_ratio_10m_weighted"),
        # p90도 가중 평균으로 하나 만들면 전반 상태 판단에 도움
        (F.sum(F.col("n") * F.col("p90_eta_sec")) / F.sum("n")).alias("p90_eta_sec_weighted"),
        F.count("*").alias("groups"),
    ).collect()[0]

    total_n = int(agg["total_n"] or 0)
    delay_ratio = float(agg["delay_ratio_10m_weighted"] or 0.0)
    p90_weighted = float(agg["p90_eta_sec_weighted"] or 0.0)
    groups = int(agg["groups"] or 0)

    # 심각 지연 역 TOPK: long_ratio_10m 높고 표본 충분한 곳
    top = (
        df.where(F.col("n") >= F.lit(20))
          .select("station_name", "line_id", "updn_line", "n", "long_ratio_10m", "p90_eta_sec")
          .orderBy(F.col("long_ratio_10m").desc(), F.col("n").desc())
          .limit(TOPK)
          .collect()
    )


    # 알림 조건
    should_alert = (total_n >= MIN_TOTAL_N) and (
        delay_ratio >= ALERT_DELAY_RATIO or p90_weighted >= ALERT_P90_SEC
    )

    if should_alert:
        lines = []
        for r in top:
            lines.append(
                f"- {r['station_name']} (line={r['line_id']}, {r['updn_line']}) "
                f"n={r['n']} delay10m={float(r['long_ratio_10m']):.2f} p90={int(r['p90_eta_sec'])}s"
            )

        msg = (
            f"🚨 [Subway Delay Alert]\n"
            f"- window: {window_start} ~ {window_end} (KST)\n"
            f"- total_n={total_n}, groups={groups}\n"
            f"- delay10m(weighted)={delay_ratio:.2f}, p90(weighted)={p90_weighted:.0f}s\n"
            f"- TOP delayed stations:\n" + "\n".join(lines)
        )
        discord_notify(webhook, msg)
    else:
        print(f"[OK] QA pass: total_n={total_n}, delay10m={delay_ratio:.3f}, p90w={p90_weighted:.1f}s")

    spark.stop()

if __name__ == "__main__":
    main()
