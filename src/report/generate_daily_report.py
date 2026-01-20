# src/report/generate_daily_report.py
from __future__ import annotations

import sys
import json
from pathlib import Path
from datetime import datetime, timedelta
import os
from typing import Optional

from dotenv import load_dotenv

CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

load_dotenv(PROJECT_ROOT / ".env")

from src.report.build_report_context_daily import build_report_context
from src.common.config import load_s3_config  # ✅ bucket/region 읽기용

# OpenAI (openai>=1.0.0)
from openai import OpenAI

openai_key = os.getenv("OPENAI_API_KEY")
client = OpenAI(api_key=openai_key)

# S3 Upload
def upload_text_to_s3(
    text: str,
    bucket: str,
    key: str,
    content_type: str = "text/markdown; charset=utf-8",
) -> str:
    """
    text를 S3에 업로드하고 s3://... 경로를 반환
    - AWS 자격증명은 환경변수(AWS_ACCESS_KEY_ID/SECRET_ACCESS_KEY 등) 또는 default chain을 사용
    """
    import boto3

    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=text.encode("utf-8"),
        ContentType=content_type,
    )
    return f"s3://{bucket}/{key}"

def build_prompt(target_ymd: str, ctx: dict) -> tuple[list[dict], str]:
    """
    LLM에 넘길 messages와, 나중을 위해 원본 JSON 문자열을 같이 반환.
    """
    ctx_json = json.dumps(ctx, ensure_ascii=False, indent=2)

    system_msg = {
        "role": "system",
        "content": (
            "너는 서울 지하철 데이터를 분석해서 일간 리포트를 작성하는 데이터 분석가이다. "
            "숫자를 단순 나열하는 것이 아니라, 패턴과 의미를 해석해서 글로 풀어내야 한다. "
            "톤은 침착하고 설명 잘하는 데이터 분석가 느낌으로, 과장 없이 사실 위주로 쓰되, "
            "중요한 포인트는 강조해준다. 답변은 반드시 한국어로 작성한다."
        ),
    }

    temporal_note = f"""
    데이터 기준 시점 주의 (매우 중요)
    - usage(승하차 이용량) 데이터는 실시간이 아니라, 수집/정제 지연이 있는 '확정 일간 집계'로서 4일전 기준이다.
    따라서 usage는 "오늘의 실시간 상황"이 아니라, 최근 확정된 이용 패턴/기준 혼잡 구조를 설명하는 용도로 해석해야 한다.
    - arrival(도착) 데이터는 오늘 기준의 실시간/준실시간 지표로,
    오늘의 체감 품질과 운행 상황을 설명하는 용도로 해석한다.
    - 이 시간 차이를 반드시 고려해서 문장을 작성하라.
    """

    user_msg = {
        "role": "user",
        "content": f"""
    아래는 {target_ymd} 기준 서울 지하철 통계 데이터이다. (JSON 형식)

    ```json
    {ctx_json}
    이 데이터를 기반으로 한국어로 "서울 지하철 일간 분석 보고서"를 작성해줘.

    전체 스타일 / 길이 요구
    - 단순 요약이 아니라, 분석 보고서 형태로 상세하게 작성해줘.
    - 전체 분량은 최소 800자 이상, 가능하면 1,500자 안팎으로 써줘.
    - 각 지표(승하차, 대기시간, 5분 이상 비율, active_trains 등)는 단순 숫자 나열이 아니라, "무슨 의미인지"까지 해석해서 설명해줘.
    - Bullet만 나열하지 말고, 문장형 해설과 함께 섞어서 써줘.

    형식 :
    다음과 같은 큰 구조를 사용해줘. (이 이모지/섹션 제목을 그대로 써도 좋고, 약간만 변형해도 괜찮아)

    맨 앞에, 아래와 비슷한 형식의 요약 블록을 넣어줘:

    {target_ymd[:4]}-{target_ymd[4:6]}-{target_ymd[6:8]} 서울 지하철 서비스 요약

    - 일반 혼잡도(평소 이용량 기준): …
    - 체감 지연(대기시간 + 이용량): …
    - 열차 운행 측면: …
    처럼 3~5줄 정도의 bullet 요약을 넣어줘.

    그 아래부터는 섹션 번호를 구분해서 써줘:

    - 승하차 기준 혼잡도 (Usage)
    - 실시간 도착 기준 체감 품질 (Arrival)
    - 종합 코멘트 (한 줄 총평)

    각 섹션에는 다음 내용을 포함해줘.

    1️. 승하차 기준 혼잡도 (Usage)
    usage.top_stations_by_total 을 활용해서 TOP 10 역을 서술식 + bullet로 정리해줘.
    예시 스타일:
    - 홍대입구 (2호선) – 약 13.6만 명 – 1위
    - 잠실(송파구청) (2호선) – 약 13.4만 명 – 2위
    이런 식으로 역명 / 노선 / 승하차 인원 / 순위를 함께 써줘.
    데이터에 근거해서 설명해줘

    usage.line_usage_stats 를 사용해서,

    1호선, 2호선, 9호선, 경부선, 분당선 등 평균 이용량이 높은 노선을 중심으로
    "서울/수도권 출퇴근 메인 축" 같은 식으로 해석을 붙여줘.
    데이터에 근거해서 설명해주면 좋겠어.

    마지막에 한두 줄 정도로
    "2호선 + 경부선 축이 핵심 축이다"처럼 축/벨트 관점의 한 줄 정리를 넣어줘.
    예시를 그대로 따라하지 말고, 오늘 데이터에 맞게 새로 작성해줘.

    2. 실시간 도착 기준 체감 품질 (Arrival)
    arrival.line_wait_stats, arrival.painful_stations, arrival.last_train_info, arrival.late_night_hotspots 를 활용해서:
    노선별 평균 대기시간(avg_wait_sec)과 5분 이상 대기 비율(long_wait_ratio_5)을
    1호선, 2호선, 3호선, 4호선, 5·6·7·9호선, 우이신설선(1092) 위주로 비교해줘.
    예: "2호선은 평균 8분 이상, 5분 이상 대기 비율 66%로 체감 지연이 큰 편" 식으로 서술해줘.
    데이터에 근거해서 설명해줘.

    arrival.painful_stations 를 이용해서
    데이터를 꼼꼼히 확인하면서 예를 들어 건대입구, 홍대입구, 고속터미널, 연신내, 강남, 신림, 을지로입구, 성수 등
    "사람도 많고, 열차도 늦게 오는 역"을 TOP 5~10개 정도 뽑아서
    또한, 각 역마다 평균 대기시간 + 5분 이상 비율 + 승하차 인원을 같이 설명해줘.
    예시를 따라하지 말고, 데이터에 근거해서 설명해줘.


    arrival.last_train_info, late_night_hotspots 를 활용해서
    막차가 늦게까지 운행된 구간(예: 인천, 파주, 정부과천청사 등)과
    심야 운행이 많은 도심/환승역(청량리, 제기동, 사당, 불광 등)을
    "야간 운행 패턴" 관점에서 설명해줘.

    3. 종합 코멘트 (한 줄 총평)
    마지막에 3~5줄 정도로, 오늘 데이터를 한 번 더 압축해서 정리해줘.
    예를 들면 이런 느낌으로:

    “최근 4일 기준으로는 2호선·경부선 축(홍대입구, 잠실, 고속터미널, 강남 등)이 여전히 최상위 혼잡도를 기록하고 있고,
    오늘 실시간 데이터 기준으로는 2·7·6·9호선 일부 구간에서 평균 대기시간과 5분 이상 대기 비율이 높게 나타나
    건대입구, 홍대입구, 고속터미널, 강남, 신림, 연신내 같은 역들이
    ‘많이 타면서 오래 기다리는 고통역’으로 드러난다.
    운행 측면에서는 9호선이 가장 많은 열차가 동시에 움직이고, 상·하행 불균형도 가장 커서
    특정 방향으로의 수요 쏠림과 배차 집중이 뚜렷하게 나타난다.”

    위 문장을 그대로 따라하지 말고, 오늘 데이터(입력 JSON)에 맞춰서 비슷한 밀도와 톤으로 데이터에 맞게 새로 작성해줘.
    또한 위에 모든 내용에 대한 설명들이 좀 더 데이터에 근거해서 설명까지 구체적으로 덧붙여지면 좋겠어.
    """,
    }

    messages = [system_msg, user_msg]
    return messages

def generate_daily_report(target_ymd: str | None = None, save_to_s3: bool = True) -> Path:
    if target_ymd is None:
        target_ymd = datetime.today().strftime("%Y%m%d")
    
    # 1) 컨텍스트 생성
    ctx = build_report_context(target_ymd)

    # 2) 프롬프트 생성
    messages = build_prompt(target_ymd, ctx)

    # 3) LLM 호출
    response = client.chat.completions.create(
        model="gpt-5-mini",  # model
        messages=messages,
        # temperature=0.3,
    )

    report_text = response.choices[0].message.content

    # 4) save result
    out_dir = PROJECT_ROOT / "data" / "report_text" / "daily"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"subway_report_{target_ymd}.md"

    out_path.write_text(report_text, encoding="utf-8")
    print(f"[INFO] Saved daily report: {out_path}")

    # 5) S3 save
    if save_to_s3:
        s3cfg = load_s3_config()
        bucket = getattr(s3cfg, "bucket", None) or os.getenv("SSDR_S3_BUCKET")
        if not bucket:
            raise RuntimeError("S3 bucket not configured. (load_s3_config().bucket or SSDR_S3_BUCKET)")

        # 요청한 경로: report_text/daily/
        key = f"report_text/daily/subway_report_{target_ymd}.md"
        s3_uri = upload_text_to_s3(report_text, bucket=bucket, key=key)
        print(f"[INFO] Uploaded daily report (s3): {s3_uri}")

    return out_path

def main():
    # 오늘 기준
    generate_daily_report()

if __name__ == "__main__":
    main()