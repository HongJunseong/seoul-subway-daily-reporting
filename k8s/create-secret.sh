#!/bin/bash
set -a
source /home/wnstjd4504/projects/seoul-subway-daily-reporting/.env
set +a

kubectl create secret generic ssdr-secrets \
  --namespace airflow \
  --from-literal=OPENAI_API_KEY="$OPENAI_API_KEY" \
  --from-literal=AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
  --from-literal=AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" \
  --from-literal=SEOUL_ARRIVAL_API_KEY="$SEOUL_ARRIVAL_API_KEY" \
  --from-literal=SEOUL_PASSENGER_API_KEY="$SEOUL_PASSENGER_API_KEY" \
  --from-literal=DISCORD_WEBHOOK_URL="$DISCORD_WEBHOOK_URL" \
  --from-literal=SSDR_S3_BUCKET="$SSDR_S3_BUCKET" \
  --from-literal=SSDR_AWS_REGION="$SSDR_AWS_REGION" \
  --from-literal=AWS_DEFAULT_REGION="$AWS_DEFAULT_REGION"
