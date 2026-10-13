#!/usr/bin/env bash
set -euo pipefail
# Project-relative dotenv loading; no secrets in argv or printed output.
project_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_dir"
python - <<'PY' | kubectl apply -f -
import json
import os
from dotenv import dotenv_values

values = {**dotenv_values(".env"), **os.environ}
names = ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN",
         "AWS_DEFAULT_REGION", "SSDR_AWS_REGION", "SSDR_S3_BUCKET",
         "SEOUL_ARRIVAL_API_KEY", "SEOUL_PASSENGER_API_KEY",
         "SLACK_WEBHOOK_URL", "OPENAI_API_KEY"]
required = ["SSDR_S3_BUCKET", "SEOUL_ARRIVAL_API_KEY", "SEOUL_PASSENGER_API_KEY"]
missing = [name for name in required if not values.get(name)]
if missing:
    raise SystemExit("Missing required settings: " + ", ".join(missing))
print(json.dumps({"apiVersion": "v1", "kind": "Secret",
                  "metadata": {"name": "ssdr-secrets", "namespace": "airflow"},
                  "type": "Opaque",
                  "stringData": {name: values[name] for name in names if values.get(name)}}))
PY
