#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${1:-https://medici-monitor-dashboard.azurewebsites.net}"

check_endpoint() {
  local method="$1"
  local path="$2"
  local expected="$3"

  local code
  if [[ "$method" == "GET" ]]; then
    code=$(curl -sS -o /dev/null -w "%{http_code}" "$BASE_URL$path")
  else
    code=$(curl -sS -o /dev/null -w "%{http_code}" -X "$method" "$BASE_URL$path")
  fi

  if [[ "$code" == "$expected" ]]; then
    echo "✅ $method $path -> $code"
  else
    echo "❌ $method $path -> $code (expected $expected)"
  fi
}

echo "Running MediciMonitor smoke test on: $BASE_URL"
echo "----------------------------------------"

check_endpoint GET /healthz 200
check_endpoint GET /api/alerts 200
check_endpoint GET /api/alerts/thresholds 200
check_endpoint GET /api/notifications/config 200
check_endpoint GET /api/monitor/status 200
check_endpoint GET "/api/monitor/history?last=1" 200
check_endpoint GET "/api/monitor/trend?hours=24" 200

echo "----------------------------------------"
echo "Tip: If thresholds/config endpoints are 404, production is likely on an older build."
