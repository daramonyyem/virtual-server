#!/usr/bin/env bash
set -euo pipefail

VS_URL="${VS_URL:-http://localhost:3037}"
CHARGE_POINT_ID="${1:-CP68}"
TRANSACTION_ID="${2:?transactionId required}"
CONNECTOR_ID="${3:-1}"

echo "POST ${VS_URL}/api/proxy/stop  chargePointId=${CHARGE_POINT_ID} transactionId=${TRANSACTION_ID}"
curl -sS -X POST "${VS_URL}/api/proxy/stop" \
  -H "Content-Type: application/json" \
  -d "{\"chargePointId\":\"${CHARGE_POINT_ID}\",\"connectorId\":${CONNECTOR_ID},\"transactionId\":${TRANSACTION_ID}}" \
  | python3 -m json.tool 2>/dev/null || cat
echo
