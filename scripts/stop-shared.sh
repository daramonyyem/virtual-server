#!/usr/bin/env bash
# POST /stop → remoteStop on EV_HUB_BRIDGE_URL/{chargePointId}
set -euo pipefail

VS_URL="${VS_URL:-http://localhost:3037}"
CHARGE_POINT_ID="${1:?chargePointId required}"
TRANSACTION_ID="${2:?transactionId required}"
CONNECTOR_ID="${3:-1}"

body="{\"chargePointId\":\"${CHARGE_POINT_ID}\",\"transactionId\":${TRANSACTION_ID},\"connectorId\":${CONNECTOR_ID}}"

echo "POST ${VS_URL}/stop  ${body}"
curl -sS -X POST "${VS_URL}/stop" \
  -H "Content-Type: application/json" \
  -d "${body}" \
  | python3 -m json.tool 2>/dev/null || cat
echo
