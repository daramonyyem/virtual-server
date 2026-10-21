#!/usr/bin/env bash
# Start a Power Hub shared-master charger via this virtual-server (forwards to EV_HUB_PROXY_URL).
set -euo pipefail

VS_URL="${VS_URL:-http://localhost:3037}"
CHARGE_POINT_ID="${1:-CP68}"
CONNECTOR_ID="${2:-1}"
ID_TAG="${3:-TEST-A}"

echo "POST ${VS_URL}/api/proxy/start  chargePointId=${CHARGE_POINT_ID}"
curl -sS -X POST "${VS_URL}/api/proxy/start" \
  -H "Content-Type: application/json" \
  -d "{\"chargePointId\":\"${CHARGE_POINT_ID}\",\"connectorId\":${CONNECTOR_ID},\"idTag\":\"${ID_TAG}\"}" \
  | python3 -m json.tool 2>/dev/null || cat
echo
