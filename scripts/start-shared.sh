#!/usr/bin/env bash
# POST /start → remoteStart on EV_HUB_BRIDGE_URL/{chargePointId}
set -euo pipefail

VS_URL="${VS_URL:-http://localhost:3037}"
CHARGE_POINT_ID="${1:?chargePointId required}"
CONNECTOR_ID="${2:-1}"
ID_TAG="${3:-OUTSIDE}"

echo "POST ${VS_URL}/start  chargePointId=${CHARGE_POINT_ID}"
curl -sS -X POST "${VS_URL}/start" \
  -H "Content-Type: application/json" \
  -d "{\"chargePointId\":\"${CHARGE_POINT_ID}\",\"connectorId\":${CONNECTOR_ID},\"idTag\":\"${ID_TAG}\"}" \
  | python3 -m json.tool 2>/dev/null || cat
echo
