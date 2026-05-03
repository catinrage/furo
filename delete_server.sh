#!/usr/bin/env bash
set -euo pipefail

API_TOKEN="${CAASIFY_TOKEN:?Set CAASIFY_TOKEN first}"

DELETE_URL="https://api-panel.caasify.com/webhook/panel/orderButton"
SHOW_URL_BASE="https://api.caasify.com/api/orders"

read -rp "Enter order/server ID to delete: " ORDER_ID

if [[ -z "$ORDER_ID" ]]; then
  echo "[-] No order ID provided"
  exit 1
fi

echo "[!] You are about to delete order/server: $ORDER_ID"
read -rp "Type DELETE to confirm: " CONFIRM

if [[ "$CONFIRM" != "DELETE" ]]; then
  echo "[-] Cancelled"
  exit 1
fi

echo "[+] Sending delete request..."

DELETE_RESPONSE=$(curl -s --location "$DELETE_URL" \
  -H "accept: application/json" \
  -H "content-type: application/x-www-form-urlencoded" \
  -H "token: $API_TOKEN" \
  --data-raw "order_id=$ORDER_ID&button_id=10")

echo "[DEBUG] Delete response:"
echo "$DELETE_RESPONSE"

OK=$(echo "$DELETE_RESPONSE" | jq -r '.ok // false')

if [[ "$OK" != "true" ]]; then
  echo "[-] Delete request was not accepted"
  exit 1
fi

echo "[+] Delete request accepted"
echo "[+] Checking deletion status..."

for i in {1..40}; do
  SHOW_RESPONSE=$(curl -s --location "$SHOW_URL_BASE/$ORDER_ID/show" \
    -H "Authorization: Bearer $API_TOKEN" \
    -H "Accept: application/json" || true)

  STATUS=$(echo "$SHOW_RESPONSE" | jq -r '.data.status // .status // empty' 2>/dev/null || true)

  if [[ "$STATUS" == "passive" || "$STATUS" == "cancelled" || "$STATUS" == "canceled" || "$STATUS" == "deleted" || "$STATUS" == "terminated" ]]; then
    echo "[+] Deleted successfully"
    echo "[+] Final status: $STATUS"
    exit 0
  fi

  if echo "$SHOW_RESPONSE" | grep -qiE 'not found|404|no query results'; then
    echo "[+] Deleted successfully; order no longer exists"
    exit 0
  fi

  echo "[*] Current status: ${STATUS:-unknown}"
  sleep 5
done

echo "[!] Delete request was accepted, but final deletion was not confirmed yet."
echo "[!] Last status: ${STATUS:-unknown}"