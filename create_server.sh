#!/usr/bin/env bash
set -euo pipefail

API_TOKEN="${CAASIFY_TOKEN:?Set CAASIFY_TOKEN first}"

CREATE_URL="https://api-panel.caasify.com/webhook/panel/createVPS"
SHOW_URL_BASE="https://api.caasify.com/api/orders"

PRODUCT_ID="3776"
TEMPLATE="ubuntu-24.04"
NOTE="VPS-Germany-2C-4GBx"

echo "[+] Creating Germany Ubuntu VPS..."

CREATE_RESPONSE=$(curl -s --location "$CREATE_URL" \
  -H "accept: application/json" \
  -H "content-type: application/json" \
  -H "token: $API_TOKEN" \
  --data-raw "{
    \"product_id\": $PRODUCT_ID,
    \"note\": \"$NOTE\",
    \"Template\": \"$TEMPLATE\",
    \"IPv4\": 1,
    \"IPv6\": 1
  }")

echo "[DEBUG] Create response:"
echo "$CREATE_RESPONSE"

ORDER_ID=$(echo "$CREATE_RESPONSE" | jq -r '.data.id // .id // empty')

if [[ -z "$ORDER_ID" ]]; then
  echo "[-] Failed to create order"
  exit 1
fi

echo "[+] Order ID: $ORDER_ID"

IP=""
PASSWORD=""

echo "[+] Waiting for IP + password..."

for i in {1..90}; do
  SHOW_RESPONSE=$(curl -s --location "$SHOW_URL_BASE/$ORDER_ID/show" \
    -H "Authorization: Bearer $API_TOKEN" \
    -H "Accept: application/json")

  PASSWORD=$(echo "$SHOW_RESPONSE" | jq -r '.data.secret // empty')

  IP=$(echo "$SHOW_RESPONSE" | jq -r '
    .data.view.references[]?
    | select(.reference.type == "ipv4")
    | .value
  ' | head -n1)

  if [[ -n "$IP" && -n "$PASSWORD" ]]; then
    echo "[+] IP: $IP"
    echo "[+] Password received"
    break
  fi

  echo "[*] Waiting..."
  sleep 10
done

if [[ -z "$IP" || -z "$PASSWORD" ]]; then
  echo "[-] Failed to get IP/password"
  echo "$SHOW_RESPONSE"
  exit 1
fi

echo "[+] Waiting for SSH..."

for i in {1..90}; do
  if nc -z "$IP" 22 >/dev/null 2>&1; then
    echo "[+] SSH is reachable"
    break
  fi
  sleep 5
done

echo "[+] Creating ~/hello.txt..."

sshpass -p "$PASSWORD" ssh \
  -o StrictHostKeyChecking=no \
  -o UserKnownHostsFile=/dev/null \
  root@"$IP" \
  'echo "hello from caasify automation" > ~/hello.txt'

echo "[+] Done"
echo "[+] Server IP: $IP"
echo "[+] Created file: ~/hello.txt"