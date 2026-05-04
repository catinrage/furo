#!/usr/bin/env bash
set -euo pipefail

API_KEY="${DOPRAX_API_KEY:-${DOPRAX_TOKEN:-}}"
BASE_URL="${DOPRAX_BASE_URL:-https://www.doprax.com}"

if [[ -z "$API_KEY" ]]; then
  echo "[-] Set DOPRAX_API_KEY first"
  exit 1
fi

need_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "[-] Missing required command: $1" >&2
    exit 1
  fi
}

api_get() {
  curl -fsS --location "$1" \
    -H "accept: application/json" \
    -H "content-type: application/json" \
    -H "X-API-Key: $API_KEY"
}

api_delete() {
  curl -fsS --location --request DELETE "$1" \
    -H "accept: application/json" \
    -H "content-type: application/json" \
    -H "X-API-Key: $API_KEY"
}

need_cmd curl
need_cmd jq

read -rp "Doprax VM code to delete: " VM_CODE
if [[ -z "$VM_CODE" ]]; then
  echo "[-] VM code is required"
  exit 1
fi

echo "[!] You are about to delete Doprax VM: $VM_CODE"
read -rp "Type DELETE to confirm: " CONFIRM
if [[ "$CONFIRM" != "DELETE" ]]; then
  echo "[-] Cancelled"
  exit 1
fi

echo "[+] Sending delete request..."
DELETE_RESPONSE=$(api_delete "$BASE_URL/api/v1/vms/$VM_CODE/")
echo "$DELETE_RESPONSE" | jq .

SUCCESS=$(echo "$DELETE_RESPONSE" | jq -r '.success // false')
if [[ "$SUCCESS" != "true" ]]; then
  echo "[-] Delete request was not accepted"
  exit 1
fi

echo "[+] Delete request accepted"
echo "[+] Polling VM status..."

for _ in {1..60}; do
  STATUS_RESPONSE=$(api_get "$BASE_URL/api/v1/vms/$VM_CODE/status/" 2>/dev/null || true)
  if [[ -z "$STATUS_RESPONSE" ]]; then
    echo "[+] VM no longer returned by status endpoint"
    exit 0
  fi

  STATUS=$(echo "$STATUS_RESPONSE" | jq -r '.data.status // empty' 2>/dev/null || true)
  ACTIVE=$(echo "$STATUS_RESPONSE" | jq -r '.data.isActive // empty' 2>/dev/null || true)
  case "$STATUS" in
    deleted|deleting|terminated|terminating)
      echo "[+] Delete status: $STATUS"
      exit 0
      ;;
  esac

  echo "[*] status=${STATUS:-unknown} active=${ACTIVE:-unknown}"
  sleep 5
done

echo "[!] Delete was accepted, but final deletion was not confirmed yet."
