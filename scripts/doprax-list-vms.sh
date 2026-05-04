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

need_cmd curl
need_cmd jq

echo "[+] Fetching Doprax VMs..."
LIST_RESPONSE=$(api_get "$BASE_URL/api/v1/vms/")

printf "%-38s %-14s %-32s %-16s %-22s %-15s\n" "VM_CODE" "STATUS" "HOSTNAME" "SSH_USER" "SSH_PASSWORD" "IPV4"
printf "%-38s %-14s %-32s %-16s %-22s %-15s\n" "--------------------------------------" "--------------" "--------------------------------" "----------------" "----------------------" "---------------"

echo "$LIST_RESPONSE" | jq -r '
  .data[]? |
  [
    (.vmCode // ""),
    (.status // ""),
    (.name // ""),
    (.username // ""),
    (.ipv4 // "")
  ] | @tsv
' | while IFS=$'\t' read -r vm_code status hostname username ipv4; do
  ssh_user="${username:-"-"}"
  ssh_password="-"

  if [[ -n "$vm_code" && "$vm_code" != "null" ]]; then
    PASSWORD_RESPONSE=$(api_get "$BASE_URL/api/v1/vms/$vm_code/password/" 2>/dev/null || true)
    if [[ -n "$PASSWORD_RESPONSE" ]]; then
      password_user=$(echo "$PASSWORD_RESPONSE" | jq -r '.data.vmUsername // empty' 2>/dev/null || true)
      if [[ -n "$password_user" ]]; then
        ssh_user="$password_user"
      fi
      ssh_password=$(echo "$PASSWORD_RESPONSE" | jq -r '.data.tempPass // "-"' 2>/dev/null || echo "-")
    fi
  fi

  printf "%-38s %-14s %-32s %-16s %-22s %-15s\n" "$vm_code" "$status" "$hostname" "$ssh_user" "$ssh_password" "${ipv4:-"-"}"
done

echo
echo "[+] Done"
