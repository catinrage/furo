#!/usr/bin/env bash
set -euo pipefail

API_TOKEN="${CAASIFY_TOKEN:?Set CAASIFY_TOKEN first}"
BASE_URL="https://api.caasify.com/api"

echo "[+] Fetching servers..."

RESPONSE=$(curl -s --location "$BASE_URL/orders" \
  -H "Authorization: Bearer $API_TOKEN" \
  -H "Accept: application/json")

echo
printf "%-8s %-25s %-15s %-15s %-15s\n" "ID" "HOSTNAME" "IP" "LOCATION" "OS"
printf "%-8s %-25s %-15s %-15s %-15s\n" "--------" "-------------------------" "---------------" "---------------" "---------------"

echo "$RESPONSE" | jq -r '
.data[] |
[
  .id,
  (.note // "N/A"),
  (
    .view.references[]? 
    | select(.reference.type=="ipv4") 
    | .value
  ) // "N/A",
  (
    .records[0].product.detail.dc_city // "N/A"
  ),
  (
    .records[0].product.title 
    | capture("(?<os>Ubuntu [0-9]+)") 
    | .os
  ) // "Unknown"
] | @tsv
' | while IFS=$'\t' read -r id note ip location os; do
  printf "%-8s %-25s %-15s %-15s %-15s\n" "$id" "$note" "$ip" "$location" "$os"
done

echo
echo "[+] Done"