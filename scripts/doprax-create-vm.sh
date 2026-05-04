#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${DOPRAX_BASE_URL:-https://www.doprax.com}"

PRODUCT_VERSION_ID="${DOPRAX_PRODUCT_VERSION_ID:-4034ee51-9731-4663-95ee-a162dc47b119}"
LOCATION_OPTION_ID="${DOPRAX_LOCATION_OPTION_ID:-ec5bc1aa-db5f-48a8-8d88-ef654a2a6dc8}"
OS_OPTION_ID="${DOPRAX_OS_OPTION_ID:-ec9473a2-caa6-4197-95a4-7ee7e2b59dba}"
ACCESS_METHOD="${DOPRAX_ACCESS_METHOD:-password}"

EMAIL="${DOPRAX_EMAIL:-}"
PASSWORD="${DOPRAX_PASSWORD:-}"
BEARER_TOKEN="${DOPRAX_BEARER_TOKEN:-}"

need_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "[-] Missing required command: $1" >&2
    exit 1
  fi
}

new_uuid() {
  if command -v uuidgen >/dev/null 2>&1; then
    uuidgen | tr '[:upper:]' '[:lower:]'
    return
  fi
  if [[ -r /proc/sys/kernel/random/uuid ]]; then
    cat /proc/sys/kernel/random/uuid
    return
  fi
  echo "uuidgen-or-proc-random-uuid-required" >&2
  return 1
}

api_request_json() {
  local method="$1"
  local url="$2"
  local body="${3:-}"
  shift 3 || true

  local tmp
  tmp=$(mktemp)
  local status
  local args=(curl -sS --location "$url" -o "$tmp" -w '%{http_code}')
  args+=(-H "accept: application/json, text/plain, */*")
  args+=(-H "accept-language: en-US,en;q=0.9")
  args+=(-H "content-type: application/json")
  args+=(-H "origin: $BASE_URL")
  args+=(-H "referer: $BASE_URL/v/virtual-machines/new")
  args+=(-H "sec-fetch-mode: cors")
  args+=(-H "sec-fetch-site: same-origin")
  args+=(-H "user-agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36")
  while (($#)); do
    args+=(-H "$1")
    shift
  done
  case "$method" in
    GET)
      ;;
    POST)
      args+=(--request POST --data-raw "$body")
      ;;
    *)
      args+=(--request "$method")
      if [[ -n "$body" ]]; then
        args+=(--data-raw "$body")
      fi
      ;;
  esac

  status=$("${args[@]}")
  local response
  response=$(cat "$tmp")
  rm -f "$tmp"

  if [[ "$status" -lt 200 || "$status" -ge 300 ]]; then
    echo "[-] $method $url failed status=$status" >&2
    echo "$response" >&2
    return 1
  fi
  if ! echo "$response" | jq empty >/dev/null 2>&1; then
    echo "[-] $method $url returned non-JSON status=$status" >&2
    echo "$response" >&2
    return 1
  fi
  printf '%s\n' "$response"
}

api_v2_post_file() {
  local url="$1"
  local body="$2"
  local body_file
  local response_file
  body_file=$(mktemp)
  response_file=$(mktemp)
  printf '%s' "$body" > "$body_file"

  local status
  status=$(curl -sS "$url" \
    -o "$response_file" \
    -w '%{http_code}' \
    -H "accept: application/json, text/plain, */*" \
    -H "accept-language: en-US,en;q=0.9" \
    -H "authorization: Bearer $BEARER_TOKEN" \
    -H "content-type: application/json" \
    -H "cookie: cca=$BEARER_TOKEN" \
    -H "origin: $BASE_URL" \
    -H "referer: $BASE_URL/v/virtual-machines/new" \
    -H "sec-fetch-dest: empty" \
    -H "sec-fetch-mode: cors" \
    -H "sec-fetch-site: same-origin" \
    -H "user-agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36" \
    --data-binary "@$body_file")

  local response
  response=$(cat "$response_file")
  rm -f "$body_file" "$response_file"

  if [[ "$status" -lt 200 || "$status" -ge 300 ]]; then
    echo "[-] POST $url failed status=$status" >&2
    echo "$response" >&2
    return 1
  fi
  if ! echo "$response" | jq empty >/dev/null 2>&1; then
    echo "[-] POST $url returned non-JSON status=$status" >&2
    echo "$response" >&2
    return 1
  fi
  printf '%s\n' "$response"
}

login() {
  if [[ -n "$BEARER_TOKEN" ]]; then
    printf '%s\n' "$BEARER_TOKEN"
    return
  fi
  if [[ -z "$EMAIL" ]]; then
    read -rp "Doprax username/email: " EMAIL
  fi
  if [[ -z "$PASSWORD" ]]; then
    read -rsp "Doprax account password: " PASSWORD
    echo
  fi
  if [[ -z "$EMAIL" || -z "$PASSWORD" ]]; then
    echo "[-] Doprax email/password are required"
    exit 1
  fi

  local login_body
  login_body=$(jq -cn --arg user_email "$EMAIL" --arg user_pass "$PASSWORD" '{user_email: $user_email, user_pass: $user_pass}')
  local login_response
  login_response=$(api_request_json POST "$BASE_URL/api/v1/account/login-doprax-123321/" "$login_body")
  local token
  token=$(echo "$login_response" | jq -r '.cca // .data.cca // empty')
  if [[ -z "$token" ]]; then
    echo "[-] Login response did not include cca token"
    echo "$login_response" | jq .
    exit 1
  fi
  printf '%s\n' "$token"
}

api_v2_get() {
  local url="$1"
  api_request_json GET "$url" "" "authorization: Bearer $BEARER_TOKEN" "cookie: cca=$BEARER_TOKEN"
}

api_v2_post() {
  local url="$1"
  local body="$2"
  api_v2_post_file "$url" "$body"
}

need_cmd curl
need_cmd jq

read -rp "Hostname: " HOSTNAME
if [[ -z "$HOSTNAME" ]]; then
  echo "[-] Hostname is required"
  exit 1
fi

echo "[+] Logging in to Doprax..."
BEARER_TOKEN=$(login)
BEARER_TOKEN=$(printf '%s' "$BEARER_TOKEN" | tr -d '\r\n')

CREATE_BODY=$(jq -cn \
  --arg product_version_id "$PRODUCT_VERSION_ID" \
  --arg idempotency_key "$(new_uuid)" \
  --arg name "$HOSTNAME" \
  --arg access_method "$ACCESS_METHOD" \
  --arg location_option_id "$LOCATION_OPTION_ID" \
  --arg os_option_id "$OS_OPTION_ID" \
  '{
    product_version_id: $product_version_id,
    idempotency_key: $idempotency_key,
    name: $name,
    metadata: {access_method: $access_method},
    selections: {
      location: {optionId: $location_option_id},
      operating_system: {optionId: $os_option_id}
    }
  }')

echo "[+] Creating Doprax VM hostname=$HOSTNAME product_version_id=$PRODUCT_VERSION_ID location_option_id=$LOCATION_OPTION_ID os_option_id=$OS_OPTION_ID access_method=$ACCESS_METHOD"
CREATE_RESPONSE=$(api_v2_post "$BASE_URL/api/v2/services/instances/" "$CREATE_BODY")
echo "$CREATE_RESPONSE" | jq .

SUCCESS=$(echo "$CREATE_RESPONSE" | jq -r '.success // false' 2>/dev/null || echo false)
if [[ "$SUCCESS" != "true" ]]; then
  echo "[-] Create request failed"
  exit 1
fi

SERVICE_ID=$(echo "$CREATE_RESPONSE" | jq -r '.data.service_id // .data.service.service_id // .service_id // empty')
if [[ -z "$SERVICE_ID" ]]; then
  echo "[-] Create response did not include service_id"
  exit 1
fi

echo "[+] Service ID: $SERVICE_ID"
echo "[+] Waiting for VM status, public IPv4, and password..."

DETAIL_RESPONSE=""
PASSWORD_RESPONSE=""
STATUS=""
PUBLIC_IPV4=""
VM_CODE=""
SSH_USER=""
SSH_PASSWORD=""

for _ in {1..90}; do
  DETAIL_RESPONSE=$(api_v2_get "$BASE_URL/api/v2/services/instances/$SERVICE_ID/" || true)
  STATUS=$(echo "$DETAIL_RESPONSE" | jq -r '.data.service.status // empty' 2>/dev/null || true)
  PUBLIC_IPV4=$(echo "$DETAIL_RESPONSE" | jq -r '.data.access.public_ipv4 // .data.vm.ipv4 // empty' 2>/dev/null || true)
  VM_CODE=$(echo "$DETAIL_RESPONSE" | jq -r '.data.links.vm_code // .data.service.metadata.vm_code // .data.vm.vm_code // empty' 2>/dev/null || true)

  if [[ -n "$VM_CODE" ]]; then
    PASSWORD_RESPONSE=$(api_v2_get "$BASE_URL/api/v2/vms/$VM_CODE/actions/access/" || true)
    SSH_USER=$(echo "$PASSWORD_RESPONSE" | jq -r '.data.vmUsername // empty' 2>/dev/null || true)
    SSH_PASSWORD=$(echo "$PASSWORD_RESPONSE" | jq -r '.data.tempPass // empty' 2>/dev/null || true)
  fi

  if [[ "$STATUS" == "running" && -n "$PUBLIC_IPV4" && -n "$SSH_USER" && -n "$SSH_PASSWORD" ]]; then
    break
  fi

  echo "[*] status=${STATUS:-unknown} ip=${PUBLIC_IPV4:-pending} vm_code=${VM_CODE:-pending} password_set=$([[ -n "$SSH_PASSWORD" ]] && echo true || echo false)"
  sleep 10
done

if [[ "$STATUS" != "running" || -z "$PUBLIC_IPV4" || -z "$SSH_USER" || -z "$SSH_PASSWORD" ]]; then
  echo "[-] VM was created, but did not become ready in time"
  echo "[DEBUG] Last detail response:"
  echo "$DETAIL_RESPONSE" | jq . || true
  if [[ -n "$PASSWORD_RESPONSE" ]]; then
    echo "[DEBUG] Last access response:"
    echo "$PASSWORD_RESPONSE" | jq . || true
  fi
  exit 1
fi

echo "[+] Ready"
echo "service_id=$SERVICE_ID"
echo "vm_code=$VM_CODE"
echo "status=$STATUS"
echo "hostname=$HOSTNAME"
echo "public_ipv4=$PUBLIC_IPV4"
echo "ssh_username=$SSH_USER"
echo "ssh_password=$SSH_PASSWORD"
