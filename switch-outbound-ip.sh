#!/usr/bin/env bash
set -euo pipefail

CHECK_URL="${CHECK_URL:-https://chabokan.net/ip/}"
STATE_DIR="/var/lib/outbound-ip"
STATE_FILE="$STATE_DIR/selected-ip"
RULE_PREF_BASE=1000
TABLE_BASE=2000
MAX_SLOTS=256
DHCP_REFRESH="${DHCP_REFRESH:-auto}"
DHCP_REFRESH_TIMEOUT="${DHCP_REFRESH_TIMEOUT:-20}"
DHCP_REFRESH_ATTEMPTS="${DHCP_REFRESH_ATTEMPTS:-6}"
DHCP_REFRESH_INTERVAL="${DHCP_REFRESH_INTERVAL:-2}"
QUIET=0

usage() {
  cat <<'EOF'
Usage:
  ./switch-outbound-ip.sh [--quiet] [ipv4]

When ipv4 is provided, the script runs non-interactively.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --quiet)
      QUIET=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      if [[ -n "${SELECTED_IP:-}" ]]; then
        echo "Unexpected argument: $1" >&2
        usage >&2
        exit 1
      fi
      SELECTED_IP="$1"
      shift
      ;;
  esac
done

[ "$EUID" -eq 0 ] || { echo "Run as root"; exit 1; }
mkdir -p "$STATE_DIR"

get_router() {
  local iface=$1
  local router=""

  if command -v dhcpcd >/dev/null 2>&1; then
    router="$(dhcpcd -U "$iface" 2>/dev/null | awk -F= '$1=="routers" {print $2; exit}')"
  fi

  if [[ -z "$router" ]]; then
    router="$(ip -4 route show default dev "$iface" 2>/dev/null | awk '{for (i=1; i<NF; i++) if ($i=="via") {print $(i+1); exit}}')"
  fi

  printf '%s' "$router"
}

get_connected_net() {
  local iface=$1
  ip -4 route show dev "$iface" scope link | awk 'NR==1 {print $1; exit}'
}

collect_addrs() {
  mapfile -t ADDRS < <(
    ip -4 -o addr show scope global |
    awk '{split($4,a,"/"); print $2, a[1]}' |
    sort -u
  )
}

list_candidate_ifaces() {
  {
    for entry in "${ADDRS[@]:-}"; do
      read -r IFACE _ <<< "$entry"
      printf '%s\n' "$IFACE"
    done

    ip -4 route show default 2>/dev/null |
      awk '{for (i=1; i<NF; i++) if ($i=="dev") {print $(i+1)}}'

    ip -o link show up 2>/dev/null |
      awk -F': ' '$2!="lo" {sub(/@.*/, "", $2); print $2}'
  } | awk 'NF && !seen[$0]++'
}

run_with_timeout() {
  if command -v timeout >/dev/null 2>&1; then
    timeout "$DHCP_REFRESH_TIMEOUT" "$@"
  else
    "$@"
  fi
}

refresh_dhcp_addresses() {
  [[ "$DHCP_REFRESH" != "0" && "$DHCP_REFRESH" != "false" && "$DHCP_REFRESH" != "off" ]] || return 1

  local attempted=0
  local iface
  for iface in "$@"; do
    [[ -n "$iface" && "$iface" != "lo" ]] || continue

    if command -v dhcpcd >/dev/null 2>&1; then
      attempted=1
      run_with_timeout dhcpcd -4 -n "$iface" >/dev/null 2>&1 ||
        run_with_timeout dhcpcd -4 "$iface" >/dev/null 2>&1 ||
        true
      continue
    fi

    if command -v dhclient >/dev/null 2>&1; then
      attempted=1
      run_with_timeout dhclient -4 -1 "$iface" >/dev/null 2>&1 || true
      continue
    fi

    if command -v networkctl >/dev/null 2>&1; then
      attempted=1
      networkctl renew "$iface" >/dev/null 2>&1 || true
    fi
  done

  [[ "$attempted" -eq 1 ]]
}

declare -A GW_BY_IFACE
declare -A NET_BY_IFACE
declare -A FIRST_IP_BY_IFACE

build_iface_state() {
  GW_BY_IFACE=()
  NET_BY_IFACE=()
  FIRST_IP_BY_IFACE=()

  local entry IFACE IP
  for entry in "${ADDRS[@]}"; do
    read -r IFACE IP <<< "$entry"

    if [[ -z "${GW_BY_IFACE[$IFACE]:-}" ]]; then
      GW_BY_IFACE["$IFACE"]="$(get_router "$IFACE")"
    fi

    if [[ -z "${NET_BY_IFACE[$IFACE]:-}" ]]; then
      NET_BY_IFACE["$IFACE"]="$(get_connected_net "$IFACE")"
    fi

    if [[ -z "${FIRST_IP_BY_IFACE[$IFACE]:-}" ]]; then
      FIRST_IP_BY_IFACE["$IFACE"]="$IP"
    fi
  done
}

find_selected_iface() {
  SELECTED_IFACE=""
  local entry IFACE IP
  for entry in "${ADDRS[@]}"; do
    read -r IFACE IP <<< "$entry"
    if [[ "$IP" == "$SELECTED_IP" ]]; then
      SELECTED_IFACE="$IFACE"
      break
    fi
  done
}

reload_address_state() {
  collect_addrs
  build_iface_state
}

wait_for_selected_ip_after_dhcp() {
  local ifaces=()
  mapfile -t ifaces < <(list_candidate_ifaces)
  refresh_dhcp_addresses "${ifaces[@]}" || return 1

  local attempt
  for ((attempt=1; attempt<=DHCP_REFRESH_ATTEMPTS; attempt++)); do
    reload_address_state
    find_selected_iface
    [[ -n "${SELECTED_IFACE:-}" ]] && return 0
    sleep "$DHCP_REFRESH_INTERVAL"
  done

  return 1
}

collect_addrs
if [[ "${#ADDRS[@]}" -eq 0 ]]; then
  mapfile -t DHCP_IFACES < <(list_candidate_ifaces)
  refresh_dhcp_addresses "${DHCP_IFACES[@]}" || true
  collect_addrs
fi

[ "${#ADDRS[@]}" -gt 0 ] || { echo "No global IPv4 addresses found"; exit 1; }

build_iface_state

SELECTED_IP="${SELECTED_IP:-}"
SELECTED_IFACE=""

if [[ -z "$SELECTED_IP" ]]; then
  echo "Detected IPv4 addresses:"
  for i in "${!ADDRS[@]}"; do
    read -r IFACE IP <<< "${ADDRS[$i]}"
    echo "$((i+1))) $IP on $IFACE"
  done

  echo
  echo -n "Current outbound IP: "
  curl -4 -sL "$CHECK_URL" || true
  echo

  read -rp "Choose outbound IP number: " CHOICE
  INDEX=$((CHOICE-1))
  read -r SELECTED_IFACE SELECTED_IP <<< "${ADDRS[$INDEX]:-}"
else
  find_selected_iface
  if [[ -z "${SELECTED_IFACE:-}" ]]; then
    wait_for_selected_ip_after_dhcp || true
  fi
fi

[ -n "${SELECTED_IP:-}" ] || { echo "Invalid selection"; exit 1; }
[ -n "${SELECTED_IFACE:-}" ] || { echo "Selected IP not found after DHCP refresh"; exit 1; }

SELECTED_GW="${GW_BY_IFACE[$SELECTED_IFACE]:-}"
[ -n "$SELECTED_GW" ] || { echo "No gateway found for $SELECTED_IFACE"; exit 1; }

printf '%s\n' "$SELECTED_IP" > "$STATE_FILE"

for ((i=1; i<=MAX_SLOTS; i++)); do
  PREF=$((RULE_PREF_BASE + i))
  TABLE_ID=$((TABLE_BASE + i))

  while ip rule show | grep -q "^${PREF}:"; do
    ip rule del pref "$PREF" || true
  done

  ip route flush table "$TABLE_ID" 2>/dev/null || true
done

for i in "${!ADDRS[@]}"; do
  read -r IFACE IP <<< "${ADDRS[$i]}"
  GW="${GW_BY_IFACE[$IFACE]:-}"
  NET="${NET_BY_IFACE[$IFACE]:-}"
  TABLE_ID=$((TABLE_BASE + i + 1))
  PREF=$((RULE_PREF_BASE + i + 1))

  [[ -n "$GW" ]] || continue
  [[ -n "$NET" ]] || continue

  ip route add "$NET" dev "$IFACE" src "$IP" table "$TABLE_ID"
  ip route add default via "$GW" dev "$IFACE" src "$IP" table "$TABLE_ID"
  ip rule add pref "$PREF" from "$IP/32" table "$TABLE_ID"
done

ip route replace default via "$SELECTED_GW" dev "$SELECTED_IFACE" src "$SELECTED_IP" metric 50

for IFACE in "${!FIRST_IP_BY_IFACE[@]}"; do
  [[ "$IFACE" == "$SELECTED_IFACE" ]] && continue
  GW="${GW_BY_IFACE[$IFACE]:-}"
  IP="${FIRST_IP_BY_IFACE[$IFACE]}"
  [[ -n "$GW" ]] || continue
  ip route replace default via "$GW" dev "$IFACE" src "$IP" metric 500
done

sysctl -qw net.ipv4.conf.all.rp_filter=2
for IFACE in "${!FIRST_IP_BY_IFACE[@]}"; do
  sysctl -qw "net.ipv4.conf.${IFACE}.rp_filter=2" || true
done

ip route flush cache 2>/dev/null || true

if [[ "$QUIET" -eq 0 ]]; then
  echo
  echo "ip rule:"
  ip rule

  echo
  echo "default routes:"
  ip route show default

  echo
  echo "Route tests:"
  for entry in "${ADDRS[@]}"; do
    read -r IFACE IP <<< "$entry"
    ip route get 8.8.8.8 from "$IP"
  done

  echo
  echo -n "New outbound IP: "
  curl -4 -sL "$CHECK_URL" || true
  echo
fi

# Also keep rp_filter disabled persistently:
# cat > /etc/sysctl.d/99-multihomed.conf <<'EOF'
# net.ipv4.conf.all.rp_filter=0
# net.ipv4.conf.default.rp_filter=0
# EOF

# sysctl --system
