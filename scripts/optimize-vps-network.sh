#!/usr/bin/env bash
set -euo pipefail

SYSCTL_CONF="/etc/sysctl.d/99-furo-tcp-tuning.conf"
LIMITS_CONF="/etc/security/limits.d/99-furo-nofile.conf"
MODULES_CONF="/etc/modules-load.d/furo-bbr.conf"

usage() {
  cat <<'EOF'
Usage:
  sudo bash scripts/optimize-vps-network.sh apply
  sudo bash scripts/optimize-vps-network.sh status
  sudo bash scripts/optimize-vps-network.sh remove

What it does:
  - enables fq + BBR when the kernel supports it
  - raises socket buffer ceilings
  - increases listen/backlog and ephemeral port capacity
  - enables a few conservative TCP latency/throughput tunings
  - raises the open-file limit for high-connection workloads

Run this on both Go VPSes, not the PHP relay host.
EOF
}

require_root() {
  if [[ "${EUID}" -ne 0 ]]; then
    echo "This script must run as root." >&2
    exit 1
  fi
}

supports_bbr() {
  if [[ -f /proc/sys/net/ipv4/tcp_available_congestion_control ]]; then
    grep -qw "bbr" /proc/sys/net/ipv4/tcp_available_congestion_control && return 0
  fi

  if modprobe tcp_bbr 2>/dev/null; then
    return 0
  fi

  return 1
}

write_sysctl_conf() {
  local cc="cubic"
  local qdisc="fq_codel"

  if supports_bbr; then
    cc="bbr"
    qdisc="fq"
    printf '%s\n' "tcp_bbr" > "${MODULES_CONF}"
  else
    rm -f "${MODULES_CONF}"
  fi

  cat > "${SYSCTL_CONF}" <<EOF
# FURO tunnel tuning for client/server VPSes.
# Safe defaults for long-lived TCP tunnel sessions and many concurrent sockets.

net.core.default_qdisc = ${qdisc}
net.ipv4.tcp_congestion_control = ${cc}

net.core.somaxconn = 4096
net.core.netdev_max_backlog = 250000
net.ipv4.tcp_max_syn_backlog = 8192

net.core.rmem_default = 262144
net.core.wmem_default = 262144
net.core.rmem_max = 33554432
net.core.wmem_max = 33554432
net.ipv4.tcp_rmem = 4096 262144 33554432
net.ipv4.tcp_wmem = 4096 262144 33554432
net.ipv4.udp_rmem_min = 16384
net.ipv4.udp_wmem_min = 16384

net.ipv4.ip_local_port_range = 10240 65535
net.ipv4.tcp_fastopen = 3
net.ipv4.tcp_mtu_probing = 1
net.ipv4.tcp_slow_start_after_idle = 0
net.ipv4.tcp_keepalive_time = 600
net.ipv4.tcp_keepalive_intvl = 30
net.ipv4.tcp_keepalive_probes = 5
net.ipv4.tcp_fin_timeout = 15
net.ipv4.tcp_tw_reuse = 1
net.ipv4.tcp_moderate_rcvbuf = 1
EOF
}

write_limits_conf() {
  cat > "${LIMITS_CONF}" <<'EOF'
# Allow high socket/file counts for tunnel daemons.
* soft nofile 1048576
* hard nofile 1048576
root soft nofile 1048576
root hard nofile 1048576
EOF
}

apply_tuning() {
  require_root
  write_sysctl_conf
  write_limits_conf
  sysctl --system

  echo
  echo "Applied FURO network tuning."
  show_status
  echo
  echo "Reboot or restart long-lived services if you want the higher nofile limits"
  echo "to apply everywhere consistently."
}

show_status() {
  echo "Kernel: $(uname -r)"
  echo "Congestion control: $(sysctl -n net.ipv4.tcp_congestion_control 2>/dev/null || echo unknown)"
  echo "Available CCs: $(cat /proc/sys/net/ipv4/tcp_available_congestion_control 2>/dev/null || echo unknown)"
  echo "Default qdisc: $(sysctl -n net.core.default_qdisc 2>/dev/null || echo unknown)"
  echo "somaxconn: $(sysctl -n net.core.somaxconn 2>/dev/null || echo unknown)"
  echo "netdev_max_backlog: $(sysctl -n net.core.netdev_max_backlog 2>/dev/null || echo unknown)"
  echo "tcp_rmem: $(sysctl -n net.ipv4.tcp_rmem 2>/dev/null || echo unknown)"
  echo "tcp_wmem: $(sysctl -n net.ipv4.tcp_wmem 2>/dev/null || echo unknown)"
  echo "ip_local_port_range: $(sysctl -n net.ipv4.ip_local_port_range 2>/dev/null || echo unknown)"
  echo "tcp_fastopen: $(sysctl -n net.ipv4.tcp_fastopen 2>/dev/null || echo unknown)"
  echo "sysctl file: ${SYSCTL_CONF}"
  echo "limits file: ${LIMITS_CONF}"
  if [[ -f "${MODULES_CONF}" ]]; then
    echo "module file: ${MODULES_CONF}"
  fi
}

remove_tuning() {
  require_root
  rm -f "${SYSCTL_CONF}" "${LIMITS_CONF}" "${MODULES_CONF}"
  sysctl --system
  echo "Removed FURO tuning files."
}

main() {
  local action="${1:-}"
  case "${action}" in
    apply)
      apply_tuning
      ;;
    status)
      show_status
      ;;
    remove)
      remove_tuning
      ;;
    -h|--help|help|"")
      usage
      ;;
    *)
      echo "Unknown action: ${action}" >&2
      usage
      exit 1
      ;;
  esac
}

main "$@"
