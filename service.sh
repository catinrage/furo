#!/usr/bin/env bash

set -euo pipefail

SCRIPT_PATH="$(readlink -f "${BASH_SOURCE[0]}")"
SCRIPT_DIR="$(cd "$(dirname "$SCRIPT_PATH")" && pwd)"

SYSTEMCTL_BIN="${SYSTEMCTL_BIN:-systemctl}"
SERVICE_DIR="${SYSTEMD_SERVICE_DIR:-/etc/systemd/system}"
SERVICE_USER="${SERVICE_USER:-root}"

usage() {
    cat <<'EOF'
Usage:
  ./service.sh <server|client> <init|start|stop|restart|status|enable|disable|stateus>

Examples:
  ./service.sh server init
  ./service.sh client start
  ./service.sh server status

Notes:
  - Run 'init' once per role before using other commands.
  - 'stateus' is accepted as an alias for 'status'.
EOF
}

fail() {
    printf 'Error: %s\n' "$*" >&2
    exit 1
}

require_command() {
    command -v "$1" >/dev/null 2>&1 || fail "required command not found: $1"
}

validate_role() {
    case "$1" in
        server|client) ;;
        *) fail "invalid role '$1'; expected 'server' or 'client'" ;;
    esac
}

set_role_vars() {
    local role="$1"

    case "$role" in
        server)
            ROLE_BINARY="furo-server"
            ROLE_CONFIG="config.server.json"
            ROLE_DESCRIPTION_DEFAULT="Furo Server"
            ROLE_SERVICE_DEFAULT="furo-server"
            ;;
        client)
            ROLE_BINARY="furo-client"
            ROLE_CONFIG="config.client.json"
            ROLE_DESCRIPTION_DEFAULT="Furo Client"
            ROLE_SERVICE_DEFAULT="furo-client"
            ;;
    esac

    ROLE_WORKDIR="$SCRIPT_DIR"
    ROLE_BINARY_PATH="$ROLE_WORKDIR/$ROLE_BINARY"
}

prompt_with_default() {
    local prompt="$1"
    local default_value="$2"
    local answer=""

    read -r -p "$prompt [$default_value]: " answer
    if [[ -z "$answer" ]]; then
        answer="$default_value"
    fi

    printf '%s' "$answer"
}

validate_service_name() {
    local value="$1"
    [[ "$value" =~ ^[A-Za-z0-9_.@-]+$ ]] || fail "service name must match [A-Za-z0-9_.@-]+"
}

service_file_path() {
    local service_name="$1"
    printf '%s/%s.service' "$SERVICE_DIR" "$service_name"
}

service_name_file_path() {
    local role="$1"
    printf '%s/.service-%s-name' "$SCRIPT_DIR" "$role"
}

save_service_name() {
    local role="$1"
    local service_name="$2"
    printf '%s\n' "$service_name" >"$(service_name_file_path "$role")"
}

load_service_name() {
    local role="$1"
    local name_file
    name_file="$(service_name_file_path "$role")"

    [[ -f "$name_file" ]] || fail "$role service is not initialized. Run './service.sh $role init' first."

    local service_name
    service_name="$(tr -d '\r\n' <"$name_file")"
    [[ -n "$service_name" ]] || fail "$role service metadata is empty. Run './service.sh $role init' again."
    validate_service_name "$service_name"
    printf '%s' "$service_name"
}

require_initialized() {
    local role="$1"
    local service_name="$2"
    local service_file

    service_file="$(service_file_path "$service_name")"
    [[ -f "$service_file" ]] || fail "$role service is not initialized. Run './service.sh $role init' first."
}

run_systemctl() {
    "$SYSTEMCTL_BIN" "$@"
}

init_service() {
    local role="$1"
    local service_name
    local description
    local service_file

    require_command "$SYSTEMCTL_BIN"
    [[ -d "$SERVICE_DIR" ]] || fail "systemd service directory not found: $SERVICE_DIR"
    [[ -x "$ROLE_BINARY_PATH" ]] || fail "binary not found or not executable: $ROLE_BINARY_PATH"
    [[ -f "$ROLE_WORKDIR/$ROLE_CONFIG" ]] || fail "config file not found: $ROLE_WORKDIR/$ROLE_CONFIG"

    service_name="$(prompt_with_default "Service name" "$ROLE_SERVICE_DEFAULT")"
    validate_service_name "$service_name"
    description="$(prompt_with_default "Description" "$ROLE_DESCRIPTION_DEFAULT")"
    service_file="$(service_file_path "$service_name")"

    cat >"$service_file" <<EOF
[Unit]
Description=$description
After=network.target

[Service]
Type=simple
User=$SERVICE_USER
WorkingDirectory=$ROLE_WORKDIR
ExecStart=$ROLE_BINARY_PATH -c $ROLE_CONFIG
Restart=always
RestartSec=5

LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
EOF

    save_service_name "$role" "$service_name"
    run_systemctl daemon-reload

    printf 'Created %s\n' "$service_file"
    printf 'Role: %s\n' "$role"
    printf 'WorkingDirectory: %s\n' "$ROLE_WORKDIR"
    printf 'ExecStart: %s -c %s\n' "$ROLE_BINARY_PATH" "$ROLE_CONFIG"
    printf 'Next steps: ./service.sh %s enable && ./service.sh %s start\n' "$role" "$role"
}

manage_service() {
    local role="$1"
    local action="$2"
    local service_name

    require_command "$SYSTEMCTL_BIN"
    service_name="$(load_service_name "$role")"
    require_initialized "$role" "$service_name"

    case "$action" in
        start|stop|restart|status|enable|disable)
            run_systemctl "$action" "$service_name"
            ;;
        stateus)
            run_systemctl status "$service_name"
            ;;
        *)
            fail "unsupported action '$action'"
            ;;
    esac
}

main() {
    [[ $# -eq 2 ]] || {
        usage
        exit 1
    }

    local role="$1"
    local action="$2"

    validate_role "$role"
    set_role_vars "$role"

    case "$action" in
        init)
            init_service "$role"
            ;;
        start|stop|restart|status|enable|disable|stateus)
            manage_service "$role" "$action"
            ;;
        *)
            usage
            fail "unsupported action '$action'"
            ;;
    esac
}

main "$@"
