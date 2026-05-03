#!/usr/bin/env bash

set -euo pipefail

SCRIPT_PATH="$(readlink -f "${BASH_SOURCE[0]}")"
SCRIPT_DIR="$(cd "$(dirname "$SCRIPT_PATH")" && pwd)"

SYSTEMCTL_BIN="${SYSTEMCTL_BIN:-systemctl}"
SERVICE_DIR="${SYSTEMD_SERVICE_DIR:-/etc/systemd/system}"
SERVICE_USER="${SERVICE_USER:-root}"
FURO_SERVICE_NAMESPACE="${FURO_SERVICE_NAMESPACE:-}"
FURO_UPDATE_REPO="${FURO_UPDATE_REPO:-catinrage/furo}"
FURO_UPDATE_API_URL="${FURO_UPDATE_API_URL:-https://api.github.com/repos/${FURO_UPDATE_REPO}/releases}"
FURO_UPDATE_INCLUDE_PRERELEASE="${FURO_UPDATE_INCLUDE_PRERELEASE:-1}"

usage() {
    cat <<'EOF'
Usage:
  ./service.sh update [--force]
  ./service.sh <start|stop|restart|status>
  ./service.sh init <server|server-node|server-master|client>
  ./service.sh <server|server-node|server-master|client|airs> <init|start|stop|restart|status|enable|disable|stateus>

Examples:
  ./service.sh update
  ./service.sh status
  ./service.sh init client
  ./service.sh init server
  ./service.sh server init
  ./service.sh client start
  ./service.sh airs init
  ./service.sh server status

Notes:
  - 'update' downloads the newest GitHub release and updates all release-managed files.
  - 'update' preserves real config.*.json files.
  - Top-level start/stop/restart operate on initialized Furo services.
  - Top-level init client creates, enables, and starts both client and AIRS.
  - Top-level init server creates, enables, and starts the standalone/node server.
  - Top-level init server-master creates, enables, and starts the server master.
  - Set FURO_SERVICE_NAMESPACE=name to keep service metadata and defaults separate.
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

download_file() {
    local url="$1"
    local output="$2"

    if command -v curl >/dev/null 2>&1; then
        local args=(-fsSL)
        if [[ -n "${GITHUB_TOKEN:-}" ]]; then
            args+=(-H "Authorization: Bearer ${GITHUB_TOKEN}")
        fi
        args+=(-H "Accept: application/vnd.github+json" -o "$output" "$url")
        curl "${args[@]}"
        return
    fi

    if command -v wget >/dev/null 2>&1; then
        local args=(-q -O "$output")
        if [[ -n "${GITHUB_TOKEN:-}" ]]; then
            args+=(--header "Authorization: Bearer ${GITHUB_TOKEN}")
        fi
        args+=(--header "Accept: application/vnd.github+json" "$url")
        wget "${args[@]}"
        return
    fi

    fail "required command not found: curl or wget"
}

current_install_version() {
    local binary
    local output

    for binary in furo-client furo-server furo-server-master furo-airs; do
        if [[ ! -x "$SCRIPT_DIR/$binary" ]]; then
            continue
        fi
        output="$("$SCRIPT_DIR/$binary" --version 2>/dev/null || true)"
        if [[ "$output" =~ version=([^[:space:]]+) ]]; then
            printf '%s' "${BASH_REMATCH[1]}"
            return
        fi
    done

    printf 'dev'
}

select_update_release() {
    local releases_json="$1"
    local include_prerelease="$2"

    require_command python3
    python3 - "$releases_json" "$include_prerelease" <<'PY'
import json
import sys

path = sys.argv[1]
include_prerelease = sys.argv[2] == "1"

with open(path, "r", encoding="utf-8") as fh:
    releases = json.load(fh)

if not isinstance(releases, list):
    raise SystemExit("GitHub releases response was not a list")

for release in releases:
    if release.get("draft"):
        continue
    if release.get("prerelease") and not include_prerelease:
        continue

    tar_asset = None
    sha_asset = None
    for asset in release.get("assets", []):
        name = asset.get("name") or ""
        if name.endswith("_linux_amd64.tar.gz"):
            tar_asset = asset
        elif name.endswith("_linux_amd64.tar.gz.sha256"):
            sha_asset = asset

    if not tar_asset:
        continue

    values = [
        release.get("name") or release.get("tag_name") or "",
        release.get("tag_name") or "",
        release.get("published_at") or "",
        tar_asset.get("browser_download_url") or "",
        tar_asset.get("name") or "",
        (sha_asset or {}).get("browser_download_url") or "",
        (sha_asset or {}).get("name") or "",
    ]
    print("\t".join(values))
    raise SystemExit(0)

raise SystemExit("no suitable linux amd64 release asset found")
PY
}

verify_archive_checksum() {
    local archive="$1"
    local checksum_file="$2"
    local expected
    local actual

    require_command sha256sum
    expected="$(awk 'NF >= 1 {print $1; exit}' "$checksum_file")"
    [[ -n "$expected" ]] || fail "empty checksum file"
    actual="$(sha256sum "$archive" | awk '{print $1}')"
    [[ "$actual" == "$expected" ]] || fail "checksum mismatch for downloaded release"
}

copy_release_tree() {
    local release_root="$1"
    local rel
    local target
    local target_tmp

    (cd "$release_root" && find . -mindepth 1 -print0) |
        while IFS= read -r -d '' rel; do
            rel="${rel#./}"
            case "$rel" in
                config.*.json)
                    printf 'Preserving existing %s\n' "$rel"
                    continue
                    ;;
            esac

            if [[ -d "$release_root/$rel" ]]; then
                mkdir -p "$SCRIPT_DIR/$rel"
            else
                target="$SCRIPT_DIR/$rel"
                mkdir -p "$(dirname "$target")"
                target_tmp="$(mktemp "$(dirname "$target")/.${rel##*/}.tmp.XXXXXX")"
                cp -a "$release_root/$rel" "$target_tmp"
                mv -f "$target_tmp" "$target"
            fi
        done
}

optional_service_name() {
    local role="$1"
    local name_file
    local service_name

    name_file="$(service_name_file_path "$role")"
    [[ -f "$name_file" ]] || return 1
    service_name="$(tr -d '\r\n' <"$name_file")"
    [[ -n "$service_name" ]] || return 1
    validate_service_name "$service_name"
    printf '%s' "$service_name"
}

stop_active_services() {
    local services=("$@")
    local service_name
    local i

    if [[ ${#services[@]} -eq 0 ]]; then
        printf 'No active Furo systemd services were detected before update.\n'
        return
    fi

    require_command "$SYSTEMCTL_BIN"

    for ((i = ${#services[@]} - 1; i >= 0; i--)); do
        service_name="${services[$i]}"
        printf 'Stopping %s\n' "$service_name"
        run_systemctl stop "$service_name"
    done
}

start_services() {
    local services=("$@")
    local service_name

    if [[ ${#services[@]} -eq 0 ]]; then
        return
    fi

    require_command "$SYSTEMCTL_BIN"
    run_systemctl daemon-reload

    for service_name in "${services[@]}"; do
        printf 'Starting %s\n' "$service_name"
        run_systemctl start "$service_name"
    done
}

update_install() {
    local force="${1:-}"
    [[ "$force" == "" || "$force" == "--force" ]] || fail "unsupported update option '$force'"

    require_command tar
    require_command find
    require_command awk

    local current_version
    local tmp_dir
    local releases_json
    local release_line
    local release_version
    local release_tag
    local release_published_at
    local archive_url
    local archive_name
    local checksum_url
    local checksum_name
    local archive_path
    local checksum_path
    local extract_dir
    local release_root
    local active_services=()
    local role
    local service_name
    local services_stopped=0

    current_version="$(current_install_version)"
    tmp_dir="$(mktemp -d)"
    trap '[[ -n "${tmp_dir:-}" ]] && rm -rf "$tmp_dir"' RETURN
    trap 'status=$?; if [[ ${services_stopped:-0} -eq 1 ]]; then printf "Update failed; starting previously active services again.\n" >&2; start_services "${active_services[@]}" || true; fi; [[ -n "${tmp_dir:-}" ]] && rm -rf "$tmp_dir"; exit "$status"' ERR

    releases_json="$tmp_dir/releases.json"
    printf 'Checking %s for releases...\n' "$FURO_UPDATE_REPO"
    download_file "$FURO_UPDATE_API_URL" "$releases_json"

    release_line="$(select_update_release "$releases_json" "$FURO_UPDATE_INCLUDE_PRERELEASE")"
    IFS=$'\t' read -r release_version release_tag release_published_at archive_url archive_name checksum_url checksum_name <<<"$release_line"

    [[ -n "$release_version" ]] || fail "release version is empty"
    [[ -n "$archive_url" ]] || fail "release archive URL is empty"

    printf 'Current version: %s\n' "$current_version"
    printf 'Latest release: %s (%s)\n' "$release_version" "$release_published_at"

    if [[ "$force" != "--force" && "$current_version" == "$release_version" ]]; then
        printf 'Already up to date. Use ./service.sh update --force to reinstall this release.\n'
        return
    fi

    for role in server server-master client airs; do
        if service_name="$(optional_service_name "$role")"; then
            if command -v "$SYSTEMCTL_BIN" >/dev/null 2>&1 && run_systemctl is-active --quiet "$service_name"; then
                active_services+=("$service_name")
            fi
        fi
    done

    archive_path="$tmp_dir/$archive_name"
    printf 'Downloading %s\n' "$archive_name"
    download_file "$archive_url" "$archive_path"

    if [[ -n "$checksum_url" ]]; then
        checksum_path="$tmp_dir/$checksum_name"
        printf 'Downloading %s\n' "$checksum_name"
        download_file "$checksum_url" "$checksum_path"
        verify_archive_checksum "$archive_path" "$checksum_path"
        printf 'Checksum verified.\n'
    else
        printf 'Warning: no checksum asset found for this release.\n' >&2
    fi

    extract_dir="$tmp_dir/extract"
    mkdir -p "$extract_dir"
    tar -xzf "$archive_path" -C "$extract_dir"
    release_root="$(find "$extract_dir" -mindepth 1 -maxdepth 1 -type d | head -n 1)"
    [[ -n "$release_root" && -d "$release_root" ]] || fail "release archive did not contain a top-level directory"

    stop_active_services "${active_services[@]}"
    services_stopped=1

    printf 'Installing release files into %s\n' "$SCRIPT_DIR"
    copy_release_tree "$release_root"

    start_services "${active_services[@]}"
    services_stopped=0
    trap - ERR
    printf 'Update complete: %s -> %s\n' "$current_version" "$release_version"
}

validate_role() {
    case "$1" in
        server-node) printf 'server' ;;
        server|server-master|client|airs) printf '%s' "$1" ;;
        *) fail "invalid role '$1'; expected 'server', 'server-node', 'server-master', 'client', or 'airs'" ;;
    esac
}

sanitize_namespace() {
    local value="$1"
    value="${value//[^A-Za-z0-9_.@-]/}"
    printf '%s' "$value"
}

role_key() {
    local role="$1"
    local ns
    ns="$(sanitize_namespace "$FURO_SERVICE_NAMESPACE")"
    if [[ -n "$ns" ]]; then
        printf '%s-%s' "$role" "$ns"
    else
        printf '%s' "$role"
    fi
}

namespaced_default_service() {
    local default_name="$1"
    local ns
    ns="$(sanitize_namespace "$FURO_SERVICE_NAMESPACE")"
    if [[ -n "$ns" ]]; then
        printf '%s-%s' "$default_name" "$ns"
    else
        printf '%s' "$default_name"
    fi
}

set_role_vars() {
    local role="$1"

    role="$(validate_role "$role")"
    case "$role" in
        server)
            ROLE_BINARY="furo-server"
            ROLE_CONFIG="config.server.json"
            ROLE_DESCRIPTION_DEFAULT="Furo Server"
            ROLE_SERVICE_DEFAULT="$(namespaced_default_service furo-server)"
            ;;
        server-master)
            ROLE_BINARY="furo-server-master"
            ROLE_CONFIG="config.server-master.json"
            ROLE_DESCRIPTION_DEFAULT="Furo Server Master"
            ROLE_SERVICE_DEFAULT="$(namespaced_default_service furo-server-master)"
            ;;
        client)
            ROLE_BINARY="furo-client"
            ROLE_CONFIG="config.client.json"
            ROLE_DESCRIPTION_DEFAULT="Furo Client"
            ROLE_SERVICE_DEFAULT="$(namespaced_default_service furo-client)"
            ;;
        airs)
            ROLE_BINARY="furo-airs"
            ROLE_CONFIG="config.client.json"
            ROLE_DESCRIPTION_DEFAULT="Furo AIRS"
            ROLE_SERVICE_DEFAULT="$(namespaced_default_service furo-airs)"
            ;;
    esac

    ROLE_WORKDIR="$SCRIPT_DIR"
    ROLE_BINARY_PATH="$ROLE_WORKDIR/$ROLE_BINARY"
}

prompt_with_default() {
    local prompt="$1"
    local default_value="$2"
    local answer=""

    if [[ "${FURO_SERVICE_NONINTERACTIVE:-0}" == "1" ]]; then
        printf '%s' "$default_value"
        return
    fi

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
    printf '%s/.service-%s-name' "$SCRIPT_DIR" "$(role_key "$role")"
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

color_text() {
    local code="$1"
    local text="$2"

    if [[ -t 1 && -z "${NO_COLOR:-}" ]]; then
        printf '\033[%sm%s\033[0m' "$code" "$text"
    else
        printf '%s' "$text"
    fi
}

status_color_code() {
    case "$1" in
        active|enabled) printf '32' ;;
        inactive|disabled) printf '90' ;;
        activating|deactivating|not-initialized|unknown) printf '33' ;;
        failed|not-found) printf '31' ;;
        *) printf '36' ;;
    esac
}

print_compact_status() {
    local role="$1"
    local service_name="$2"
    local active_state="$3"
    local enabled_state="$4"
    local active_colored
    local enabled_colored

    active_colored="$(color_text "$(status_color_code "$active_state")" "$active_state")"
    enabled_colored="$(color_text "$(status_color_code "$enabled_state")" "$enabled_state")"
    printf '%-6s %-28s %s %s\n' "$role" "$service_name" "$active_colored" "$enabled_colored"
}

top_level_status() {
    require_command "$SYSTEMCTL_BIN"

    local role
    local service_name
    local service_file
    local active_state
    local enabled_state

    printf '%-6s %-28s %s %s\n' "ROLE" "SERVICE" "ACTIVE" "ENABLED"
    for role in server server-master client airs; do
        if ! service_name="$(optional_service_name "$role")"; then
            print_compact_status "$role" "-" "not-initialized" "-"
            continue
        fi
        service_file="$(service_file_path "$service_name")"
        if [[ ! -f "$service_file" ]]; then
            print_compact_status "$role" "$service_name" "not-found" "-"
            continue
        fi
        active_state="$(run_systemctl is-active "$service_name" 2>/dev/null || true)"
        enabled_state="$(run_systemctl is-enabled "$service_name" 2>/dev/null || true)"
        [[ -n "$active_state" ]] || active_state="unknown"
        [[ -n "$enabled_state" ]] || enabled_state="unknown"
        print_compact_status "$role" "$service_name" "$active_state" "$enabled_state"
    done
}

top_level_start() {
    require_command "$SYSTEMCTL_BIN"

    local role
    local service_name
    local found=0

    run_systemctl daemon-reload
    for role in server server-master client airs; do
        if ! service_name="$(optional_service_name "$role")"; then
            continue
        fi
        require_initialized "$role" "$service_name"
        printf 'Starting %s (%s)\n' "$service_name" "$role"
        run_systemctl start "$service_name"
        found=1
    done

    [[ "$found" -eq 1 ]] || printf 'No initialized Furo services found.\n'
}

top_level_stop() {
    require_command "$SYSTEMCTL_BIN"

    local role
    local service_name
    local found=0

    for role in airs client server-master server; do
        if ! service_name="$(optional_service_name "$role")"; then
            continue
        fi
        require_initialized "$role" "$service_name"
        if run_systemctl is-active --quiet "$service_name"; then
            printf 'Stopping %s (%s)\n' "$service_name" "$role"
            run_systemctl stop "$service_name"
            found=1
        fi
    done

    [[ "$found" -eq 1 ]] || printf 'No running Furo services found.\n'
}

top_level_restart() {
    require_command "$SYSTEMCTL_BIN"

    local role
    local service_name
    local found=0

    run_systemctl daemon-reload
    for role in server server-master client airs; do
        if ! service_name="$(optional_service_name "$role")"; then
            continue
        fi
        require_initialized "$role" "$service_name"
        if run_systemctl is-active --quiet "$service_name"; then
            printf 'Restarting %s (%s)\n' "$service_name" "$role"
            run_systemctl restart "$service_name"
            found=1
        fi
    done

    [[ "$found" -eq 1 ]] || printf 'No running Furo services found.\n'
}

enable_and_start_service() {
    local role="$1"
    local service_name

    require_command "$SYSTEMCTL_BIN"
    service_name="$(load_service_name "$role")"
    require_initialized "$role" "$service_name"
    printf 'Enabling %s (%s)\n' "$service_name" "$role"
    run_systemctl enable "$service_name"
    printf 'Starting %s (%s)\n' "$service_name" "$role"
    run_systemctl start "$service_name"
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
    printf 'Manage with: ./service.sh %s <enable|start|status|restart|stop>\n' "$role"
}

top_level_init() {
    local target="$1"
    case "$target" in
        server|server-node)
            set_role_vars server
            init_service server
            enable_and_start_service server
            ;;
        server-master)
            set_role_vars server-master
            init_service server-master
            enable_and_start_service server-master
            ;;
        client)
            set_role_vars client
            init_service client
            enable_and_start_service client
            set_role_vars airs
            init_service airs
            enable_and_start_service airs
            ;;
        *)
            fail "invalid init target '$target'; expected 'server', 'server-node', 'server-master', or 'client'"
            ;;
    esac
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
    if [[ $# -ge 1 && "$1" == "update" ]]; then
        [[ $# -le 2 ]] || {
            usage
            exit 1
        }
        update_install "${2:-}"
        return
    fi

    if [[ $# -eq 1 ]]; then
        case "$1" in
            start)
                top_level_start
                return
                ;;
            stop)
                top_level_stop
                return
                ;;
            restart)
                top_level_restart
                return
                ;;
            status|stateus)
                top_level_status
                return
                ;;
        esac
    fi

    if [[ $# -eq 2 && "$1" == "init" ]]; then
        top_level_init "$2"
        return
    fi

    [[ $# -eq 2 ]] || {
        usage
        exit 1
    }

    local role="$1"
    local action="$2"

    role="$(validate_role "$role")"
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
