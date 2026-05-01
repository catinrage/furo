# FURO

FURO is a TCP tunnel that keeps a PHP host in the middle of the path.

- `furo-client.go` runs on the client-side VPS and exposes a local SOCKS5 proxy.
- `furo-server.go` runs on the exit VPS and dials final internet targets.
- `furo-airs.go` optionally renews the ArvanCloud public IP used by the client VPS.
- `furo-relay.php` runs on a PHP host and bridges long-lived client/server sessions.

The design goal is to keep the PHP host in the traffic path without falling back to per-connection HTTP polling. FURO keeps a small pool of long-lived relay sessions open and multiplexes SOCKS streams across them.

## Topology

```text
Application / Browser / Xray / 3x-ui
                |
                v
      +-------------------+
      | Client VPS        |
      | furo-client       |
      | SOCKS5 :18713     |
      +-------------------+
                |
                | long-lived multiplexed session(s)
                v
      +-------------------+
      | PHP Host          |
      | furo-relay.php    |
      | bridges TCP only  |
      +-------------------+
                |
                | long-lived multiplexed session(s)
                v
      +-------------------+
      | Exit VPS          |
      | furo-server       |
      | dials targets     |
      +-------------------+
                |
                v
          Internet targets
```

## Protocol

1. `furo-client` keeps a pool of relay requests open against `furo-relay.php`, split across one or more configured routes.
2. For each request, `furo-relay.php` connects back to the client agent port and the server agent port.
3. Both Go binaries authenticate the session with the shared `api_key`.
4. Streams are multiplexed over the session using binary frames:
   - `HELLO`
   - `HELLO_ACK`
   - `OPEN`
   - `OPEN_OK`
   - `OPEN_ERR`
   - `DATA`
   - `CLOSE`
5. `furo-server` dials the real target and forwards bytes back through the relay to the client.

## Requirements

- A client-side VPS that can reach the PHP relay URL.
- An exit VPS that can reach final internet targets.
- A PHP host with:
  - PHP 8+
  - the sockets extension enabled
  - outbound TCP access to both VPSes
- Matching `api_key` values across:
  - `config.client.json`
  - `config.server.json`
  - `furo-relay.php`

## Configuration

The repo ships example configs:

- `config.client.json.example`
- `config.server.json.example`

Create your working configs from those examples:

```bash
cp config.client.json.example config.client.json
cp config.server.json.example config.server.json
```

### Client config

Key fields in `config.client.json`:

- `client_id`
  Stable client identifier used as part of generated session ids. Set this when multiple client instances may share the same relay and server backend.
- `route_selection`
  How FURO chooses among healthy ready routes. Supported values:
  `round_robin`, `random`, `least_load`, `least_latency`.
- `api_key`
  Shared secret. Must match the server and relay.
- `socks_listen`
  Local SOCKS5 listener for applications.
- `agent_listen`
  TCP listener that the PHP host connects back to.
- `public_host` / `public_port`
  Public callback address of the client agent as seen by the PHP host. This is the default for all routes unless a route overrides it.
- `admin_listen`
  Optional local admin HTTP listener. Recommended on loopback only.
- `open_timeout`
  Timeout for relay HTTP response headers and stream open waits.
- `keepalive`
  TCP keepalive period for session and stream sockets.
- `write_timeout`
  Per-write deadline for relay session writes. Set to `0s` to disable write deadlines.
- `frame_min_size` / `frame_mid_size` / `frame_max_size`
  Adaptive DATA frame payload sizes in bytes. Keep client and server `frame_max_size` compatible; the defaults preserve the built-in 32 KiB / 64 KiB / 128 KiB behavior.
- `log_file`
  Optional debug log path. Empty disables debug logs.
- `routes`
  List of `relay -> server` paths that the client may use.
- `airs`
  Optional Arvan IP Renew System config. AIRS shares this client config, periodically or reactively adds a fresh Arvan public IP, detaches the old public IP using the IP-specific `port_id`, switches the VPS outbound source IP, updates `public_host`, runs `inspect`, and restarts the client service.

Each route entry supports:

- `id`
  Route identifier used in status output and `inspect --route-id`.
- `relay_url`
  URL of the deployed `furo-relay.php` for that route.
- `server_host` / `server_port`
  Public address of the server agent for that route, as seen by the PHP host.
- `session_count`
  Number of multiplexed sessions to keep open on that route.
- `enabled`
  Whether the route should be used.
- `public_host` / `public_port`
  Optional per-route override for the client callback address. If omitted, the top-level values are used.

Route selection always filters to healthy ready paths first, so dead routes automatically drop out of consideration. `least_load` chooses the least busy ready session, while `least_latency` chooses the route with the best recent relay request latency and then the least busy session on that route.

Legacy single-route configs using top-level `relay_url`, `server_host`, `server_port`, and `session_count` are still accepted for backward compatibility.

### AIRS config

Key fields in `config.client.json` under `airs`:

- `arvan_api_key`
  ArvanCloud API key, including the `apikey ` prefix.
- `arvan_region` / `arvan_server_id`
  ArvanCloud region and server UUID for the client VPS.
- `fixed_public_ip`
  Optional public IP that AIRS must never detach. Use this for SSH, inbounds, and any stable address you want to keep attached while `public_host` rotates.
- `auto_renew_interval_seconds`
  Scheduled renewal interval. Set to `0` to disable scheduled renewals and renew only after inspect failures.
- `check_interval_seconds`
  How often AIRS runs the lightweight `inspect` check. This cannot be disabled.
- `failure_confirm_attempts` / `failure_confirm_interval_seconds`
  Extra inspect retries before renewing after a failed health check. Defaults to 3 retries, 4 seconds apart.
- `log_file`
  AIRS log path. Empty disables AIRS logs.
- `switch_script`
  Script used to change the outbound source IP. Defaults to `./switch-outbound-ip.sh` and is called non-interactively.
- `inspect_binary`
  Path to `inspect`. AIRS assumes this lives in the same directory unless configured otherwise.
- `service_script` / `client_service_role`
  Service manager command used after a successful renewal. Defaults to `./service.sh client restart`.

### Server config

Key fields in `config.server.json`:

- `api_key`
  Shared secret. Must match the client and relay.
- `agent_listen`
  TCP listener that accepts relay session connections.
- `admin_listen`
  Optional local admin HTTP listener. Recommended on loopback only.
- `dial_timeout`
  Timeout for outbound target dials.
- `keepalive`
  TCP keepalive period for server-side sockets.
- `write_timeout`
  Per-write deadline for relay session and target writes. Set to `0s` to disable write deadlines.
- `frame_min_size` / `frame_mid_size` / `frame_max_size`
  Adaptive DATA frame payload sizes in bytes. Keep this compatible with the client-side `frame_max_size`.
- `max_pending_bytes`
  Maximum queued outbound session DATA bytes before the server closes the affected stream to protect the session from unbounded buffering.
- `max_sessions`
  Maximum active relay sessions. Set this above the total client-side sessions across all enabled routes if `inspect` may run while `furo-client` is already running, so one extra session remains available.
- `log_file`
  Optional debug log path. Empty disables debug logs.

### Relay config

Top-of-file variables in `furo-relay.php`:

- `$RELAY_API_KEY`
- `$RELAY_CONNECT_TIMEOUT_SEC`
- `$RELAY_IDLE_TIMEOUT_SEC`
- `$RELAY_BUFFER_SIZE`
- `$RELAY_IO_CHUNK_SIZE`
- `$RELAY_MAX_PENDING_BYTES`
- `$RELAY_ENABLE_LOGS`

## Build

If `go` is on `PATH`:

```bash
go build -o furo-client ./furo-client.go
go build -o furo-server ./furo-server.go
go build -o furo-airs ./furo-airs.go
go build -o inspect ./inspect.go
```

Print embedded build metadata:

```bash
./furo-client --version
./furo-server --version
./furo-airs --version
./inspect --help
```

## Run

Server on the exit VPS:

```bash
./furo-server -c config.server.json
```

Client on the client-side VPS:

```bash
./furo-client -c config.client.json
```

AIRS on the client-side VPS:

```bash
./furo-airs -c config.client.json
./furo-airs -c config.client.json --verbose
./furo-airs -c config.client.json --once
./furo-airs -c config.client.json --check-once
```

`--once` and `--check-once` print step-by-step logs to the terminal automatically. Use `--verbose` when running the continuous daemon in the foreground and you also want logs on stdout.

### systemd service manager

The repo ships a single helper script, `service.sh`, for creating and managing `furo-server`, `furo-client`, and `furo-airs` systemd units from the project directory.

Examples:

```bash
./service.sh server init
./service.sh server enable
./service.sh server start

./service.sh client init
./service.sh client restart
./service.sh client status

./service.sh airs init
./service.sh airs enable
./service.sh airs start

./service.sh update
```

Behavior:

- `update` checks GitHub releases for `catinrage/furo`, downloads the newest Linux amd64 tarball, verifies the SHA-256 asset when present, installs all release-managed files, preserves real `config.*.json` files, and restarts any Furo systemd services that were active before the update.
- `update --force` reinstalls the newest release even when the local binary reports the same version.
- `update` includes prereleases by default so `main` branch release builds are eligible. Set `FURO_UPDATE_INCLUDE_PRERELEASE=0` to use only stable tag releases.
- `init` prompts for the service name and description.
- `init` infers `WorkingDirectory` from the script location and `ExecStart` from `./furo-server -c config.server.json`, `./furo-client -c config.client.json`, or `./furo-airs -c config.client.json`.
- Other commands fail with a clear message until `init` has been completed for that role.

Relay path inspection from the client-side VPS:

```bash
./inspect -c config.client.json
./inspect -c config.client.json --route-id relay-primary
./inspect -c config.client.json --speed-test
./inspect -c config.client.json --speed-test --speed-test-url https://example.com/test.bin
./inspect --help
```

`inspect` binds `agent_listen` itself, asks the relay for a single session, verifies the server-side `HELLO/HELLO_ACK`, reports relay ping, and optionally downloads `https://nbg1-speed.hetzner.com/100MB.bin` through the tunnel. If it fails, it reports the failing stage directly, for example relay request TLS failure, relay callback timeout, server handshake failure, or speed-test stream failure.

If `furo-client` is already running, `inspect` will first try `agent_listen` and then fall back to a temporary free port if that listener is busy. The server still needs spare session capacity for this extra inspection session, so keep `max_sessions` above the client session total across all enabled routes. When `routes` are present, `inspect` uses the first enabled route by default, or a specific route when `--route-id` is passed.

Deploy `furo-relay.php` on the PHP host, then make sure `relay_url` in `config.client.json` points to the deployed URL.

### Relay-side diagnostics page

The repo also ships `furo-route-diagnostics.php`, a separate PHP page intended to live next to `furo-relay.php` on the same host.

- It prompts for a hardcoded in-file passkey before showing the UI.
- It accepts pasted `config.client.json` content instead of reading a file from disk.
- It tests TCP reachability and connect latency from the PHP host to:
  - each route's relay origin host/port
  - each route's client callback `public_host:public_port`
  - each route's server agent `server_host:server_port`
- It supports both multi-route configs and the legacy single-route fields.
- It includes a light/dark theme toggle and stores only the theme choice in `localStorage`.

Before deployment, change `$DIAGNOSTICS_PASSKEY` near the top of `furo-route-diagnostics.php`.

## Observability

Both Go binaries now support a small admin HTTP surface when `admin_listen` is set.

- `GET /healthz`
  Lightweight health probe.
- `GET /status`
  JSON status snapshot with version, uptime, counters, and per-session state.
- `GET /` on `furo-client`
  Browser panel protected by the configured `api_key`. The panel can edit the client config file and invoke `service.sh client` actions such as `start`, `stop`, `restart`, `enable`, `disable`, and `status`.

Example:

```bash
curl http://127.0.0.1:19080/status
curl http://127.0.0.1:19081/status
```

The client status includes:

- ready and connected session counts
- configured route selection and per-route health snapshots
- per-session reconnect backoff state
- relay request success/failure counters
- stream, frame, byte, and slow-write counters

The server status includes:

- active, accepted, rejected, and closed session counters
- per-session authentication and stream counts
- frame, byte, and slow-write counters

## Reconnect behavior

When the relay request fails or is rejected, the client no longer retries at a fixed cadence forever. It now applies capped exponential backoff:

- 1st failure: 1s
- 2nd failure: 2s
- 3rd failure: 4s
- 4th failure: 8s
- 5th failure: 16s
- 6th failure and beyond: 30s max

Backoff state is exposed in the client `/status` output.

## Verification

Local test commands:

```bash
go test -v ./tests/...
go test -run '^$' -bench BenchmarkFullStackTunnelThroughput ./tests/e2e
```

Test layout:

- `tests/client`
  Unit tests for `furo-client.go`
- `tests/inspect`
  Unit tests for `inspect.go`
- `tests/server`
  Unit tests for `furo-server.go`
- `tests/integration`
  PHP relay validation tests
- `tests/e2e`
  Full-stack client + PHP relay + server tunnel tests, including the standalone `inspect` binary

The integration and E2E suites require:

- PHP CLI
- the PHP `sockets` extension

CI runs on pushes, pull requests, and manual dispatch through `.github/workflows/ci.yml`. The release workflow also runs the full `./tests/...` suite before packaging artifacts.

## Releases

`.github/workflows/release.yml` builds Linux amd64 binaries and publishes:

- `furo_<version>_linux_amd64.tar.gz`
- `furo_<version>_linux_amd64.tar.gz.sha256`
- raw `furo-relay.php`

The tarball contains:

- `furo-client`
- `furo-server`
- `furo-airs`
- `inspect`
- `furo-relay.php`
- `furo-route-diagnostics.php`
- `service.sh`
- `switch-outbound-ip.sh`
- `scripts/optimize-vps-network.sh`
- `config.client.json.example`
- `config.server.json.example`
- `README.md`

For `main` branch pushes, the workflow creates a prerelease version in this form:

```text
0.0.<run_number>-<short_sha>
```

For pushed tags like `v1.2.3`, the workflow creates a normal GitHub release for that tag.

## Deployment checklist

1. Copy the example configs and update all addresses and secrets.
2. Deploy `furo-relay.php` on the PHP host.
3. Confirm `relay_url` matches the deployed relay URL.
4. Open the client agent port so the PHP host can reach it.
5. Open the server agent port so the PHP host can reach it.
6. Start `furo-server`.
7. Start `furo-client`.
8. If enabled, verify `admin_listen` on both binaries.
9. Test the SOCKS endpoint:

```bash
curl --socks5-hostname 127.0.0.1:18713 https://api.ipify.org
```

## Notes

- FURO is TCP-only. UDP is outside this design.
- Empty `log_file` disables Go debug logs.
- Keep `admin_listen` bound to loopback unless you intentionally want remote visibility.
