# FURO

FURO is a TCP tunnel that keeps a PHP host in the middle of the path.

- `furo-client.go` runs on the client-side VPS and exposes a local SOCKS5 proxy.
- `furo-server.go` runs on the exit VPS and dials final internet targets.
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

1. `furo-client` keeps `session_count` relay requests open against `furo-relay.php`.
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

- `relay_url`
  URL of the deployed `furo-relay.php`.
- `api_key`
  Shared secret. Must match the server and relay.
- `socks_listen`
  Local SOCKS5 listener for applications.
- `agent_listen`
  TCP listener that the PHP host connects back to.
- `public_host` / `public_port`
  Public address of the client agent as seen by the PHP host.
- `server_host` / `server_port`
  Public address of the server agent as seen by the PHP host.
- `admin_listen`
  Optional local admin HTTP listener. Recommended on loopback only.
- `open_timeout`
  Timeout for relay HTTP response headers and stream open waits.
- `keepalive`
  TCP keepalive period for session and stream sockets.
- `session_count`
  Number of multiplexed sessions to keep open.
- `log_file`
  Optional debug log path. Empty disables debug logs.

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
- `max_sessions`
  Maximum active relay sessions. Set this at or above the client `session_count`.
- `log_file`
  Optional debug log path. Empty disables debug logs.

### Relay config

Top-of-file variables in `furo-relay.php`:

- `$RELAY_API_KEY`
- `$RELAY_CONNECT_TIMEOUT_SEC`
- `$RELAY_IDLE_TIMEOUT_SEC`
- `$RELAY_BUFFER_SIZE`
- `$RELAY_ENABLE_LOGS`

## Build

If `go` is on `PATH`:

```bash
go build -o furo-client ./furo-client.go
go build -o furo-server ./furo-server.go
```

Print embedded build metadata:

```bash
./furo-client --version
./furo-server --version
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

Deploy `furo-relay.php` on the PHP host, then make sure `relay_url` in `config.client.json` points to the deployed URL.

## Observability

Both Go binaries now support a small admin HTTP surface when `admin_listen` is set.

- `GET /healthz`
  Lightweight health probe.
- `GET /status`
  JSON status snapshot with version, uptime, counters, and per-session state.

Example:

```bash
curl http://127.0.0.1:19080/status
curl http://127.0.0.1:19081/status
```

The client status includes:

- ready and connected session counts
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
```

Test layout:

- `tests/client`
  Unit tests for `furo-client.go`
- `tests/server`
  Unit tests for `furo-server.go`
- `tests/integration`
  PHP relay validation tests
- `tests/e2e`
  Full-stack client + PHP relay + server tunnel test

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
- `furo-relay.php`
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
