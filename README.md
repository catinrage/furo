# Host-Tun / FURO

FURO is a TCP tunnel that keeps a PHP host in the middle of the path:

- `furo-client.go` runs on the Iran VPS and exposes a local SOCKS5 proxy.
- `furo-server.go` runs on the Outer VPS and dials the final internet targets.
- `furo-relay.php` runs on the PHP host and bridges long-lived client/server sessions.

The point of the project is to use a PHP host as a relay while still keeping latency and throughput practical by using persistent multiplexed sessions instead of HTTP polling per connection.

## Topology

```text
Application / Browser / Xray / 3x-ui
                |
                v
      +-------------------+
      | Iran VPS          |
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
      | Outer VPS         |
      | furo-server       |
      | dials targets     |
      +-------------------+
                |
                v
          Internet targets
```

## How it works

1. `furo-client` opens one or more long-lived session requests to `furo-relay.php`.
2. `furo-relay.php` connects back to the client agent port and the server agent port.
3. `furo-server` accepts those session connections and authenticates them with the shared key.
4. SOCKS streams are multiplexed over the session(s) using binary frames: `OPEN`, `OPEN_OK`, `OPEN_ERR`, `DATA`, `CLOSE`.
5. The server dials real targets and returns traffic through the relay back to the client.

This avoids the old file queue / polling design and keeps the PHP host out of per-connection request churn.

## Required components

- Iran VPS with outbound access to the PHP host
- Outer VPS with outbound access to final internet targets
- PHP host with:
  - PHP 8+
  - sockets extension enabled
  - ability to open outbound TCP connections to both VPSes
- Open TCP access from the PHP host to:
  - client agent port (`28080` by default)
  - server agent port (`28081` by default)
- Same `api_key` configured on all three components

## Build

If `go` is installed in `/usr/local/go/bin/go`:

```bash
/usr/local/go/bin/go build -o furo-client ./furo-client.go
/usr/local/go/bin/go build -o furo-server ./furo-server.go
```

If `go` is already on `PATH`:

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

Server on the Outer VPS:

```bash
./furo-server -c config.server.json
```

Client on the Iran VPS:

```bash
./furo-client -c config.client.json
```

Deploy the PHP relay file as `furo-relay.php` on the relay host, then make sure `relay_url` in `config.client.json` matches the real URL.

## Client config

File: [config.client.json](/home/catinrage/projects/Host-Tun/config.client.json)

- `relay_url`
  URL of `furo-relay.php`.
  If this path is wrong, the client will never establish sessions.

- `api_key`
  Shared secret used by client, server, and relay.
  All three values must match exactly.

- `socks_listen`
  Local SOCKS5 listener address on the Iran VPS.
  Example: `0.0.0.0:18713`

- `agent_listen`
  TCP listener the PHP relay connects back to on the client side.
  This must be reachable from the PHP host.

- `public_host`
  Public IP or DNS name of the Iran VPS as seen by the PHP host.

- `public_port`
  Public TCP port on the Iran VPS that maps to `agent_listen`.

- `server_host`
  Public IP or DNS name of the Outer VPS as seen by the PHP host.

- `server_port`
  Public TCP port on the Outer VPS that maps to the server agent listener.

- `open_timeout`
  Timeout for session setup HTTP headers.
  Higher values tolerate a slower PHP host.
  Lower values fail faster when the relay is unhealthy.

- `keepalive`
  TCP keepalive period for session and stream sockets.
  Usually leave at `30s`.

- `session_count`
  Number of multiplexed relay sessions the client keeps open.
  This is one of the main throughput/fairness tuning knobs.
  More sessions reduce head-of-line blocking, but too many sessions increase load on the PHP host.
  In your recent testing, `8` was a good value.

- `log_file`
  Optional log file path.
  Empty string disables Go debug logs completely.

## Server config

File: [config.server.json](/home/catinrage/projects/Host-Tun/config.server.json)

- `api_key`
  Must match the client and relay.

- `agent_listen`
  TCP listener that accepts relay session connections from the PHP host.

- `dial_timeout`
  Timeout for outbound target connects from the Outer VPS.
  Lower values fail bad targets faster.
  Higher values help on slow destinations, but keep dead targets around longer.

- `keepalive`
  TCP keepalive period for server-side sockets.

- `max_sessions`
  Maximum number of active multiplexed sessions accepted by the server.
  Set this at or above the client `session_count`.
  If it is lower than the client value, some sessions will be rejected.

- `log_file`
  Optional log file path.
  Empty string disables Go debug logs completely.

## Relay config

File: [furo-relay.php](/home/catinrage/projects/Host-Tun/furo-relay.php)

Top-of-file variables:

- `$RELAY_API_KEY`
  Shared secret. Must match both JSON configs.

- `$RELAY_CONNECT_TIMEOUT_SEC`
  TCP connect timeout when the relay attaches to client and server.

- `$RELAY_IDLE_TIMEOUT_SEC`
  Session idle timeout inside the relay bridge loop.

- `$RELAY_BUFFER_SIZE`
  PHP socket read buffer size.
  Larger values can improve throughput, but very large values can increase per-iteration burstiness.

- `$RELAY_ENABLE_LOGS`
  `true` enables relay logging via `error_log()`.
  `false` keeps the relay quiet.

## Tuning notes

- `session_count` / `max_sessions`
  Best first tuning knob.
  Too low: heavy streams block lighter traffic.
  Too high: more load on the PHP host.

- `maxFramePayload`
  Currently fixed in code at `128 KiB`.
  This was reduced from `256 KiB` to cut long writer stalls.

- Fair scheduling
  The server now uses a queued fair sender so one heavy stream cannot repeatedly hold the only session writer.
  This is important for x-ui and whole-system tunnel mode.

- Logging
  Logging is now opt-in on all three components.
  Use it only while debugging.

## GitHub release automation

Workflow file:

- [.github/workflows/release.yml](/home/catinrage/projects/Host-Tun/.github/workflows/release.yml)

What it does:

- on every push to `main`
  - builds `furo-client` and `furo-server`
  - packages them together with `furo-relay.php`, both config files, and `README.md`
  - creates a GitHub **prerelease**
- on every pushed tag like `v1.2.3`
  - runs the same build
  - creates a normal GitHub release for that tag

Release assets:

- `furo_<version>_linux_amd64.tar.gz`
- `furo_<version>_linux_amd64.tar.gz.sha256`
- raw `furo-relay.php`

The Go binaries receive embedded version metadata via `ldflags`, so `--version` reflects the release version, commit, and build date.

## Auto versioning on each commit

The workflow already gives each `main` commit a unique prerelease version:

```text
0.0.<run_number>-<short_sha>
```

Example:

```text
0.0.42-a1b2c3d
```

This is the safest form of automatic per-commit versioning:

- every commit gets a unique build version
- normal semantic tags like `v1.0.0` are still available for official releases
- the repo does not need to create and push a permanent tag for every commit

If you want strict semantic version automation instead, the next step would be using:

- Release Please
- semantic-release
- GitVersion

Those tools are better when you want commit-message-driven version bumps such as:

- `feat:` -> minor
- `fix:` -> patch
- breaking change -> major

For this repo, the current setup is pragmatic:

- branch commits -> auto-versioned prereleases
- version tags -> official releases

## Typical deployment checklist

1. Upload [furo-relay.php](/home/catinrage/projects/Host-Tun/furo-relay.php) to the PHP host.
2. Confirm `relay_url` points to the deployed file.
3. Open client agent port on Iran VPS.
4. Open server agent port on Outer VPS.
5. Build and run `furo-server`.
6. Build and run `furo-client`.
7. Test the SOCKS endpoint:

```bash
curl --socks5-hostname 127.0.0.1:18713 https://api.ipify.org
```

## Notes

- Empty `log_file` means no Go debug logs are written at all.
- The relay only supports TCP. UDP-based traffic is outside this design.
- If the PHP host path changes, update only `relay_url`; the transport protocol stays the same.
