# unituner

Tuner sharing and abstraction server with a minimal mirakurun-compatible HTTP API.
The full implementation spec is [docs/SPEC.md](docs/SPEC.md) (Japanese).

Phase 1 currently implements configuration loading/validation and logging only.
The HTTP server, sessions, pipelines, TS filtering, scanning and webui land in
later phases.

## Configuration

Config lives in `/etc/unituner` by default (`--config-dir` overrides it) and is
split into `server.yml`, `tuners.yml`, `channels.yml` and `pipelines.yml`.
Missing optional fields fall back to the defaults below. Unknown fields are
rejected (e.g. `cmd:` instead of `command:`), and validation is fail-fast at
startup.

### `server.yml` defaults

| Field | Default | Notes |
|---|---|---|
| `addr` | `":40772"` | Binds all interfaces (0.0.0.0:40772), matching the mirakurun port. |
| `allowed_cidrs` | `10.0.0.0/8`, `127.0.0.0/8`, `172.16.0.0/12`, `192.168.0.0/16` | RFC1918 + loopback. Network control, not authentication. |
| `idle_timeout_secs` | `5` | Session teardown delay after the last subscriber leaves. |
| `subscriber_buffer_chunks` | `1024` | Per-subscriber bounded queue, in chunks. |
| `chunk_size_bytes` | `65536` | Chunk size handed to subscribers. |

### Example

```yaml
# /etc/unituner/server.yml
addr: ":40772"
allowed_cidrs: [10.0.0.0/8, 127.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16]
idle_timeout_secs: 5
subscriber_buffer_chunks: 1024
chunk_size_bytes: 65536
```

```yaml
# /etc/unituner/tuners.yml
- name: pt3-gr0
  types: [GR]
  command: "recpt1 --device /dev/pt3video0 {channel} -"
```

```yaml
# /etc/unituner/channels.yml
- name: NHK General
  type: GR
  channel: "27"
```

```yaml
# /etc/unituner/pipelines.yml
# Stages run as argv-spawned processes, chained stdin -> stdout.
# Stage 1 here is the external B25 decoder (mirakurun-style install:
#   sudo npm install arib-b25-stream-test -g --unsafe-perm).
pipelines:
  cas:
    - "arib-b25-stream-test"
types:
  GR: { pipeline: cas }
```

### Channel types and channel values

* Type names must match `^[A-Za-z0-9][A-Za-z0-9_-]*$` (e.g. `GR`, `BS`, `CABLE_CATV`).
* `channels.yml` `channel` values are single argv tokens. Allowed characters are
  ASCII alphanumerics plus `. _ + - : @ ,`. `/` is rejected so that a value
  always fits in one URL path segment. Whitespace, quotes, backslash and shell
  metacharacters are rejected. The same check applies to `{channel}` values
  taken from URL paths.

### Logging

Set `UNITUNER_LOG` (or `RUST_LOG`) to a `tracing` filter; the default is `info`.

## Known limitations

Phase 1 only loads and validates config; `unituner` exits after printing
`phase 1: config loaded`. Everything else in docs/SPEC.md (§4 onward) is not yet
implemented.
