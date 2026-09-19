# Linux container deployment

The laptop runs natively through `./tree` (see the README). This document is the
contract for the optional Linux host: the same application, the same database
schema and the same buckets, assembled from three images instead of Homebrew.

`docker-compose.yml` is the reference assembly. The deployment on
`sitzfleisch` uses the same topology with Podman Quadlets managed by the private
`ppdms/docker` homelab control plane; nothing in this repository needs to change
to run it, but the notes below are what that deployment depends on.

## Images

| Role | Reference |
| --- | --- |
| Application (browser build, Go binary, parser, helpers, static assets) | built from `Dockerfile`, one image |
| Database | `docker.io/library/postgres:18.6` |
| Object storage | `docker.io/chrislusf/seaweedfs:4.46` |

Base images in `Dockerfile` and `docker-compose.yml` are fully qualified
(`docker.io/...`) because Podman enforces short-name resolution on hosts without
an interactive terminal. Pin the built application image by immutable id or
digest; the deployment does.

## Commands

The compiled binary takes one of:

- `container-serve` — supervises the API as a child process, blocks until it
  exits, and rewrites the tool paths inside the image (`containerTools`):
  `parser_root=/app`, `frontend_dir=/app/frontend`,
  `parser_python=/usr/local/bin/python3.14`, `tessdata=/opt/tessdata`,
  `discord_exporter=/opt/discord-exporter/DiscordChatExporter.Cli`,
  `pdf_diff=/usr/local/bin/diff-pdf`. It validates that `temp` is absolute,
  `session` is non-empty and `mode` is `stable`.
- `container-migrate` — the same wrapper running the child `migrate`: applies the
  embedded Goose migrations, then creates/versions the S3 buckets. Run it while
  no application instance is running; the advisory locks refuse otherwise
  ("another application owns this database"). This is the **only** step that
  creates buckets, and compose disables SeaweedFS auto-creation.
- `migrate`, `collect`, `manifest`, `_supervise`, `_exec` — the native developer
  commands; `container-serve` never migrates, it only verifies the exact
  migration ledger at startup.

## Runtime configuration

`TREE_RUNTIME_CONFIG` points at the JSON read by `server.Load`. In containers
only these keys matter:

```json
{
  "allowed_hosts": ["uni.apps.lan", "uni.ppdms.gr"],
  "allowed_origins": ["https://uni.apps.lan", "https://uni.ppdms.gr"],
  "database_url": "postgresql://tree:PASSWORD@tree-postgres:5432/tree_app?sslmode=disable",
  "s3_endpoint": "http://tree-seaweed:8333",
  "s3_access": "ACCESS_KEY",
  "s3_secret": "SECRET_KEY",
  "address": "0.0.0.0:8001",
  "mode": "stable",
  "release": "3d125a3",
  "session": "RANDOM_SESSION_TOKEN",
  "temp": "/jobs",
  "external_workers": true,
  "provider_keys": {"ZAI_API_KEY": "..."}
}
```

- `database_url`, `s3_endpoint`, `s3_access`, `s3_secret`, `mode` and `session`
  are mandatory; `address` defaults to port 80 if omitted.
- `external_workers` **must be `true`**: with the zero value the sync, Discord,
  notification and analysis workers park and their triggers answer 503.
- `allowed_hosts` / `allowed_origins` must contain every name a reverse proxy
  forwards, or every request is answered `403 Untrusted host`. Loopback values
  are always accepted.
- `provider_keys` carries the AI credentials; the provider aliases are listed in
  `domain/settings/provider_keys.go`.

## Storage

- **Database** — PostgreSQL 18, user `tree`, one database owned by this
  application. `admitDatabase` refuses a database that contains tables but no
  `public.tree_go_migrations` ledger, so an existing pre-rewrite (Python-era)
  database cannot be adopted: create a fresh one. There is no SQLite import
  path; `eclass.db`, `knowledge.db` and `discord_knowledge.db` are legacy files.
- **Buckets** — `tree-eclass-data` (versioned; every write needs a version id)
  and `tree-eclass-cache` (7-day expiry). Keys are `objects/<sha256>`, objects
  are capped at 50 MiB.
- **SeaweedFS identity** (`-s3.config`, `s3.json`) — the shape the native
  controller writes:

```json
{"identities":[{"name":"tree","credentials":[{"accessKey":"...","secretKey":"..."}],
                "actions":["Admin","Read","Write","List","Tagging"]}]}
```

  `Admin` is required because `blob.Setup` creates buckets, enables versioning
  and sets the cache lifecycle. `accessKey`/`secretKey` must equal `s3_access`
  and `s3_secret`.

- **`/jobs`** — the `temp` directory: parser workspaces, upload spool, PDF-diff
  and Discord staging, the helper registry (`.helpers`), the supervisor state
  (`.processes/`), the container lock and the generated
  `container-runtime.json`. It must be writable by the container user and it
  holds the application logs: the API child writes
  `.processes/api/output.log` (8 MiB, one rotation) and `.processes/api/supervisor.log`,
  **not** container stdout.

## Runtime expectations

- `GET /api/health` answers `200 {"status":"ok"}` when the database answers;
  `GET /api/runtime` returns `{mode,release,session}`.
- The static frontend is served by the same process; `index.html` must carry the
  `__TREE_RUNTIME__`, `__TREE_STORAGE__`, `__TREE_MODE__` and `__TREE_BUILD__`
  markers, which the Vite build inserts.
- The helper memory supervisor samples descendant RSS through `ps -axo
  pid=,ppid=,rss=`, so the final image must keep a `ps` implementation
  (Debian trixie slim images provide `procps`). Without it every parse, PDF
  difference and Discord export is killed after ~250 ms.
- `container-serve` exits non-zero when it is asked to stop, so a container
  engine sees a graceful stop as a failure; run it with `Restart=no` (or
  `SuccessExitStatus=1` under systemd) and treat the exit code as informational.
- Shutdown budget: the supervisor waits up to 45 s for the API child, which
  spends up to 30 s draining; allow a 90 s stop timeout.
- The reverse proxy must forward `Host` unchanged and may forward `Origin`;
  `/mcp` and `/api` need no WebSocket upgrade, the browser UI does not need one
  either in stable mode.
