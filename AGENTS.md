# AGENTS.md

Guidance for AI coding agents working in this repository. Follow these rules unless the user explicitly says otherwise.

## Local development model

Development and regular use run **natively on macOS** with **manual start and
stop**, not login services. Daily commands use the installed Go controller and
the implementation lives in `backend/`. Inspect `backend/internal/app/workflow/`
before changing commands. Machine-specific operational and recovery context,
including the private local workflow runbook, lives in `docs/private/`
(gitignored, never published). The user wants **one
shared authoritative database and document store**, switching between editable
development and a fixed stable release, never running both application modes
concurrently. The final approved plan supersedes the earlier shared-write proposal:
**never keep development data**. Enter development from a cold stable checkpoint;
exit restores its exact database, objects directory and mutable settings. A new release
promotes committed code only, then migrates the restored stable dataset. Keep
synthetic verification disposable and separate from the live dataset.

Run services **natively on macOS**, with containers retained as an optional future
server deployment using the same application contracts. Move document/blob storage
to the **local content-addressed objects directory** of the active dataset;
`active/objects` is the on-disk layout. Native macOS uses PostgreSQL; deployment
configuration may select PostgreSQL or SQLite. These user choices supersede the
earlier Colima/WebDAV replacement proposal.
An idiomatic **Go backend and controller are required in this implementation**.
Short-lived Python document parsers are allowed; a persistent Python API or worker
is not the target architecture. Measure the complete process tree to verify memory
savings. Switching stops all writers and native storage before APFS cloning.
Development exit explicitly discards its writes; restoring an older stable
checkpoint must report which later stable writes are being discarded.
Stable releases require a clean committed revision. Do not commit automatically.
Use a private local bare Git remote; do not provision a hosted Git service, an
image registry, or external backup features. Local recovery checkpoints remain
required.
Release inventories include dependency symlink identity. Reject absolute, broken
or escaping links so an artifact cannot depend on its temporary build directory
or caches. Compare canonical parent paths on macOS (`/var` can resolve through
`/private/var`). Release builds use a separate disposable cache; cleanup must
preserve running development binaries, parser workspaces and browser build output.
Retention protects the selected and previous releases, the latest completed build,
all retained checkpoint release references, and the in-progress activation target.
Go serves the compiled React browser app directly from the release inventory.
Stable use starts no Bun, Node or frontend process. Vite/Bun compile assets only
during builds and development. The development compiler publishes its entry
atomically after hashed assets; failed builds preserve the last working entry.
Development reload checks the page session and never refreshes a stale tab into
new runtime. Keep static assets bounded to their release root; API/file misses
must not become HTML. Verify with `backend/internal/infrastructure/webui` and the native browser fixture. Native helper libraries and resource
paths must resolve inside the release, with macOS frameworks/system fonts as the
platform boundary. Run the complete relocated parser/helper fixture after changing
native packaging. Poppler's packaged resource path is relative to the release;
both parsing and PDF comparison must set that working directory.
Pin that target before shutdown cleanup; otherwise cleanup can delete the very
release being activated. Unknown/mismatched release manifests require inspection
and must not be automatically removed.

Native verification uses `go -C backend test ./cmd/... ./internal/...` (avoid `./...`, which
also traverses Go files inside frontend dependencies). Opt-in native integration
tests use `TREE_NATIVE_TESTS=1`; they create and remove private synthetic
PostgreSQL clusters and disposable objects directories and never use
`DATABASE_URL` or the authoritative dataset. SQL-free typed ports live in
`backend/internal/domain/database/`; directly written PostgreSQL and SQLite
queries live only in `backend/internal/infrastructure/rdbms/`.
Applied migration bytes are immutable release data: never reformat or edit an
applied migration; append a new migration for schema changes.
The full `./tree check` gate also exercises the optimized API serving the production browser build
and repeated PDF uploads; it measures all supervised process trees and requires
helpers to exit. Setup refreshes the editable Bun path after package-manager
upgrades and stages diff-pdf under the managed native tools directory; release
packaging never persists a package-manager-resolved path. Stable releases contain
only static browser assets.
Native packaging tests additionally take `TREE_TEST_PYTHON_BASE` and
`TREE_TEST_PARSER_BASE`, pointing to setup's verified interpreter and locked parser
distributions. Tests never download them. A copied venv is insufficient for stable
packaging because its base interpreter and standard library can remain outside
the artifact. Keep parser dependencies and source relocatable; run the packaged
parser fixture after changing its explicitly scoped source inventory.
Native parser OCR tests additionally take `TREE_TEST_TESSDATA`; setup provisions
only pinned Greek/English models. Keep browser persistence behind
`frontend/src/lib/browserStorage.ts` so development preferences cannot overwrite
stable keys. Native process creation must use the committed-child bootstrap in
`backend/internal/infrastructure/process/execute.go`: a storage writer may execute only after its kernel
process identity has been durably journaled. Never replace that handshake with
starting a child and recording its PID afterward. Finite migration commands use
`process.Manager.Run` and require a durable successful completion record; a lost
supervisor is never success. Stop migrations before checking cold storage. Schema
migration and application admission hold the same exclusive database ownership
lock, with compatibility checked after ownership is acquired. Short-lived parser,
PDF and Discord children use `process.StartHelper` with the runtime's private
helper registry and a replacement environment. Shutdown and reload must recover
that registry after stopping the API; its process group alone does not include
helpers that started separate process groups. Do not place ownership records in
paths controlled by document or attachment filenames. The development
watcher compiles a private source copy without the operation lock, then rechecks
source identity and selection under that lock before replacing the API. Shutdown
must stop the watcher before deleting its build output. Schema checkpoints that
contain development writes carry their baseline ID and must be removed on exit;
never allow `snapshot restore` to promote them into stable data.

Objects garbage collection is an offline controller operation using the selected
release's exact schema contract. All publishers must remain stopped throughout
catalog pruning and file deletion; a grace period alone does not protect in-flight
uploads. Preserve every catalog foreign-key reference, including historical/deleted
revisions, message media and PDF comparisons. Check per-file deletion
acknowledgements: a file leaves the objects directory only when the catalog no
longer references its content hash. Deletion completes synchronously, but
physical reclamation must still account for file blocks retained by cold
checkpoints; logical deleted bytes are not free-disk measurements.

Checker notifications enter a durable outbox in the same transaction as their
source updates. Delivery binds to the original destination hash, disables mentions,
waits for acknowledgement and shares destination-wide rate-limit cooldowns.
Claims must filter the captured destination even after canceling obsolete rows;
a concurrently committed old-target event must never be sent to a new webhook.
Delivery is at least once: a lost acknowledgement can repeat one message, while
acknowledged earlier batches remain sent. Development must never deliver webhooks.
Verify with the synthetic notification publication/delivery fixture.

Discord export intervals publish all validated partitions, raw object references,
media associations, derived conversations and the exclusive cursor in one
transaction. EOF is successful only after the closing JSON object; truncated
exports must not advance cursors. Mapping changes immediately hide old evidence
and attachments; reindex from the registered export into the new course.
Settings and import must use the same `hashtextextended` advisory-lock namespace;
the generic queue lock uses a different hash and cannot substitute for it.
DiscordChatExporter is a pinned, short-lived native helper with its bundled .NET
runtime, not a persistent service. Export one channel at a time, discover threads
separately, and never place its token in arguments or surface its raw diagnostics.
Verify with the native Discord interval/import fixtures.

ZIP/RAR indexing publishes verified immutable member objects, membership and child
index jobs atomically. Preserve unchanged child document IDs across container
revisions and mark removed leaves non-current. Ordinary upstream reconciliation
must exclude archive members from its direct-file deletion set; their current
availability depends on the complete parent chain. Reader bytes, compatibility
paths and readiness share that source boundary. The parser member-read protocol
must pass `archive_format` separately from its path/chain and checksum options.
Route ZIP/RAR directly through this member protocol, without first running the
legacy flattening extractor: ZIP detection can mistake a ZIP nested in a RAR for
the outer archive. Verify both formats with the native archive publication and
relocated parser fixtures.

The native synchronizer publishes a complete course tree in one transaction after
its crawl succeeds. Preserve hidden-course synchronization and never interpret an
upstream login/error page as an empty course. Current object reads must follow the
catalog's content hash, not revision creation time: an upstream file can change
A → B → A while reusing the same immutable content-addressed object. Upstream and user-uploaded
materials occupy separate `eclass` and `external` catalog namespaces.

Browser settings submit multipart `FormData`; native handlers must use the bounded
`formBody` parser. Go serves both page routes and API responses on one origin,
so legacy redirects return directly to the browser. React Router owns page
navigation and cancellation; API streaming never passes through a JavaScript proxy.

Browser writes carry the session embedded in the page, through `X-Tree-Runtime`
or the legacy form's `_tree_runtime` field. Never replace it with the current
frontend session or a refreshed cookie: that would authorize stale tabs after a
mode switch. Provider keys are imported into `active/settings/provider-keys.json`
before a development checkpoint; application startup reads only that file.
Reimporting keys requires the stopped stable dataset and never modifies Keychain.

Coalesce background commands only while they are pending, with the selected row
locked against claiming. A running job has already captured its inputs; later
changes must leave a pending command so finishing the old job cannot lose them.
Learner exports use explicit column allowlists and a repeatable-read snapshot;
new storage or credential fields must never silently enter portable exports.

File guides and page insights must match source hash, analysis version and the
configured model generation; page fallback output matches its requested model.
File-list metadata must not load every guide payload. Coverage has separate
source and learner generations: file-study edits refresh completion without
invalidating immutable roadmap content. Completion counts current registered
files; historical path preferences remain stored but cannot inflate completion.

Native study schedules and material priorities are background projections. Their
fingerprint includes per-course source/learner generations, navigation publication,
planner/analysis settings and the Athens calendar day. Read the fingerprint and
payload in one database snapshot; stale reads return pending rather than rebuild
evidence in an HTTP request. A study-event append invalidates scheduling without
changing immutable roadmap identity. Legacy event responses may report that the
saved progress is awaiting a schedule refresh; never report a committed append as
failed merely because its follow-up read fails.

Ask and MCP share the read-only `backend/internal/services/library` registry. Validate schemas
without converting integer IDs through float64. Only exact `/mcp` and `/mcp/`
read-only POSTs bypass the browser runtime token; Host/Origin checks still apply.
The stdio bridge holds no lifecycle lock or database/storage connection. Ask stores
complete question/answer pairs atomically after successful streaming; canceled or
failed turns must leave no half-turn. Verify with the synthetic Ask/MCP fixtures.

Document/page analysis, course/practice synthesis and indexing share one
expensive-work admission lock.
Commit claims before model or parser I/O, and compare source, context, requested
model and claim again before publication. PostgreSQL `now()` is the transaction
start time: a claim inserted later in that transaction must compare eligibility
against `clock_timestamp()`, or it can reject itself. Serving-model fallback must
not change the requested generation. Quota pauses are durable and do not consume
job retry budgets; canceled quota probes must never cache an available decision.
Generated guidance normalizes NUL characters; original source units retain their
exact reversible identity. See the native analysis integration fixture.

Course/practice packets bind immutable document revisions, exact guide payloads
and mapped conversation content. Keep trusted planning settings separate from
source evidence; map short model citation labels back to canonical IDs before
validation. Publication holds the course generation row through its final source
check and transaction commit. Community message and membership mutations must
invalidate navigation in the same transaction. A failed successor must preserve
a source-valid prior plan. Returning to an earlier packet must reactivate its
revision, not become permanently stuck behind its unique hash. Verify these
contracts with the native synthesis and community freshness fixtures.

Knowledge maintenance shares the serial extraction queue. Rebuild derived chunks
under existing document IDs, preserving object revisions, learner history and
annotations; never delete `knowledge.documents` to rebuild search. Reconciliation
repairs missing derived indexes against registered current object revisions, without
starting an upstream sync. Exhausted retries reset their attempt budget. Verify
these boundaries with the synthetic material-publication integration fixture.

Workspace heartbeats serialize by session and deduplicate the exact request.
Cap each interval at 90 seconds, retain source hashes in reading spans, and
commit the declared finish outcome and its study event in one transaction.
Repeated finish requests must match the saved outcome, note and confidence.
Hidden courses with enabled exam commitments remain valid study targets.

Native navigation publication validates the blueprint and its exact document
analysis snapshots in a repeatable-read transaction, then checks the captured
source/configuration generation again while publishing. Object-revision and
index-command mutations participate in invalidation. Keep action IDs semantic:
ordering, estimates and evidence-list changes must not erase completion history.
Progress writes use `navigation.AdmitAction` while holding the course lock, and
append the event in that same transaction. Navigation JSONB uses the reversible
`identity.EncodeJSON`/`DecodeJSON` boundary to preserve NUL and private-use text.
Native extraction failures must update the matching source revision's status;
cancellation returns it to pending without overwriting a newer source hash.

Practice reads and attempts require a current navigation generation and the exact
configured practice model/version. Question IDs preserve prompt semantics across
set regeneration. Commit each self-graded attempt and its study event atomically;
hash the full retry key when deriving the event key, never truncate it. Reader
contexts share one repeatable-read snapshot for documents, marks, reading and
practice. Deleted bookmark history must not hide a replacement live bookmark.

## How to work in this repository

This is the operating guide, not just an architecture inventory. Start here, then
read the relevant linked runbook and affected code before choosing commands or
editing. Keep this guide current using the learning rules below.

### Decision priorities

- **Respect the user's intent and existing work.** Solve the requested problem
  completely, without silently narrowing it or adding unrelated features. Treat
  reported failures as evidence to act on, not claims to challenge. Preserve
  unrelated edits and data; do not reset or overwrite them.
- **Correctness first, then maintainability.** Fix the cause, not the symptom.
  Prefer simple code and existing project patterns over new abstractions,
  compatibility layers, speculative options, or duplicated implementations.
  Avoid unnecessary allocation, copying, and computation.
- **Investigate before asking.** Use code, configuration, existing documentation,
  and available runtime evidence to resolve questions. Make routine, reversible
  decisions yourself. Ask when a missing decision materially changes scope,
  safety, or a user-visible tradeoff.
- **Space beats rebuild speed.** The low-disk requirement below is a design
  constraint for commands and experiments, not an optional end-of-task tidy-up.
- **Local development is not production work.** Do not publish, deploy, refresh
  staging, enable workers, commit, or push merely to finish an unrelated task.
  These actions need authorization in the task. Never expose private services
  or start real external-service work as an incidental verification step.

### Task workflow

1. **Scope:** identify the requested outcome, affected surfaces, relevant rules,
   and how success will be observed. Plan multi-step work before editing.
2. **Inspect:** read the implementation and its callers, tests, and runbook.
   Follow existing conventions. Use symbol-aware navigation/refactoring when
   available; account for every caller when changing a contract.
3. **Implement:** make the smallest complete change. Update affected callers,
   tests, and documentation together. Remove code made obsolete by the change;
   do not leave stubs, temporary compatibility paths, or unimplemented promises.
   Preserve intentional public facade boundaries described below.
4. **Verify:** exercise the changed behavior, not just compilation. For a bug,
   demonstrate that the failing scenario is fixed without making the user
   reproduce a failure already reported. For UI work, inspect the actual browser
   surface; for CLI work, run the command and inspect its result and side effects.
   Use disposable synthetic data, never production data, for tests.
5. **Finish:** remove your temporary fixtures and generated storage, record
   durable learnings, and leave services in the requested state. Report what
   changed, exactly what was verified, and any remaining prerequisite or risk.
   Do not claim success from an unrun check or leave actionable work unfinished.

Independent substantial work may run in parallel, with explicit file ownership
and shared contracts. Keep small or tightly coupled changes inline. Run shared
formatting and validation after integration, not concurrently with edits.

### Choose the supported workflow

Prefer `./tree`: it owns native configuration, operation locking, disposable
verification and cleanup. If a workflow is missing, inspect `backend/internal/app/workflow/`
and fix the controller rather than institutionalizing a manual workaround.

| Task                                                                 | Start here                                                                                                               |
| -------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| Daily startup or full shutdown                                       | [README daily workflow](README.md#daily-mac-start-and-stop); `dev up` ensures prerequisites; `dev down` fully shuts down |
| First-time setup, credentials, editable development, stable releases | [README setup and releases](README.md#setup-and-releases); setup is not a daily startup command                          |
| Native schema and recovery boundaries                                | `./tree snapshot list`; use cold checkpoints and forward migrations                                                      |
| Full application validation                                          | `./tree check`; uses disposable PostgreSQL and removes generated build output afterward                                  |
| Native build cleanup                                                 | `./tree clean` while stopped; see [low-disk policy](README.md#low-disk-development-policy)                               |
| Frontend composition and styling                                     | Read [StyleX authoring](docs/stylex-authoring.md) and follow the design-system rules below                               |

Do not boot Colima, start the tunnel, or build application images for a
documentation-only change. Validate documentation formatting and local links;
the lightweight quality gates below can run without the runtime. For code
changes, choose focused behavioral verification and use `./tree check` for the
full application gate. If a prerequisite prevents verification, state which
check could not run and why; do not substitute a production environment.

## Record learnings and maintain this guide

Recording reusable learning is part of completing a task, not an optional
follow-up. Before finishing, ask: **what did this work establish that the next
agent should not have to rediscover?**

- Record verified, durable facts: a non-obvious invariant, a repeatable failure
  and its cause, a safe operational procedure, a tool/environment constraint, or
  an explicit user preference. Distinguish observed facts from hypotheses; do
  not promote guesses or one-off symptoms into rules.
- Put cross-cutting user rules and workflow constraints in **this file**.
  Put detailed procedures and troubleshooting in the relevant **existing
  runbook**, linked from here. Put implementation-local invariants near the
  code. Keep one authoritative explanation rather than copies that can drift.
- Make each learning actionable: state **when it applies, what to do or avoid,
  why, and how to verify it**, with a concrete command or source path where
  useful. Generalize the lesson; do not append a transcript of the investigation.
- Update or replace outdated guidance instead of appending contradictory notes.
  Preserve deliberate user decisions; a new observation is not permission to
  weaken a rule. Flag a genuine conflict for the user to resolve.
- Do not record secrets, tokens, private data, temporary paths, current process
  state, dated test counts, disk measurements, or release IDs as permanent
  guidance. Machine-specific setup belongs in the appropriate runbook, clearly
  scoped to that machine, not presented as a universal prerequisite.
- Do not create per-session learning logs or a new documentation file by default.
  If nothing new is durable, do not manufacture an entry. In the final response,
  mention meaningful guidance updates alongside implementation and verification.

## Project at a glance

- Go follows the standard module layout under `backend/`: executables live in
  `backend/cmd/`, private implementation packages live in `backend/internal/`,
  and there is no `src/` or `pkg/` tree because the application exposes no public
  Go library. SQL-free database contracts live in `backend/internal/domain/database/`;
  native adapters and embedded migrations live in `backend/internal/infrastructure/rdbms/`.
- Frontend: React 19 + React Router under `frontend/`, built with Vite. Go serves its assets and API on one port; browser writes preserve their original page runtime session.
- Document parsing is the pure Python boundary under `parser/`; stable artifacts contain only its explicitly inventoried parser sources. Application API, storage, workers and migrations are implemented in Go.
- The design system is under `frontend/src/styles/`, assembled by `entry.css`. Navigation chrome is wired by `frontend/src/shell/navChrome.ts`.
- Native storage is PostgreSQL 18 plus the active dataset's local content-addressed objects directory. Provider keys live in the active dataset's private settings.
- The local catalog mirror is optional and configured by the Settings
  `download_base_path`; a course check refreshes upstream `eclass` files and
  current external-library objects, while stable browser uploads refresh the
  `external` subtree immediately. There is no standalone `./tree mirror`
  workflow.

## Architecture

Relational persistence must expose SQL-free operation contracts to application
and domain code. Select the PostgreSQL or SQLite implementation at composition
time from deployment configuration. Each adapter owns directly written,
backend-native SQL and maps results and errors to backend-neutral Go types.
Do not translate PostgreSQL SQL into SQLite SQL at runtime or emulate PostgreSQL
syntax/functions to make application SQL run on SQLite. The contract must define
observable results, ordering, null handling, errors, snapshot consistency and
atomic cross-feature writes; verify those guarantees against both disposable
backends. Raw SQL handles and driver types stay inside infrastructure adapters.

The runtime owner lock must outlive all admitted transactions and iterators:
`Store.Close` rejects new work and waits for consumers to finish before releasing
ownership. SQLite file aliases use the canonical database path for that lock.
Read-only repeatable snapshots use visibility checks without PostgreSQL row locks;
writer/publication guards retain their locks. Verify the common contract with
`TREE_NATIVE_TESTS=1 go -C backend test ./internal/infrastructure/rdbms`.

```text
./tree                  Installed Go lifecycle controller
├── PostgreSQL 18       One active native relational database
├── Objects directory   One active content-addressed object store
├── tree-eclass         Go API, MCP, queues and background projections
│   └── helpers         Short-lived Python, OCR, PDF and Discord processes
└── Browser assets      React pages and routing, served by the same Go process
```

Use [Daily Mac start and stop](README.md#daily-mac-start-and-stop).
`./tree up` selects stable code; `./tree dev up` takes or resumes the protected
development baseline. `./tree down` stops all writers/helpers/storage and restores
that baseline, discarding all development data. No login service or container
engine is involved.

The launcher builds a controller only for `setup` and `controller update`, using
an isolated disposable cache. Shutdown, status and recovery always use the installed
controller even if editable source fails to compile. Replacing that controller or
running setup requires stopped storage, no baseline and no pending activation.
Keep its stable executable path available to process supervisors and the watcher.
Verify launcher behavior with `TestLauncherRecoveryDoesNotCompileEditableCode`.

The `laptop` remote is a private local bare Git repository. Only sanitized,
public-safe history is published. Build stable releases from clean committed
revisions; do not create commits automatically.

Navigation requests read generation-checked `read_model` projections and never
rebuild evidence synchronously. Projection workers run inside the Go application;
external synchronization, inference and webhook delivery remain disabled in
development. Source/configuration changes invalidate projections while learner
progress updates independently of immutable guidance.

## Code quality / engineering rules

1. **Keep hand-written source files below 400 lines and physical source lines below 120 characters.** This applies to Go, Python, JavaScript, JSX, TypeScript, TSX, CSS, SQL and shell sources, including tests. Generated code, vendored tools, lockfiles, build artifacts, configuration data and immutable migration bytes are excluded. The shared gate is `python3 scripts/quality/audit_limits.py`; there is no grandfather clause for hand-written source.
2. **Keep functions below 50 lines.** Python functions are checked by `scripts/quality/audit_limits.py`, Go declarations and literals by its Go AST checker, and JS/JSX/TS/TSX functions by Oxlint's `max-lines-per-function` (see `frontend/oxlint.config.ts`). Long, flat data-declaration or mapping tables do not count against function bodies.
3. **Format every source language with its native formatter.** Oxfmt (config: `.oxfmtrc.json`, 120-character print width) covers JS/JSX/TS/TSX, JSON, CSS, Markdown, and YAML; Ruff (`ruff format`, config: `ruff.toml`) covers Python; `gofmt` covers Go. Run the repository's quality gate before finishing.
4. **Run the gates before finishing:** `python3 scripts/quality/audit_limits.py` (repo root), `bun run lint` (from `frontend/`), `frontend/node_modules/.bin/oxfmt --check .` (repo root), `ruff check . && ruff format --check .` (repo root), and `go -C backend test ./cmd/... ./internal/...`. The limit gate also verifies Go formatting and runs in CI before the native tests.
5. **Low disk usage is a development requirement.** Native builds and checks use
   owned disposable caches and remove them on failure as well as success. Preserve
   release/checkpoint references, active data, keys and unrelated work. Do not
   retain multi-gigabyte ad-hoc build caches for faster reruns. `./tree clean`
   manages native output while stopped; it does not clean container engines,
   and cleanup or verification must never boot one.

## Testing

- Keep permanent tests for observable behavior, boundaries, invariants, and
  plausible regressions—not to prove that an edit has tests. Avoid tests that
  merely pin wording, internal wiring, or incidental defaults. A one-off
  experiment can use a throwaway smoke check; remove its fixtures afterward.
- Native tests live beside the Go packages under `backend/internal/` and
  `backend/cmd/`; run them with `go -C backend test ./cmd/... ./internal/...` and
  `go -C backend test -race ./cmd/... ./internal/...`. Native integration tests
  opt in with `TREE_NATIVE_TESTS=1` and use private synthetic PostgreSQL and
  objects-store fixtures. The parser fixture is short-lived and isolated; it never opens
  application storage or credentials.
- Tests must stay offline and use disposable synthetic data. Never point native checks at the authoritative dataset. Browser behavior is checked by the native browser fixture and frontend tests.

## Security / trust boundary

- The service has **no authentication or authorization** — no session, login, or API key on any `/api/*` or `/files/*` route. This is a deliberate single-user, trusted-network design; do not expose the service beyond a private network without adding auth.
- Go is the single HTTP entry and serves browser assets, JSON, SSE, files and MCP. Bind it to loopback; keep the same private-access boundary in optional containers.
- `/files/{path}` is scoped by the native Go file handler; the id-based `/api/study/document/{id}/content` route is the preferred surface for the reader.
- The MCP server is the hardened surface: read-only tool annotations, opaque IDs instead of paths, host/origin allowlist, DNS-rebinding protection.
- CORS intentionally allows any origin without credentials; the SPA is served same-origin.

## Refactoring conventions

- Keep native Go domains in `backend/internal/domain/`, application services in
  `backend/internal/services/`, external integrations in
  `backend/internal/integrations/`, runtime adapters in
  `backend/internal/infrastructure/`, and HTTP/lifecycle entrypoints under
  `backend/internal/app/`. Preserve the explicit parser boundary
  under `parser/`. Domain owns its ports and generated query models under
  `backend/internal/domain/` (`queries`, `objects`, `commands`, `extract`,
  `inference`, `scheduler`, `platform`); outer layers implement those ports and
  must never be imported by domain code. Enforced by
  `backend/.go-arch-lint.yml` via `go tool go-arch-lint check` in `backend/`
  (also gated in CI).
- **Compliance gate**: `python3 scripts/quality/audit_limits.py` (repo root) — exit 0 = compliant.

## Known deliberate removals

- Standalone knowledge search (the `/knowledge` page) was removed with the React migration; `/api/knowledge/search` still exists server-side but has no frontend entry point. Reintroduce it deliberately, not by accident.
- The obsolete standalone session entry was deleted with the SSR migration: it mounted into a `#session-root` that never existed in the server shell, so it was dead code — the session workspace now mounts through React Router and lazy-loads the PDF reader.

## Frontend design system

- Use `@astryxdesign/core` primitives for buttons, cards, layout, overlays, badges,
  banners, and dividers when their contracts match the surface.
- Use StyleX for application-owned composition and visual variants. StyleX is
  compiled by Vite's Babel transform and the matching PostCSS plugin. Keep their
  options aligned and register shared StyleX definitions as transform watch dependencies.
- When creating or modifying StyleX styles or StyleX-styled components, MUST read
  `docs/stylex-authoring.md` first and follow its authoring rules.
- Prefer StyleX tokens from `frontend/src/styles/tokens.stylex.ts`; do not add
  global CSS for application-owned styling.
- Keep application-owned CSS only for browser contracts that cannot be expressed
  as a component or StyleX rule, including the global font/theme/scrollbar/
  selection rules in `frontend/src/styles/entry.css` and the PDF text layer in
  `frontend/src/features/session/reader/*.css`.
- Use semantic HTML for content and form structure. Use Astryx
  layout primitives where they replace an application-owned layout wrapper.
