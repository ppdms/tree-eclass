# Offline UI review

The Storybook catalog renders the application’s real React Router routes, loaders,
layout, StyleX themes, fonts, Astryx primitives, forms, PDF reader and Ask transport.
Its fixtures are **authored synthetic data**, not retained production captures.
The Storybook brand, scenario names, fixture IDs and run manifests identify them
as synthetic. Simulated success is not evidence that a backend operation occurred.

No Go application, database, objects store, credentials, native helper, container
engine or external service is required. Do not start the authoritative application
or import live data to populate this catalog.

## Prerequisites

Use the repository’s Bun package manager:

```sh
cd frontend
bun install --frozen-lockfile
bun x playwright install chromium
```

Playwright’s Chromium needs its normal OS libraries. On a system that provides
Chromium separately, set `UI_REVIEW_BROWSER` to that executable instead of
installing another browser. For example, an explicitly provisioned Nix Chromium
can be used on Linux. The manifest records the browser version actually used;
results from different browser versions are not pixel-equivalent baselines.

On a Nix-managed Linux workstation, an isolated tool shell can provide both:

```sh
nix shell nixpkgs#bun nixpkgs#chromium
export UI_REVIEW_BROWSER="$(command -v chromium)"
```

Dependencies are installed during setup, not during capture. Catalog browsing and
capture themselves use only loopback HTTP and committed synthetic fixtures.
Storybook telemetry is disabled.

## Commands

```sh
bun run ui:preview                 # interactive, source-updating catalog on 127.0.0.1:6006
bun run ui:build                   # explicit static catalog build
bun run ui:capture --list          # IDs and names from the built Storybook index
bun run ui:capture --only ask-- --only session-- --out reading-review
bun run ui:capture --out complete-review
bun run ui:typecheck
```

`ui:preview` accepts Storybook dev flags, including `--port`. Select a workflow
and scenario in the sidebar. Scripted interactions run in the Interactions panel;
use **Reload story** to reset the scenario, then explore the real controls yourself.
The preview retains the application’s production styling. It does not substitute
storybook-specific copies of pages or business logic.

The primary review viewport is **1440 × 1000 CSS pixels**, device scale factor 1.
Capture uses `en-GB`, `Europe/Athens`, dark OS color preference and the fixed clock
`2026-10-13T10:00:00Z`; explicit scenario themes can select light or e-ink.
Application timers still run. Screenshots show the current viewport at each named
checkpoint, including dialogs, focus state and the relevant scrolled section.

### Static build and freshness

The static site is `frontend/.ui-review/catalog/`. It contains Storybook’s own
`index.json`, a filtered machine-readable `ui-review-index.json`, and
`ui-review-build.json` with SHA-256 source inventory, artifact digest, tool versions,
build time and fixture provenance. Application source, public fonts/assets,
Storybook configuration, fixtures, review tooling, package/lock/configuration files
and the shared performance budget participate in freshness.

Capture rejects a missing, stale, altered or incomplete catalog with an actionable
`ui:build` error. It **never rebuilds automatically**. A source change during build
also rejects the build. A build or capture holds `.ui-review/catalog.lock` so a
running capture cannot have its static assets replaced. If a process was forcibly
killed, inspect that no build/capture remains before removing its stale lock.

### Selection and output

`--only <ID-prefix>` can be repeated. Overlapping selections are deduplicated in
catalog order; every selected scenario runs once in its own browser context.
An unmatched prefix fails rather than silently selecting nothing. `--list` does
not launch a browser or create a run.

`--out <name>` creates `frontend/.ui-review/runs/<name>/`. Existing names are rejected;
runs are never overwritten. Omitting it creates a timestamped name. Names must be
simple path-free names of up to 80 characters.

Each run retains:

- `index.html`: a local gallery grouped by scenario and screenshot checkpoint.
- `manifest.json`: selection, viewport/environment, fixture/build provenance,
  per-scenario outcomes, errors and artifact paths.
- `<scenario-id>/<checkpoint>.png`: explicitly named screenshots.
- `<scenario-id>/failure.png`: additional screenshot when the scenario fails.
- `<scenario-id>/trace.zip`: Playwright trace, including snapshots and source evidence.
- `<scenario-id>/browser.json`: browser/console, HTTP and network diagnostics.
- `<scenario-id>/downloads/`: actual browser downloads, hashed and content-verified.

Open the gallery directly in a browser. Traces can be inspected locally with
`bun x playwright show-trace <trace.zip>`; do not upload private future captures to
an external trace viewer.

A manifest marked `completed` means all selected scenarios finished and browser/server
cleanup succeeded, **not** that the scenarios passed: inspect each scenario’s status.
Browser startup failure, cleanup failure or interruption leaves `incomplete`,
retaining pending/running/failed state and the reason. Exit codes are `0` for all passed,
`1` for completed runs with failed scenarios,
`2` for incomplete startup/infrastructure failure, and `130` for interruption.
Browsers, contexts, temporary loopback servers and locks are closed on normal
completion, failures, SIGINT and SIGTERM.

The capture controller owns signal handling (including SIGHUP). It retains the
available failure view and stops tracing before closing contexts and Chromium;
browser-library signal handlers must not race that evidence publication.
Server cleanup is attempted even if browser cleanup rejects; a cleanup error
must never produce a successful exit or a `completed` manifest.

Capture concurrency and scenario deadlines use `frontend/performance-budgets.json`.
Only two browser contexts run concurrently by default. Catalog/run output is ignored
by Git and formatting/lint discovery. Keep useful review evidence; remove unwanted
named runs explicitly to reclaim space. `./tree clean` does not manage these runs.

## Scenario and fixture maintenance

Stories live in `frontend/review/stories/`; fixtures in `frontend/review/fixtures/`.
The built Storybook CSF index is the only scenario registry. Do not add a second
list to the capture runner.

Keep meta `id` and exported story names stable: changing either changes the public
scenario ID. Organize names around workflows and meaningful states, not implementation
components. Existing workflows cover loading, empty results, validation, unavailable
dependencies, intentional failures, recovery and ordinary interactions where the
production UI exposes them.

```ts
import { expect, userEvent, within } from 'storybook/test';
import { checkpoint, reviewed } from '../scenario';
import { existingFixture } from '../fixtures/existing';

export default { title: 'Workflows/Existing', id: 'existing', tags: ['ui-review'] };

export const MeaningfulAction = {
  name: 'Synthetic meaningful action',
  ...reviewed(
    { path: '/existing', fixture: existingFixture(), checkpoints: ['initial', 'changed'] },
    async ({ canvasElement }) => {
      const canvas = within(canvasElement);
      await expect(await canvas.findByRole('heading', { name: 'Existing workflow' })).toBeVisible();
      await checkpoint('initial');
      await userEvent.click(canvas.getByRole('button', { name: 'Existing action' }));
      await expect(await canvas.findByRole('status')).toHaveTextContent('Actual visible result');
      await checkpoint('changed');
    },
  ),
};
```

The example describes the authoring contract; use real routes, accessible names and
existing product outcomes. Every declared checkpoint must be reached exactly once.
Checkpoint calls wait for fonts and, during capture, await the screenshot before
continuing. Use visible state assertions and `waitFor` for rerenders, never fixed
assertion sleeps. Body-portaled dialogs are queried through `within(document.body)`.

Fixtures return an ID and a fresh `handlers()` array. MSW handles requests at the
existing HTTP boundary; it does not replace application hooks, calculations, SDK
transports or modules. Keep closure state private to each scenario. Deferred fixture
responses let interactions capture loading, then release the response explicitly.
Unregistered API/file requests and external requests are blocked. Capture also blocks
unexpected external network/WebSocket connections and fails on unexpected browser,
console, HTTP or network errors.

Intentional HTTP failures declare exact method, path, status and count in
`expectedHttpErrors`. This is not a blanket console-error allowlist: unrelated errors
remain failures, and an expected failure that never occurs also fails the scenario.

An SDK-consumed streaming error can intentionally cancel its HTTP 200 SSE body.
That case declares `expectedNetworkErrors`, binding the precise request ordinal,
response status/content type/SDK header, cancellation text and failed/recovered
checkpoints. Capture correlates request identity and requires the next response
to finish successfully. It never ignores canceled requests in general.

Download scenarios click the actual application download link and declare the
checkpoint, filename, content type and meaningful expected content in `downloads`.
The learner-export bytes are authored once in `settings-export.ts` and generated as
a read-only HTTP fixture at the original API URL. This avoids service-worker stream
limitations without changing the application’s download behavior. JSON/PDF content
verification records the verified content format, not an inferred real backend result.

Use production TypeScript payload contracts and plausible linked IDs, source paths,
hashes, document revisions and learner state. Fixtures must contain no credentials,
webhook destinations, personal data or copied private course material. Any future
retained read-only capture needs explicit authorization, sanitization, provenance and
clear separation from simulated outcomes; the current catalog contains none.
After changing a fixture contract, review its real consumer, typecheck, rebuild and
capture the affected prefix.

## Evidence limits

Offline UI evidence verifies rendering, browser-side interactions, validation,
transport-contract handling and the declared visible outcomes. It does **not** prove:

- Authentication, authorization or runtime admission in a real deployment.
- Real backend persistence, transactions, migration compatibility or object storage.
- Upstream eClass/Discord, inference providers, webhooks or native helpers.
- Deployment-image parity, production packaging, networking or real-service failures.

Continue using disposable native fixtures and `./tree check` for those boundaries.
Never use the authoritative dataset as a substitute for missing UI-review fixtures.
