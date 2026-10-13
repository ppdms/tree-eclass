# Canvas Authoring Guide

This is the authoritative guide for the production browser application under
`frontend/src/canvas/`. React runs in a dedicated module worker through a custom
mutation renderer; Taffy WASM supplies layout. Canvas2D draws content and text onto
a single opaque visible canvas. WebGL2 composites Liquid Glass navigation onto a
transparent offscreen canvas; Canvas2D draws that completed layer onto the visible
output at the end of the same worker frame. Browser FontFace loads the primary
WOFF2 faces; glyph fallback is browser/system-owned. The browser owns the narrow
native-input and browser-service boundary. There is one application entry and
one authoring convention, not a separate demo or DOM UI.

The user has waived accessibility requirements for this single-user canvas
application. Correct native editing, trusted activation, routing, source identity
and runtime safety remain required.

## Source and build boundaries

Paths below are relative to the repository root; abbreviated `app/`, `ui/` and
`session/` paths within the table are under `frontend/src/canvas/`.

| Surface                                     | Source                                                                                           |
| ------------------------------------------- | ------------------------------------------------------------------------------------------------ |
| Go-injected page and browser boot           | `frontend/index.html`, `frontend/src/canvas/app/startup.ts`, `app/main.ts`                       |
| Render worker and asset initialization      | `frontend/src/canvas/app/worker.ts`, `app/workerApp.ts`, `app/workerAssets.ts`                   |
| Routes, loaders and application shell       | `frontend/src/canvas/app/router.tsx`, `app/loaders.ts`, `app/shell.tsx`                          |
| Browser services and message contracts      | `frontend/src/canvas/app/browser*.ts`, `app/protocol.ts`, `app/browserProtocol.ts`               |
| React host tree, layout and text            | `frontend/src/canvas/ui/react.ts`, `ui/tree.ts`, `ui/styles.ts`, `ui/text.ts`, `ui/paragraph.ts` |
| Palette, primitives and reusable controls   | `frontend/src/canvas/ui/palette.ts`, `ui/primitives.ts`, `ui/controls.tsx`, `ui/forms.tsx`       |
| Interaction, geometry and drawing           | `frontend/src/canvas/ui/interaction*.ts`, `ui/geometry.ts`, `ui/paint.ts`, `ui/runtime.ts`       |
| Same-frame content/navigation composition   | `frontend/src/canvas/renderer.ts`, `chrome.ts`, `glass.ts`, `glassShaders.ts`                    |
| Learning-session workspace and reader       | `frontend/src/canvas/session/`, `session/reader/`, `frontend/src/canvas/pdf.ts`, `transfer.ts`   |
| Primary WOFF2 fonts and regeneration inputs | `frontend/src/assets/`, `frontend/src/canvas/ui/fonts.ts`                                        |
| Canonical build and notices                 | `frontend/package.json`, `frontend/vite.config.ts`, `frontend/tools/releaseNotices.ts`           |

`frontend/package.json` defines `build` as `tsc --noEmit && vite build` and `dev`
as `vite build --watch`. The watcher opens no HTTP port. Use `./tree dev up` for
the supported native development lifecycle; Go serves assets and API on one
origin. Stable use starts no Bun, Node or frontend server. A static preview is
not a substitute for Go's native runtime, storage and API behavior.

Keep all four HTML substitutions: `__TREE_RUNTIME__`, `__TREE_STORAGE__`,
`__TREE_MODE__` and `__TREE_BUILD__`. Preserve the startup status/error boundary;
unavailable worker Canvas2D, WebGL2, Taffy WASM or primary FontFace loading must
not masquerade as readiness. Use shipped APIs in Chromium, WebKit/Safari and
Firefox; experimental browser flags are not an acceptance prerequisite.
Vite publishes hashed assets before atomically renaming `.index.tmp` to
`index.html`, retains previous assets across rebuilds and leaves the old entry
intact on build failure. Do not empty the active output during a rebuild.

Final hashed assets, including workers and notices, receive deterministic gzip
sidecars only when smaller. Publish those before the entry too; original names
and bytes stay unchanged. Go negotiates them at the original URL with the
original MIME, exact length and `Vary: Accept-Encoding`, preserving HEAD and
conditional responses. Explicit gzip refusal overrides a wildcard, and any
Range request uses the original bytes. Sidecars share the same rooted regular-file
boundary; missing or unsafe sidecars fall back to the original. Dynamic HTML
and runtime substitutions are never precompressed. No request-time compression
or JavaScript serving process is needed.

Keep ES module workers and `preserveEntrySignatures: 'strict'` in the worker build.
Lazy routes import shared state from ordinary modules, never `app/worker.ts`:
importing a module-worker entry can execute its bootstrap again in WebKit.
The worker resolution condition avoids browser-only Markdown initialization;
Taffy's package-relative WASM URL remains excluded from dependency optimization.

`app/worker.ts` queues application messages until `app/workerApp.ts` admits the
captured page state and connects the runtime. The router and native renderer load
with the entry; the heavier UI runtime loads concurrently with font/layout
initialization and route I/O. Lazy routes remain separate. Start route I/O before
synchronous native context creation, and report runtime-module failures through
the same early error boundary as asset failures.
State and input ordering still belong to that one worker. `app/startup.ts` must receive worker
errors before the browser-host module finishes loading, then hand message
ownership to `app/main.ts`. Otherwise an early worker, module or font failure
can leave a permanently blank loading page with no visible error.

Startup scalar/error guards use primitive checks, keeping the schema library
outside the initial worker graph. Full feature schemas remain in lazy routes.
Preserve finite-number rejection, object-versus-array distinctions, error-field
precedence and the original response body when detecting runtime conflicts.
Native JSON decoding supplies the `JsonValue` domain; use named type guards for
structural checks rather than loading a general schema parser into startup.

## Author components against the host vocabulary

`ui/tree.ts` accepts exactly five host kinds: `view`, `text`, `icon`, `vector` and
`raster`. Import their wrappers (`View`, `Text`, `Icon`, `Vector`, `Raster`) from
`ui/primitives.ts`. `TextField` is a text host backed by a real native control,
not a sixth intrinsic element. Raw strings/numbers must be inside `Text`; the
shared `Button` and similar controls normalize their textual children for you.
Do not introduce DOM JSX, CSS classes, browser layout wrappers or another
component/styling framework into the worker tree.

Reuse `Button`, `Panel` and `Notice` from `ui/controls.tsx`; `Field`, `Select`,
`Range`, `Checkbox` and `FilePicker` from `ui/forms.tsx`; the shared overlays;
and `AppLink` from `app/links.tsx`. Use meaningful stable IDs for interactive
controls, fields, scrolling regions and layout targets. React keys and native
field instance identities prevent a new screen from inheriting an old draft.

For example, a controlled editor uses the same host and palette as the app:

```tsx
import { Text, TextField, View } from '@/canvas/ui/primitives';
import { colors } from '@/canvas/ui/palette';

export function QueryEditor({ value, onChange }: { value: string; onChange: (value: string) => void }) {
  return (
    <View style={{ gap: 8, minWidth: 0 }}>
      <Text style={{ color: colors.muted }}>Question</Text>
      <TextField id="question-draft" value={value} onChange={onChange} multiline rows={3} />
    </View>
  );
}
```

### Styles and layout

`HostProps` and `UIStyle` in `ui/types.ts` are the supported contract, not a CSS
compatibility layer. Styles are ordinary typed objects, merged explicitly with
object spread. Read `ui/styles.ts` and the relevant painter before adding a new
property; extend layout, painting, clipping and input geometry together.

- Layout supports flex/grid/none, row/column, wrapping, growth/shrink, alignment,
  gaps, bounded dimensions, margins/padding, grid columns/spans and
  relative/absolute positioning. Default layout is a flex column. Lengths are
  numbers (CSS pixels), `rem` (fixed at 16 pixels), percentages or `auto` where
  the type permits them. Grid tracks are numbers, `fr` or `auto`.
- Painting supports the declared borders, radius, background/color, opacity,
  text typography and truncation, vector/raster content, hover colors and glass.
  `overflow: 'hidden'` clips; scroll ownership is a separate `scroll` host prop,
  not CSS overflow. `lineHeight` is a unitless font-size multiplier.
- Inherited text defaults are color, font size/weight, line height and mono mode.
  `mobile` applies at viewport widths at most 640; `desktop` applies at least 1024. These are resolved by the host, not arbitrary CSS media queries.
- Use `ui/palette.ts` for shared colors and theme resolution, including light,
  dark, system and eink preferences. Do not encode a second theme mechanism.
- Fixed roots use the `fixed` host prop; modal scopes remain above nonmodal roots.
  Reuse overlay/focus-scope behavior rather than inventing local z-order, focus
  trapping or outside-click conventions.
- Programmatic and scope-entry focus follows browser `:focus-visible` modality:
  the ring appears only after keyboard input, never after pointer use. A focus
  scope's own scrim keeps pointer focus and its Escape handler but is not a Tab stop.
- `ready` declares substantive content readiness and `pending` blocks readiness
  when that node is visible. Loading placeholders cannot be marked ready merely
  to improve a reported startup time. A completed, interactive route error is a
  substantive viewport: mark its error root ready while keeping the real error,
  retry and navigation controls. Otherwise the global startup banner falsely
  remains visible above an already rendered failure. Verify failures on new
  document navigations: entering the same error after a successful page can hide
  this defect because first-content readiness has already been recorded.

### Liquid Glass

Glass belongs to the navigation layer only, following Apple's Materials guidance:
`glass: 'bar'` for floating bars and `glass: 'overlay'` for menus, popovers and
dialogs. Cards, panels, rows, fields and content buttons use solid palette fills.
`scrollEdge: true` on a fixed bar softens content scrolling under it.

- Controls on glass use the translucent `glassFill`, `glassHover` and `glassEdge`
  tokens. `Dialog` and `Popover` provide `OnGlass` (`ui/controls.tsx`); shared
  `Button` reads it, and feature-local controls rendered in overlays must too.
- Overlay glass with `morphFrom` emerges from that control with a spring owned by
  `ui/emerge.ts`; React mounts only the final surface. Reduced motion and e-ink
  show it immediately. E-ink draws no glass: surface and border tokens are the
  solid fallback.
- Material values live in `theme.ts` and `chrome.ts`, optics in `glass.ts` and
  the shader in `glassShaders.ts`. Tune them against Apple reference captures
  rendered over the same backdrop, in dark and light themes and in all three engines.
- The current painter applies glass in fixed navigation layers. Non-fixed glass
  and glass inside an opacity layer use solid fallback; do not rely on them for
  a frosted content surface or bypass the navigation-only material rule.

Icons use the bounded `IconName` inventory and data-only Lucide paths in
`ui/icons.ts`. Additional vector artwork belongs in `Vector`, not an SVG DOM
subtree. Diff-tree connectors use vector geometry rather than box-drawing text,
whose fallback glyphs and metrics vary across systems.

## Lifecycle and resource ownership

React owns structure and committed application state. The renderer owns scroll,
hover, press motion, frame scheduling, geometry and painting; ordinary scrolling
must not rebuild the React tree or recreate unchanged paragraphs.

`ui/react.ts` uses a concurrent mutation reconciler. React can abandon
render-phase instances, so `UITree.create()` and raw text creation own no WASM
resources. Allocate layout nodes and prepare cached text/vector data only for
the committed tree. Native controlled edits use the synchronous committed-input path;
do not acknowledge a render that has not committed.

- `taffyStyle()` creates a temporary Style copied into the tree; callers free it
  immediately. `getLayout()` results are also owned wrappers and must be freed.
  The pinned measure ABI supplies known dimensions, available space, bigint node
  ID and context, with no unused Style wrapper to filter or retain in JavaScript.
- Removed nodes become unshown, clear cached paragraph/vector references and
  release Taffy nodes. Hidden/removed targets must not retain actionable focus.
- `ui/fonts.ts` owns persistent Canvas2D measurement caches. `ui/text.ts` owns
  cached `Paragraph` layout and width/height measurements; content/typography
  changes rebuild them, while scrolling and recoloring reuse them. These are
  browser/JavaScript resources, not native paragraph handles requiring deletion.
  Prepare word-break units only when min-content sizing or wrapping needs them;
  a fitting hard line uses its complete shaped advance directly. Preserve those
  units across width changes. The text smoke's `widthTransitions` case checks
  fitting → wrapped → fitting layout, source ranges and glyph fidelity.
- `ui/resources.ts` owns published `ImageBitmap`s. `publishImage()` consumes the
  bitmap, closes any replaced image and closes rejected input before initialization;
  owners call `releaseImage()` on eviction/unmount. Close stale or aborted decoded
  bitmaps that never reach publication. Do not double-own a bitmap.
- `renderer.ts` sizes the opaque visible Canvas2D output and transparent offscreen
  WebGL2 navigation surface together. Draw completed content first, then record
  chrome segments and glass/fill/scroll-edge operations, composite those ordered
  operations offscreen, and draw the completed navigation layer onto the visible
  Canvas2D output at the end of that worker frame. A glass backdrop includes current
  content and earlier navigation layers beneath it, never a previous-frame snapshot.
  Preserve RGBA textures, premultiplied-alpha blending and bounded regional backdrop
  uploads; crop through the bounded Canvas2D staging surfaces before uploading.
  WebGL2 DOM-source subrect dimensions and unpack offsets constrain texture contents,
  not the driver's source-processing cost: direct uploads preserved pixels but
  regressed native Firefox first-frame composition and warm navigation in matched
  runs. Do not remove staging based on the apparent reduction in API calls;
  repeat native foreground timing as well as the three-engine pixel comparison.
- Size offscreen glass output in bounded CSS-pixel blocks covering every drawn
  quad and shadow. Backdrop sampling still covers its full content region, which
  can extend below that smaller output. `gl.ts` pads those nested-navigation
  samples with transparency and preserves bottom-first framebuffer coordinates;
  never read outside the output or clip the content sampling margin. Keep initial
  allocation and subsequent resizing on the same capacity rule.
- Flush the initialization batch after submitting shader programs. Firefox otherwise
  retains those commands until the first frame's blocking link-status query; merely
  calling `linkProgram()` does not establish background compilation. Use native
  profiler IPC markers to distinguish submission, driver waits and thread CPU.
  Keep link-status/uniform resolution deferred: resolving every program immediately
  after the flush makes construction wait for compilation instead of overlapping it.
- Keep a single visible output. Separate transferred content and navigation canvases
  can present different frames even when drawn in the same worker task; worker
  scheduling alone is not optical synchronization. Verify the combined output in
  all three engines rather than inferring frame coherence from submission markers.

The root renderer, loaded FontFace sets and shared PDF parser are worker-lifetime
resources; the current root classes do not expose a general app-wide dispose
API. Route cleanup is not proof that the entire worker/GPU runtime was destroyed.
Do not add repeated in-worker application bootstraps without explicit ownership.

### Reader and learning-session ownership

The canonical session route is `/study/session`. `session/workspace.tsx` owns
workspace composition; the reader owns its source identity, requested pages,
selection and image resources. Preserve source-bound marks, attention intervals,
finish retry intent and persisted journals; these are learning writes, not merely
visual state.

`session/reader/source.ts` requires the document's content URL and source hash,
and sends `If-Match: "sha256-…"` for source reads. Unsupported, changed, missing
and failed sources need truthful terminal UI, not fake successful reader content.
The ready reader viewport requires rendered requested-page resources, not just
PDF metadata or a loading task resolving.

After a scale/layout change, `session/reader/navigation.ts` rejects scroll
publications until passive anchor reconciliation finishes. The renderer can
publish a painted new-layout frame with the old offset before that effect runs;
using that sample would replace the prior-layout anchor and reset the page.
Verify a settled page-two jump through zoom in/out and fit page/width, including
both the current-page control and the visible page pixels.

`session/reader/cache.ts` keeps resources for requested pages, caps active page
work at two and bounds raster dimensions/pixels without changing CSS geometry.
Eviction aborts work but retains the scheduler slot until all public render/parser
promises settle. Unmount/retry aborts and destroys the cache and its source.
`PdfWorkLifetime` and `pdf.ts` retain the PDF parser termination barrier: early
render rejection or unresolved operator/font work does not acknowledge teardown,
and failed termination must not admit replacement parser work.

pdf.js rasterizes OffscreenCanvas pages inside the render worker and parses in
its own shared nested worker. Preserve the custom CanvasFactory/FilterFactory
and the narrow `ownerDocument` exposing only worker fonts. `transfer.ts` emulates
`/TR` transfer-function canvas filters; pdf.js retains its own soft-mask/knockout
fallbacks. Changes need synthetic transfer-PDF pixel checks in Chromium, WebKit
and Firefox as well as reader cancellation/replacement checks.

Reader text geometry uses the original PDF operator advances and font metrics,
not replacement-font measurements. Preserve source-glyph ownership through the
pinned pdf.js normalization and bidi permutation at JavaScript UTF-16 offsets.
One source glyph can own several non-contiguous logical units, and whole-chunk
normalization can give one logical interval several source glyphs. Selection
rectangles retain those source extents; caret boundaries must bound their union.
ToUnicode extraction is independent of painted glyph outlines, so an extraction
or font-substitution limitation is not by itself a selection-geometry defect.

## Shared browser and runtime contracts

Use `app/browser.ts` and the shared browser hosts for native services; the worker
must not grow its own competing DOM, storage, history or activation path.

### Native editing, clipboard and trusted activation

`app/editor.ts`, `browserFields.ts`, `nativeFieldGeometry.ts` and
`browserInput.ts` maintain transparent native inputs/textareas over all exposed
fields before their first trusted pointerdown. Native hit testing, synchronous
focus and default selection/drag behavior remain real; the worker paints text,
selection, caret and composition. The shared protocol carries value, UTF-16
selection, composition, native dimensions, scroll and edit revision/instance.
Per-frame polling covers selection/caret changes with no single cross-engine event.

Preserve all exposed hotspot pieces when clipping against scrollers and modals.
Wheel handling consumes the native control's scroll before forwarding the
remainder to canvas; the non-scrolling DOM body cannot chain it. Preserve drafts
and selections at both ends. `visualViewport` supplies the visible band including
keyboard-induced `offsetTop`, not just a guessed keyboard height.

Trusted browser handlers must synchronously focus fields, activate file pickers,
copy or follow native links as appropriate. Awaiting a worker round trip loses
mobile keyboard/user-activation behavior. Use `browserActivation.ts` for safe
protocols, modified clicks, downloads and external links. Rich assignment HTML
is parsed inertly on the main thread into bounded text runs, not inserted as live
application HTML. `browserClipboard.ts` mirrors canvas selections into a native
selection and handles clipboard writes; do not depend on a secure-context-only
clipboard API for every supported browser surface.

Native control geometry is mirrored from computed typography and native offsets;
worker paragraph offsets and the editing protocol are UTF-16 too. Browsers do not expose caret affinity
at bidi or soft-wrap boundaries, so a DOM Range rectangle is not proof of
pixel-identical native internals. Exercise actual native selection and IME, not
only geometry calculations or scripted event dispatch.

### History, persistence and original-page authority

`app/router.tsx` runs React Router's memory router with native history mirroring
in `app/browserHistory.ts`. Keep browser entry identities, opaque user state,
PUSH/REPLACE/POP, queries, hashes, scroll restoration and anchors intact. Canonical
routes are `/`, `/activity`, `/announcements`, `/timeline`, `/history`, `/courses`,
`/courses/:courseId`, `/courses/:courseId/changes/:changeNo`, `/exercises`, `/study`,
`/study/session`, `/settings` and `/ask`. Do not add an alternate route prefix.

`app/browserStorage.ts` allowlists persisted keys and uses the shared
`frontend/src/lib/browserStorage.ts` namespace captured from the original HTML.
Stable preferences keep their established keys; each disposable development
baseline uses its own namespace. Worker preferences use the host snapshot/bridge,
including storage changes; they must not access raw browser storage or silently
replace stable keys with another application's namespace.

`app/main.ts` captures the original page runtime token for boot.
`app/api.ts` configures the worker fetch boundary once and adds that token to
same-origin mutations as `X-Tree-Runtime`, including streaming/binary requests.
Never replace it with a current cookie, newly fetched token or another runtime's
session. A `runtime_changed` conflict marks the page stale. Development reload
polling in `app/developmentReload.ts` uses the same original token and stops on a
runtime change; it must not reload stale authority into a new mode.

## Fonts, regeneration and shipped notices

The shipping faces are `frontend/src/assets/Inter.woff2` and `RobotoMono.woff2`.
`app/startup.ts` starts native editing's FontFace loads through `app/editorFonts.ts`
before importing the browser host, and sends the same immutable URLs to the worker.
Both realms retain URL-backed faces rather than copying anonymous buffers, so
the browser can reuse native font/glyph caches across document and worker lifetimes.
`data-native-fonts-ready-ms` records the document faces' actual load completion;
the editor and presentation barriers still await it. Both realms load the same bytes.
Keep native and worker typography, variation ranges and fallback families
aligned; identical bytes alone do not prove equal glyph advances, wrapping,
caret/selection geometry or color emoji across engines.

The unshipped regeneration inputs are `frontend/src/assets/InterVariable.woff2`
and `RobotoMono-Variable.ttf`. The shipped subsets retain Inter's `wght` 100–900
and Roboto Mono's `wght` 100–700 axes. Inter's optical size is fixed at 14 in the
font bytes: Firefox's Canvas2D and native controls disagree on automatic optical
sizing, while WebKit ignores the tested FontFace optical-size descriptor.
Do not substitute a CSS/FontFace override for this instancing step. The production build
imports only the subset URLs, not these larger source faces. Regenerate with
FontTools `pyftsubset` and WOFF2/Brotli support, using the existing repository-local
tool environment shown below and an owned temporary workspace:

```sh
mkdir -p "$PWD/.tree-build/canvas-verification/runtime"
export TMPDIR="$PWD/.tree-build/canvas-verification/runtime"
FONT_TMP="$(mktemp -d "$PWD/.tree-build/font-instance.XXXXXX")"
trap 'rm -rf "$FONT_TMP"' EXIT
U='U+0000-017F,U+0192,U+0218-021B,U+0237,U+02BB-02BC,U+02C6-02C7,U+02C9,U+02D8-02DD'
U="$U,U+0300-036F,U+0370-03FF,U+1F00-1FFF,U+2000-206F,U+20A0-20CF,U+2100-214F"
U="$U,U+2190-21FF,U+2200-22FF,U+25A0-25FF,U+FB01-FB02"
FEAT='kern,liga,calt,ccmp,locl,mark,mkmk,rlig,rclt,dist'
.venv/bin/fonttools varLib.instancer frontend/src/assets/InterVariable.woff2 \
  opsz=14 --no-recalc-timestamp --output="$FONT_TMP/InterText.woff2"
.venv/bin/pyftsubset "$FONT_TMP/InterText.woff2" \
  --unicodes="$U" --layout-features="$FEAT" --flavor=woff2 \
  --no-recalc-timestamp --output-file=frontend/src/assets/Inter.woff2
.venv/bin/pyftsubset frontend/src/assets/RobotoMono-Variable.ttf \
  --unicodes="$U,U+2500-257F" --layout-features="$FEAT" --flavor=woff2 \
  --no-recalc-timestamp --output-file=frontend/src/assets/RobotoMono.woff2
```

Keep the respective source OFL notice beside each output as
`Inter.woff2.license.txt` and `RobotoMono.woff2.license.txt`. This is the subset
recipe, not proof of rendered equivalence or acceptance. After regeneration,
compare actual primary text, Greek/Latin coverage, native selections, variable
axes, worker profiles and compressed transport sizes in each supported engine.
There is no separate primary-font regeneration script or pinned Roboto Mono
acquisition recipe; do not invent one or replace source bytes from an unpinned
remote. Install missing tooling only in an owned repository workspace, not
globally or in external scratch paths.

Missing primary glyphs and emoji presentation use browser/system font fallback.
There is no shipped vendor fallback catalog or fallback-font download protocol.
Do not claim universal Unicode coverage, identical regional CJK shaping, color
emoji support, private-use/custom-font support or every ligature/caret position.
Preserve shaping clusters, ZWJ and variation selectors, and compare actual
glyphs, wrapping and native editing across Chromium, WebKit/Safari and Firefox.
Font-load completion alone is not canvas presentation or selection parity.
Pinned `bidi-js` 1.0.3 supplies Unicode bidi levels/run geometry when required;
browser Canvas2D remains the text shaper and painter.
Dictionary wrapping and word selection share the word-granularity segmenter in
`ui/textBreak.ts`; grapheme segmentation remains separate.
Await each `FontFace.load()` rather than worker `FontFaceSet.ready`, which can
remain pending even after its faces report loaded. Run
`scripts/canvas/text-smoke.mjs` with the path to an already installed Playwright
module after text or font changes. It compares real glyph masks and range
behavior, native-control end carets against worker advances, and primary-face
advances across engines; it does not download browsers or use application data.

Keep the shipped license inventory alongside the source assets:

- `frontend/src/assets/Inter.woff2.license.txt` and
  `RobotoMono.woff2.license.txt`: SIL OFL 1.1 notices for the shipped primary fonts.
- Source-font notices remain beside the unshipped regeneration inputs.
- `frontend/vendor/taffy/LICENSE` covers the pinned WASM layout binding.
- `bidi-js` 1.0.3 carries its local MIT notice with copyright 2021 Jason Johnston.
- Emitted package notices, including pinned data-only `lucide` 0.545.0 (ISC),
  come from actual bundled modules' local package licenses, not a replacement
  React-wrapper license or an assumed SPDX string.

`frontend/tools/releaseNotices.ts` gathers both main and lazy-worker module
inventories, includes bundled package licenses and adjacent imported-font notices,
emits bundled `third-party-notices.txt` and inserts a `rel="license"` link into
the release HTML before index publication. Missing/empty/oversized notices fail
the build. Do not publish the vendor source or regeneration workspace
as a substitute for this bounded notice artifact.

## Meaningful verification and profiling

Use disposable synthetic data and repository-owned scratch, caches, build
outputs and browser profiles. Never use the authoritative dataset, probe an
unavailable upstream, run live synchronization/providers or deploy/publish merely
to verify frontend work. Coordinate shared checks after edits are integrated.

### Canonical build and focused checks

From the repository root, prepare an owned workspace before commands that create
temporary files (use a fresh owned child for separate runs):

```sh
export CANVAS_WORKSPACE="$PWD/.tree-build/canvas-verification"
mkdir -p "$CANVAS_WORKSPACE/runtime" "$CANVAS_WORKSPACE/go-cache"
chmod 700 "$CANVAS_WORKSPACE" "$CANVAS_WORKSPACE/runtime"
export TMPDIR="$CANVAS_WORKSPACE/runtime"
export GOCACHE="$CANVAS_WORKSPACE/go-cache"
export TREE_FRONTEND_OUT="$CANVAS_WORKSPACE/frontend"
bun run --cwd frontend build
bun test --cwd frontend
bun run --cwd frontend lint
frontend/node_modules/.bin/oxfmt --check README.md AGENTS.md docs/canvas-authoring.md
```

The build includes TypeScript checking; `bun run --cwd frontend typecheck` is the
focused non-emitting check. For the complete supported native gate use
`./tree check`, which suspends/resumes the previous mode and uses disposable
fixtures. Its frontend test command runs from `frontend/`, matching CI and
including the tooling, canvas source and calendar regressions.
Neither a build nor this gate is a rendered-fidelity, warm-cache interactivity or
deployed-origin acceptance pass.

### Real native workflows

The supported permanent native command is `./tree check`. It prepares the
compiled production browser build and native prerequisites for tests including
`TestNativeProductionFrontend` in
`backend/internal/app/workflow/frontend_runtime_test.go`, which checks Go-served
page entries, stable operation without a JavaScript process, process memory and
shutdown boundaries. These checks do not establish full seeded learner workflows
or trusted browser interactions. Keep the setup-verified native prerequisites;
do not bypass an opt-in test's missing-tool skip with fabricated paths.

For focused static browser delivery and runtime-authority checks, with the owned
temporary/cache environment above:

```sh
go -C backend test ./internal/infrastructure/webui
bun test frontend/tools/tests
```

There is no permanent full-workflow held-server command. When collecting
end-to-end browser evidence, the verification owner must provide a disposable
isolated fixture with the following contract rather than start live development
data or substitute an HTML/mock API:

- Serve the compiled canonical build through real Go handlers with actual
  Host/Origin checks, original-page runtime tokens and namespaced browser storage.
- Use private synthetic SQLite, objects and learner data under the owned
  `.tree-build/` workspace. Never read, mutate, checkpoint or synchronize the
  authoritative dataset as incidental browser verification.
- Disable external workers and block non-loopback outbound HTTP. Any catalog or
  model transport needed by a workflow must be a deterministic local double
  behind the real native handler/service boundary, with synthetic credentials.
- Seed the relevant source identities and learner state for course
  files/revisions, activity/exercises, materials, roadmap/practice, Study,
  session writes/annotations, saved/streamed Ask and Settings.
- Keep the server alive for trusted headed input, observe committed writes and
  loader races through real handlers, and finish with owned-process shutdown and
  synthetic-data cleanup. Keep profiles, screenshots and traces under the same
  private workspace.

Temporary proof fixtures are not a public lifecycle command, permanent test API
or alternate frontend. Their existence in a verification session must not become
a lasting command or source-path promise in application guidance.

### Rendered/native evidence

Exercise Chromium, WebKit/Safari and Firefox using shipped APIs on substantive canonical routes with visible
browser windows. Initialize the window on `about:blank` before measured navigation;
opening the measured URL as the initial browser page can contaminate startup.
Inspect nonblank content at representative viewport/DPR/theme combinations and
use trusted pointer/keyboard input. Synthetic dispatch, ready markers, HTTP 200s
and blank-equals-blank screenshots do not establish visual fidelity.

Include first-tap mobile focus; wrapped/newline and mixed-direction selection;
IME/grapheme ownership; disabled/read-only controls; modal clipping; single-line
Settings fields and long Ask textareas with wheel remainder/draft/selection at
both boundaries; native file/copy/link actions; history/query/hash restoration;
and stale-runtime mutation rejection. Compare actual `WheelEvent` deltas, not
driver wheel arguments, because engines can scale them differently.
For persistence across reloads, explicitly reload and verify a new document
time origin. Navigating to the same hash URL can remain a same-document navigation.

Automate text entry through the exposed native control with trusted pointer and
keyboard events. A previously captured `data-nodes` rectangle can become stale
while the Ask composer moves between its empty and streaming layouts; do not
mask a missed click with programmatic focus. Firefox request interception can
also mask native SSE cancellation. For that proof, constrain egress with a
loopback-only gateway instead of intercepting every request, then check the
actual fetch abort signal, absence of a saved half-turn, and admission of the
next real native request.

Hold loader GETs across confirmed course reorder and exercise hide/restore writes
so stale responses cannot undo the write. Verify touch scrolling instead of drag
reorder, available move buttons and disposed-node focus. Exercise source swaps,
reader paging, zoom, annotations and cancellation while PDF work is pending.

### Startup and worker profiles

`data-metrics` exposes phase timestamps, commits, text/style preparation, Taffy
layout, content/chrome/composite wall time and frame gaps. `data-nodes` exposes
inspection rectangles (sensitive text is redacted); `data-renderer` records the
reported GPU/engine label. Dispatch `canvas-inspect` to refresh the snapshot.

`firstContentMs` is recorded after worker drawing/submission when a shown ready
node exists and visible pending nodes are absent. Primary FontFace loading and
Taffy initialization precede application startup; system fallback is browser-owned.
`app/main.ts` also waits for native editor font readiness before the ready state,
then two main-thread animation frames before `data-presented-ms` and
`data-interactive-budget-ms`/`data-interactive-budget-passed`. Those are diagnostic
barriers, not optical presentation or proof every pixel and control is correct.
Browser First Contentful Paint can precede the application's complete canvas
frame; do not substitute it for full-viewport readiness or presentation evidence.
Headless captures do not replace visible-window inspection of the combined
content/navigation output.

Record fresh-profile cold navigations separately from warm-cache new-document navigations, including
engine/device/GPU identity, actual requests, transferred/compressed asset bytes,
latency and trusted input evidence. Fresh profiles alone do not prove the network
path or response representation. For compression comparisons, use a fresh owned
origin per case and record actual native-response status, encoding and body bytes;
reject an identity control that still receives gzip. Loopback proof gateways must
accept same-origin absolute-form request targets as well as paths (observed with
WebKit), while rejecting other origins. New profiles/origins do not reset OS or
GPU-driver caches.

Attach profiling to the render worker, not just the main thread. GL calls can
include driver waits: distinguish trace
thread-CPU duration from wall-time sampling before claiming a CPU saving.
For raw Firefox profiles, use `samples.schema` and `meta.sampleUnits` to locate
and unit-check `threadCPUDelta`; requested feature flags alone do not establish
whether those samples exist. Record synchronous asset-call duration separately
from promise completion: a completed asset can wait behind another module's
loading/compilation before its callback runs.
Canvas2D delegates shaping/drawing to the browser; Taffy still runs in WASM.
Distinguish browser shaping, JavaScript paragraph/bidi preparation, WASM binding
overhead and native layout work. A larger profiling Taffy WASM can locate work
but cannot stand in for measurements of the shipping build. Check paging hitches
separately from steady scrolling; culling drawing does not make eager text
preparation/layout lazy.

Check the Firefox executable as well as its control protocol. A Playwright-supplied
Firefox can initialize Juggler frame/worker actors even without a Playwright
connection. Keep that instrumentation separate from application CPU, and use an
unmodified release Firefox with a fresh owned profile for the independent control.
Record its executable/version and inspect profiles for `chrome://juggler/` frames
rather than assuming a native protocol removes the patched browser's hooks.
Firefox's native
[WebDriver BiDi viewport command](https://developer.mozilla.org/en-US/docs/Web/WebDriver/Reference/BiDi/Modules/browsingContext/setViewport)
allows a dimension-matched control without the Playwright/Juggler connection.
Verify actual viewport/DPR, native foreground identity, visible content and
trusted input. An OS-clamped, differently sized control is not a replacement
acceptance result. The command sets an internal browser-element viewport, not
the OS window size: record window/container geometry and clipping before claiming
that the full requested viewport was physically visible. A protocol screenshot
can include pixels outside the exposed window area.

Measure main-thread RAF cadence alongside worker frame duration and gaps.
If both are paced below the display target, compare a focused `about:blank`
window at the same viewport/DPR before attributing the limit to application work.
A fast worker frame does not establish 60 Hz presentation.
On macOS, also verify the disposable browser's native application PID is
foreground before each measured navigation. `document.hasFocus()` and
`visibilityState` alone do not establish native foreground status. Activate only
the owned test application, outside the measured interval, without changing
global power or window settings.

The user-approved acceptance target is **at most 150 ms from warm-cache navigation
to the full, correct, interactive first viewport**. Cold navigation is best effort,
measured and reported separately rather than subjected to the 150 ms warm target.
`frontend/performance-budgets.json` owns `warmAppInteractiveMs`. Model the real
link at about **200 Mbit/s and 15 ms RTT**, and report repeated-run medians in
Chromium, WebKit/Safari and Firefox with shipped browser APIs.

Warm means populated browser asset caches, not reusing an already-running worker,
an in-app route transition or a same-document hash navigation. Initialize a visible
window on `about:blank` before each full measured navigation, preserving the cache
for warm runs and using an empty browser cache for cold runs. Count all remaining
connection/request latency, assets, data, fonts, initialization, layout, drawing
and usable input handling from navigation start. Prove substantive nonblank content,
correct glass/text and trusted native interaction; a screenshot, loading shell,
timer started after downloads or worker CPU alone is insufficient.

The budget marker does not establish cache state, network conditions, visual
correctness or performance acceptance. A modeled localhost pass does not establish
a deployed-device pass. No deployed-origin acceptance is established by this guide
or the source cutover, and functional parity cannot approve takeover.
