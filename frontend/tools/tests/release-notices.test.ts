import assert from 'node:assert/strict';
import { mkdir, mkdtemp, readFile, rm, symlink, writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { test } from 'bun:test';
import { build } from 'vite';
import type { InlineConfig, Plugin } from 'vite';
import type { RolldownWatcher, RolldownWatcherEvent } from 'rolldown';
import { releaseNotices } from '../releaseNotices';

const frontend = resolve(import.meta.dirname, '../..');
const verification = resolve(frontend, '../.tree-build/canvas-verification');
const retiredLicense = 'MIT fixture-worker-retired license, original bytes';
const changedLicense = 'MIT fixture-worker-retired license, changed bytes';
const lazyLicense = 'MIT fixture-worker-lazy license';
const currentLicense = 'MIT fixture-worker-current license';
const workerFontLicense = 'OFL fixture-worker-font license';

interface ReleaseArtifact {
  href: string;
  notices: string;
  worker?: string;
}

function mainSource(revision: string, worker = true): string {
  return [
    "import inter from './assets/Inter.woff2?url';",
    "import mono from './assets/RobotoMono.woff2?url';",
    `document.body.dataset.fixture = JSON.stringify([inter, mono, '${revision}']);`,
    worker ? "new Worker(new URL('./worker.ts', import.meta.url), { type: 'module' });" : '',
  ].join('\n');
}

function workerSource(retired: boolean): string {
  const route = retired ? 'old-route' : 'licensed-route';
  return [
    `import value from 'fixture-worker-${retired ? 'retired' : 'current'}';`,
    retired ? "import font from './worker.woff2?url';" : "const font = '';",
    // Static imports cannot exercise the lazy worker route boundary under test.
    `onmessage = async () => postMessage([value, font, await import('./${route}.ts')]);`,
  ].join('\n');
}

async function fixturePackage(root: string, name: string, license: string): Promise<void> {
  const directory = resolve(root, 'node_modules', name);
  await mkdir(directory, { recursive: true });
  await Promise.all([
    writeFile(resolve(directory, 'package.json'), JSON.stringify({ name, version: '1.0.0', type: 'module' })),
    writeFile(resolve(directory, 'index.js'), `export default '${name}';`),
    writeFile(resolve(directory, 'LICENSE'), license),
  ]);
}

async function fixture(root: string): Promise<void> {
  await mkdir(resolve(root, 'vendor'), { recursive: true });
  await Promise.all([
    fixturePackage(root, 'fixture-worker-retired', retiredLicense),
    fixturePackage(root, 'fixture-worker-lazy', lazyLicense),
    fixturePackage(root, 'fixture-worker-current', currentLicense),
  ]);
  await Promise.all([
    symlink(resolve(frontend, 'node_modules/lucide'), resolve(root, 'node_modules/lucide')),
    symlink(resolve(frontend, 'vendor/taffy'), resolve(root, 'vendor/taffy')),
    symlink(resolve(frontend, 'src/assets'), resolve(root, 'assets')),
    symlink(resolve(frontend, 'src/assets/Inter.woff2'), resolve(root, 'worker.woff2')),
    writeFile(resolve(root, 'worker.woff2.license.txt'), workerFontLicense),
    writeFile(
      resolve(root, 'index.html'),
      '<html><head></head><body><script type="module" src="/main.ts"></script></body></html>',
    ),
    writeFile(resolve(root, 'main.ts'), mainSource('initial')),
    writeFile(resolve(root, 'worker.ts'), workerSource(true)),
    writeFile(
      resolve(root, 'old-route.ts'),
      ["export { default as retiredLazy } from 'fixture-worker-lazy';", "export * from './licensed-route';"].join('\n'),
    ),
    writeFile(
      resolve(root, 'licensed-route.ts'),
      ["export { default as taffy } from './vendor/taffy/index.js';", "export { Folder } from 'lucide';"].join('\n'),
    ),
  ]);
}

function fixtureConfig(root: string, countWorkerBuild: () => void): InlineConfig {
  const notices = releaseNotices(root);
  const counter: Plugin = { name: 'fixture-worker-build-count', buildStart: countWorkerBuild };
  return {
    configFile: false,
    root,
    publicDir: false,
    logLevel: 'silent',
    // Preserve the fixture's local package/font inventory without copying vendor bytes.
    resolve: { preserveSymlinks: true },
    plugins: [notices.plugin],
    worker: {
      format: 'es',
      plugins: () => [
        {
          name: 'fixture-worker-entry-facade',
          options: (options) => ({ ...options, preserveEntrySignatures: 'strict' }),
        },
        counter,
        notices.worker(),
      ],
    },
    build: {
      outDir: resolve(root, 'dist'),
      emptyOutDir: false,
      assetsInlineLimit: 0,
      watch: {},
    },
  };
}

function completedBuild(watcher: RolldownWatcher): Promise<void> {
  const { promise, resolve: accept, reject } = Promise.withResolvers<void>();
  // A deadline reports a stuck watcher; successful steps await actual build events.
  const deadline = AbortSignal.timeout(30_000);
  const cleanup = () => {
    watcher.off('event', listener);
    deadline.removeEventListener('abort', expired);
  };
  const expired = () => {
    cleanup();
    reject(new Error('Release notice fixture did not receive a build completion event'));
  };
  const listener = (event: RolldownWatcherEvent) => {
    if (event.code !== 'BUNDLE_END' && event.code !== 'ERROR') return;
    cleanup();
    if (event.code === 'ERROR') reject(event.error);
    else accept();
  };
  watcher.on('event', listener);
  deadline.addEventListener('abort', expired, { once: true });
  return promise;
}

async function rebuild(watcher: RolldownWatcher, file: string, source: string): Promise<void> {
  const complete = completedBuild(watcher);
  await writeFile(file, source);
  await complete;
}

async function artifact(root: string): Promise<ReleaseArtifact> {
  const out = resolve(root, 'dist');
  const html = await readFile(resolve(out, 'index.html'), 'utf8');
  const href = html.match(/<link rel="license" href="([^"]+)"/u)?.[1];
  assert.ok(href, 'emitted entry links its release notice artifact');
  const script = html.match(/<script[^>]+src="([^"]+)"/u)?.[1];
  assert.ok(script, 'emitted entry links its application chunk');
  const code = await readFile(resolve(out, script.slice(1)), 'utf8');
  const worker = code.match(/\/assets\/worker-[\w-]+\.js/u)?.[0];
  if (worker) await readFile(resolve(out, worker.slice(1)));
  return { href, worker, notices: await readFile(resolve(out, href.slice(1)), 'utf8') };
}

async function primaryNotices(notices: string): Promise<void> {
  for (const file of ['Inter.woff2', 'RobotoMono.woff2']) {
    assert.ok(notices.includes(`assets/${file} / font license`), `primary attribution for ${file}`);
    const license = await readFile(resolve(frontend, 'src/assets', `${file}.license.txt`), 'utf8');
    assert.ok(notices.includes(license.trimEnd()), `primary license bytes for ${file}`);
  }
}

async function workerNotices(notices: string): Promise<void> {
  for (const [directory, label, file] of [
    ['vendor/taffy', 'tree-canvas-taffy 3.0.0-trim.2', 'LICENSE'],
    ['node_modules/lucide', 'lucide 0.545.0 / LICENSE', 'LICENSE'],
  ]) {
    assert.ok(notices.includes(label!), `bundled dependency attribution for ${label}`);
    const license = await readFile(resolve(frontend, directory!, file!), 'utf8');
    assert.ok(notices.includes(license.trimEnd()), `bundled dependency license bytes for ${label}`);
  }
  assert.ok(notices.includes('ISC License'), 'data-only Lucide uses its ISC notice');
  assert.ok(!notices.includes('lucide-react'), 'no React-wrapper substitute');
}

async function cachedRebuilds(root: string, watcher: RolldownWatcher): Promise<void> {
  const initial = await artifact(root);
  assert.ok(initial.worker, 'initial release contains a linked worker');
  assert.ok(initial.notices.includes(retiredLicense));
  assert.ok(initial.notices.includes(lazyLicense), 'lazy worker route package is attributed');
  assert.ok(initial.notices.includes(workerFontLicense), 'worker-only font is attributed');
  await primaryNotices(initial.notices);
  await workerNotices(initial.notices);
  await rebuild(watcher, resolve(root, 'main.ts'), mainSource('cached'));
  const cached = await artifact(root);
  assert.equal(cached.worker, initial.worker);
  assert.equal(cached.notices, initial.notices, 'unchanged cached worker retains all notice inputs');
  await rebuild(watcher, resolve(root, 'node_modules/fixture-worker-retired/LICENSE'), changedLicense);
  const changed = await artifact(root);
  assert.equal(changed.worker, initial.worker);
  assert.notEqual(changed.href, initial.href, 'changed license bytes produce a new linked artifact');
  assert.ok(changed.notices.includes(changedLicense));
  assert.ok(!changed.notices.includes(retiredLicense));
  await workerNotices(changed.notices);
}

async function changedWorker(root: string, watcher: RolldownWatcher): Promise<void> {
  await rebuild(watcher, resolve(root, 'worker.ts'), workerSource(false));
  const changed = await artifact(root);
  assert.ok(changed.worker);
  assert.ok(changed.notices.includes(currentLicense));
  for (const retired of [changedLicense, lazyLicense, workerFontLicense]) {
    assert.ok(!changed.notices.includes(retired), 'changed worker retires old package/font inputs');
  }
  await primaryNotices(changed.notices);
  await workerNotices(changed.notices);
  await Promise.all([
    rm(resolve(root, 'node_modules/fixture-worker-retired/LICENSE')),
    rm(resolve(root, 'node_modules/fixture-worker-lazy/LICENSE')),
    rm(resolve(root, 'worker.woff2.license.txt')),
  ]);
  await rebuild(watcher, resolve(root, 'main.ts'), mainSource('retired-input-files-deleted'));
  const cached = await artifact(root);
  assert.equal(cached.worker, changed.worker);
  assert.equal(cached.notices, changed.notices, 'later rebuilds do not read retired license files');
}

async function retiredWorker(root: string, watcher: RolldownWatcher): Promise<void> {
  await rebuild(watcher, resolve(root, 'main.ts'), mainSource('removed', false));
  const removed = await artifact(root);
  assert.equal(removed.worker, undefined);
  for (const retired of [currentLicense, 'tree-canvas-taffy', 'lucide 0.545.0']) {
    assert.ok(
      !removed.notices.includes(retired),
      'removed worker retires dependency notices despite Vite cache assets',
    );
  }
  await primaryNotices(removed.notices);
  await rebuild(watcher, resolve(root, 'main.ts'), mainSource('reintroduced'));
  const reintroduced = await artifact(root);
  assert.ok(reintroduced.worker);
  assert.ok(reintroduced.notices.includes(currentLicense));
  await workerNotices(reintroduced.notices);
}

test('linked Vite release notices follow cached, changed and retired worker inventories', async () => {
  await mkdir(verification, { recursive: true });
  const root = await mkdtemp(resolve(verification, 'release-notices-'));
  let watcher: RolldownWatcher | undefined;
  let workerBuilds = 0;
  try {
    await fixture(root);
    const result = await build(
      fixtureConfig(root, () => {
        workerBuilds++;
      }),
    );
    assert.ok('on' in result, 'fixture starts Vite build/watch');
    watcher = result;
    await completedBuild(watcher);
    assert.equal(workerBuilds, 1);
    await cachedRebuilds(root, watcher);
    assert.equal(workerBuilds, 1, 'main and license-only rebuilds use the cached worker');
    await changedWorker(root, watcher);
    assert.equal(workerBuilds, 2, 'changed worker is rebuilt');
    await retiredWorker(root, watcher);
    assert.equal(workerBuilds, 2, 'removed/reintroduced worker retains the valid cached inventory');
  } finally {
    await watcher?.close();
    await rm(root, { recursive: true, force: true });
  }
}, 180_000);

async function fontFixture(root: string, notice: string | undefined): Promise<InlineConfig> {
  await Promise.all([
    symlink(resolve(frontend, 'src/assets/Inter.woff2'), resolve(root, 'font.woff2')),
    writeFile(
      resolve(root, 'index.html'),
      '<html><head></head><body><script type="module" src="/main.ts"></script></body></html>',
    ),
    writeFile(resolve(root, 'main.ts'), "import font from './font.woff2?url';\ndocument.body.dataset.font = font;"),
  ]);
  if (notice !== undefined) await writeFile(resolve(root, 'font.woff2.license.txt'), notice);
  return {
    configFile: false,
    root,
    publicDir: false,
    logLevel: 'silent',
    resolve: { preserveSymlinks: true },
    plugins: [releaseNotices(root).plugin],
    build: { outDir: resolve(root, 'dist'), assetsInlineLimit: 0 },
  };
}

async function rejectedFontNotice(notice: string | undefined): Promise<void> {
  await mkdir(verification, { recursive: true });
  const root = await mkdtemp(resolve(verification, 'font-notices-'));
  try {
    const config = await fontFixture(root, notice);
    const expected =
      notice === undefined ? /ENOENT.*font\.woff2\.license\.txt/u : /Invalid or oversized release notice/u;
    await assert.rejects(build(config), expected);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}

test('imported WOFF2 fonts require an adjacent license notice', async () => {
  await rejectedFontNotice(undefined);
});

test('imported WOFF2 fonts reject empty license notices', async () => {
  await rejectedFontNotice(' \n');
});

test('imported WOFF2 fonts reject oversized license notices', async () => {
  await rejectedFontNotice('x'.repeat(128 * 1024 + 1));
});
