// Usage: node scripts/canvas/text-smoke.mjs /absolute/path/to/playwright/index.mjs
// Compiles owned text/field fixtures and exercises Chromium, WebKit and Firefox offline.
import { spawnSync } from 'node:child_process';
import { createServer } from 'node:http';
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const repository = resolve(dirname(fileURLToPath(import.meta.url)), '../..');
const workspace = join(repository, '.tree-build/canvas-verification');
const runtime = join(workspace, 'runtime');
const modulePath = process.argv[2];
if (!modulePath) throw new Error('Pass the installed Playwright index.mjs path; this harness never downloads browsers');
const playwright = await import(pathToFileURL(resolve(modulePath)).href);
await mkdir(runtime, { recursive: true, mode: 0o700 });
const output = await mkdtemp(join(workspace, 'text-smoke-'));
const environment = { ...process.env, TMPDIR: runtime };
const compiled = spawnSync(
  'bun',
  [
    'build',
    'frontend/tests/canvasText.worker.ts',
    'frontend/tests/canvasText.host.ts',
    '--target=browser',
    `--outdir=${output}`,
  ],
  { cwd: repository, env: environment, encoding: 'utf8' },
);
if (compiled.status !== 0) throw new Error(compiled.stderr || compiled.stdout || 'Worker harness compilation failed');

const files = {
  '/canvasText.worker.js': [join(output, 'canvasText.worker.js'), 'text/javascript'],
  '/canvasText.host.js': [join(output, 'canvasText.host.js'), 'text/javascript'],
  '/Inter.woff2': [join(repository, 'frontend/src/assets/Inter.woff2'), 'font/woff2'],
  '/RobotoMono.woff2': [join(repository, 'frontend/src/assets/RobotoMono.woff2'), 'font/woff2'],
};
const server = createServer(async (request, response) => {
  if (request.url === '/') {
    response.writeHead(200, { 'Content-Type': 'text/html' });
    response.end('<!doctype html><meta charset="utf-8"><title>Owned Canvas2D text smoke</title>');
    return;
  }
  const entry = files[request.url];
  if (!entry) {
    response.writeHead(404);
    response.end();
    return;
  }
  try {
    const body = await readFile(entry[0]);
    response.writeHead(200, { 'Content-Type': entry[1] });
    response.end(body);
  } catch (error) {
    response.writeHead(500);
    response.end(String(error));
  }
});
await new Promise((done) => server.listen(0, '127.0.0.1', done));
const address = server.address();
if (!address || typeof address === 'string') throw new Error('Loopback test server did not bind');
const url = `http://127.0.0.1:${address.port}`;
const results = [];
try {
  for (const name of ['chromium', 'webkit', 'firefox']) {
    const profile = await mkdtemp(join(output, `${name}-profile-`));
    let context;
    try {
      context = await playwright[name].launchPersistentContext(profile, { headless: true, env: environment });
      const page = context.pages()[0] || (await context.newPage());
      await page.goto(url);
      const result = await page.evaluate(
        () =>
          new Promise((done, fail) => {
            const worker = new Worker('/canvasText.worker.js', { type: 'module' });
            const deadline = setTimeout(() => {
              worker.terminate();
              fail(new Error('Text smoke timed out'));
            }, 30_000);
            worker.onmessage = (event) => {
              clearTimeout(deadline);
              worker.terminate();
              done(event.data);
            };
            worker.onerror = (event) => {
              clearTimeout(deadline);
              worker.terminate();
              fail(new Error(event.message));
            };
            worker.postMessage({ run: true });
          }),
      );
      if (!result.fatal) {
        const alignment = await page.evaluate(async (samples) => {
          const { nativeFieldAlignment } = await import('/canvasText.host.js');
          try {
            await nativeFieldAlignment(samples);
            return { name: 'nativeFieldAlignment', passed: true };
          } catch (error) {
            return { name: 'nativeFieldAlignment', passed: false, error: String(error) };
          }
        }, result.advances);
        result.results.push(alignment);
      }
      results.push({ engine: name, ...result });
    } catch (error) {
      results.push({ engine: name, fatal: String(error) });
    } finally {
      if (context) await context.close();
      await rm(profile, { recursive: true, force: true });
    }
  }
} finally {
  await new Promise((done, fail) => server.close((error) => (error ? fail(error) : done())));
}
const parity = results.some((engine) => engine.fatal)
  ? []
  : results[0].advances.map((sample, index) => {
      const widths = results.map((engine) => engine.advances[index].width);
      const spread = Math.max(...widths) - Math.min(...widths);
      return {
        fontSize: sample.fontSize,
        fontWeight: sample.fontWeight,
        mono: sample.mono,
        spread,
        passed: spread <= 1,
      };
    });
const report = { results, parity };
await writeFile(join(output, 'results.json'), JSON.stringify(report, null, 2));
console.log(JSON.stringify({ evidence: join(output, 'results.json'), ...report }, null, 2));
if (
  parity.some((check) => !check.passed) ||
  results.some((engine) => engine.fatal || engine.results.some((check) => !check.passed))
)
  process.exitCode = 1;
