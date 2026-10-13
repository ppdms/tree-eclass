import { expect, test } from 'bun:test';
import { mkdtemp, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { resolve } from 'node:path';
import { options, select } from '../ui-review/options';
import { serve } from '../ui-review/server';
import type { ScenarioEntry } from '../ui-review/schemas';

const entries: ScenarioEntry[] = ['workflow--initial', 'workflow--recovery', 'other--empty'].map((id) => ({
  id,
  type: 'story',
  title: 'Synthetic workflow',
  name: id,
  importPath: './synthetic.stories.ts',
  tags: ['ui-review'],
}));

test('overlapping prefixes select each scenario once in catalog order', () => {
  const args = options(['--only', 'workflow--re', '--only', 'workflow--', '--only', 'workflow--re']);
  expect(select(entries, args.only).map((entry) => entry.id)).toEqual(['workflow--initial', 'workflow--recovery']);
  expect(() => select(entries, ['missing--'])).toThrow();
});

test('capture flags reject missing values and output paths that escape the run root', () => {
  expect(() => options(['--only'])).toThrow();
  expect(() => options(['--out', '../other'])).toThrow();
  expect(() => options(['--out', 'first', '--out', 'second'])).toThrow();
  expect(() => options(['--typo'])).toThrow();
});

test('static catalog never turns API misses, writes or escaping symlinks into HTML', async () => {
  const root = await mkdtemp(resolve(tmpdir(), 'tree-ui-server-'));
  const outside = await mkdtemp(resolve(tmpdir(), 'tree-ui-outside-'));
  await writeFile(resolve(root, 'index.html'), '<h1>Synthetic catalog</h1>');
  await writeFile(resolve(outside, 'private.txt'), 'Outside catalog');
  await symlink(resolve(outside, 'private.txt'), resolve(root, 'escape.txt'));
  const server = await serve(root);
  try {
    const page = await fetch(server.url);
    expect(page.headers.get('content-type')).toBe('text/html; charset=utf-8');
    expect(await page.text()).toBe('<h1>Synthetic catalog</h1>');
    expect((await fetch(`${server.url}/api/unknown`)).status).toBe(404);
    expect((await fetch(`${server.url}/escape.txt`)).status).toBe(404);
    expect((await fetch(server.url, { method: 'POST' })).status).toBe(405);
  } finally {
    await server.close();
    await rm(root, { recursive: true, force: true });
    await rm(outside, { recursive: true, force: true });
  }
});
