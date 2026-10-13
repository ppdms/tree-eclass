import { createHash, randomUUID } from 'node:crypto';
import { readFile, writeFile, rename, rm } from 'node:fs/promises';
import { resolve } from 'node:path';
import glob from 'fast-glob';
import { frontend, catalog } from './paths';
import { indexSchema, provenanceSchema, type Provenance } from './schemas';

export function digest(bytes: string | Uint8Array) {
  return createHash('sha256').update(bytes).digest('hex');
}

async function inventory(root: string, patterns: string[]) {
  const names = await glob(patterns, { cwd: root, onlyFiles: true, followSymbolicLinks: false });
  return Promise.all(
    names.sort().map(async (path) => ({
      path,
      sha256: digest(await readFile(resolve(root, path))),
    })),
  );
}

export async function sourceInventory() {
  const sources = await inventory(frontend, [
    'src/**',
    'public/**',
    'review/**',
    '.storybook/**',
    'tools/ui-review/**',
    'package.json',
    'bun.lock',
    '*config*',
    'performance-budgets.json',
  ]);
  return { sources, sourceHash: digest(JSON.stringify(sources)) };
}

export async function artifactHash(root: string) {
  const files = await inventory(root, ['**/*', '!ui-review-build.json']);
  return digest(JSON.stringify(files));
}

async function version(name: string) {
  const json = JSON.parse(await readFile(resolve(frontend, 'node_modules', name, 'package.json'), 'utf8'));
  return String(json.version);
}

export async function buildProvenance(root: string): Promise<Provenance> {
  return {
    schemaVersion: 1,
    builtAt: new Date().toISOString(),
    ...(await sourceInventory()),
    artifactHash: await artifactHash(root),
    tools: {
      storybook: await version('storybook'),
      playwright: await version('@playwright/test'),
      msw: await version('msw'),
    },
    fixtureSource: 'synthetic',
  };
}

export async function requireFreshBuild() {
  let provenance: Provenance;
  let index: ReturnType<typeof indexSchema.parse>;
  try {
    provenance = provenanceSchema.parse(JSON.parse(await readFile(resolve(catalog, 'ui-review-build.json'), 'utf8')));
    index = indexSchema.parse(JSON.parse(await readFile(resolve(catalog, 'ui-review-index.json'), 'utf8')));
  } catch (error) {
    throw new Error('Missing or invalid UI catalog. Run `bun run ui:build` before capture.', { cause: error });
  }
  const current = await sourceInventory();
  if (current.sourceHash !== provenance.sourceHash) {
    const changed = current.sources.filter(
      (file) => provenance.sources.find((old) => old.path === file.path)?.sha256 !== file.sha256,
    );
    throw new Error(
      `Stale UI catalog (${changed.map((file) => file.path).join(', ') || 'removed sources'}). ` +
        'Run `bun run ui:build`; capture never rebuilds.',
    );
  }
  if ((await artifactHash(catalog)) !== provenance.artifactHash) {
    throw new Error('UI catalog artifacts changed or are incomplete. Run `bun run ui:build`.');
  }
  return { provenance, scenarios: index.scenarios };
}

export async function saveJson<T>(path: string, value: T) {
  const temporary = `${path}.${randomUUID()}.tmp`;
  try {
    await writeFile(temporary, JSON.stringify(value, null, 2) + '\n');
    await rename(temporary, path);
  } finally {
    await rm(temporary, { force: true });
  }
}
