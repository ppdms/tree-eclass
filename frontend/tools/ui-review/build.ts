import { mkdtemp, readFile, rename, rm } from 'node:fs/promises';
import { resolve } from 'node:path';
import { catalog, output } from './paths';
import { catalogLock, prepareWorker, storybook } from './commands';
import { buildProvenance, saveJson, sourceInventory } from './provenance';
import { storybookIndexSchema } from './schemas';

async function build() {
  const unlock = await catalogLock();
  const stage = await mkdtemp(resolve(output, 'building-'));
  try {
    const before = await sourceInventory();
    await prepareWorker();
    await storybook(['build', '--output-dir', stage, '--disable-telemetry', '--quiet']);
    const index = storybookIndexSchema.parse(JSON.parse(await readFile(resolve(stage, 'index.json'), 'utf8')));
    const scenarios = Object.values(index.entries)
      .filter((entry) => entry.type === 'story' && entry.tags.includes('ui-review'))
      .sort((a, b) => a.id.localeCompare(b.id));
    if (!scenarios.length) throw new Error('No ui-review scenarios were indexed by Storybook');
    await saveJson(resolve(stage, 'ui-review-index.json'), { schemaVersion: 1, scenarios });
    const provenance = await buildProvenance(stage);
    if (provenance.sourceHash !== before.sourceHash) {
      throw new Error('UI sources changed during the build. Run `bun run ui:build` again when editing has settled.');
    }
    await saveJson(resolve(stage, 'ui-review-build.json'), provenance);
    await rm(catalog, { recursive: true, force: true });
    await rename(stage, catalog);
    console.log(`Built ${scenarios.length} scenarios: ${catalog}`);
  } finally {
    await rm(stage, { recursive: true, force: true });
    await unlock();
  }
}

await build().catch((error: Error) => {
  console.error(error.message);
  process.exitCode = 1;
});
