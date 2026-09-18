import { defineConfig, type Plugin } from 'vite';
import { transformAsync } from '@babel/core';
import { createRequire } from 'node:module';
import { resolve } from 'node:path';
import { writeFile, rename } from 'node:fs/promises';

const require = createRequire(import.meta.url);
const babelConfig = require('./postcss.config.cjs').plugins['@stylexjs/postcss-plugin'].babelConfig;

function stylex(): Plugin {
  let definitions: string[] = [];
  return {
    name: 'tree-stylex',
    enforce: 'pre',
    async transform(source, id) {
      if (!id.includes('/src/') || !/\.[jt]sx?$/.test(id) || !source.includes('@stylexjs/stylex')) return;
      for (const file of definitions) this.addWatchFile(file);
      const result = await transformAsync(source, {
        ...babelConfig,
        configFile: false,
        filename: id,
        sourceMaps: true,
      });
      if (result?.code) return { code: result.code, map: result.map };
    },
    async buildStart() {
      const { default: glob } = await import('fast-glob');
      definitions = (await glob('src/**/*.stylex.{js,ts}')).map((file) => resolve(file));
      for (const file of definitions) this.addWatchFile(file);
    },
  };
}

// Publish the entry document last and atomically. Failed rebuilds leave the last
// working entry and its hashed assets available; shutdown cleans old dev assets.
function entry(): Plugin {
  let html = '';
  return {
    name: 'tree-entry',
    enforce: 'post',
    generateBundle(_options, bundle) {
      const index = bundle['index.html'];
      if (index?.type !== 'asset') throw new Error('Missing frontend entry document');
      html = String(index.source);
      delete bundle['index.html'];
    },
    async writeBundle(options) {
      const dir = options.dir!;
      await writeFile(resolve(dir, '.index.tmp'), html);
      await rename(resolve(dir, '.index.tmp'), resolve(dir, 'index.html'));
    },
  };
}

export default defineConfig({
  plugins: [stylex(), entry()],
  resolve: { alias: { '@': resolve(import.meta.dirname, 'src') } },
  build: { outDir: process.env.TREE_FRONTEND_OUT || 'dist', emptyOutDir: false },
});
