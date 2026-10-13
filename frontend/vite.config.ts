import { defaultClientConditions, defineConfig, type Plugin } from 'vite';
import { resolve } from 'node:path';
import { writeFile, rename } from 'node:fs/promises';
import { promisify } from 'node:util';
import { gzip } from 'node:zlib';
import { releaseNotices } from './tools/releaseNotices.ts';

const root = import.meta.dirname;
const notices = releaseNotices(root);
const compress = promisify(gzip);

// Publish the entry document last and atomically. Failed rebuilds leave the last
// working entry and its hashed assets available; shutdown cleans old dev assets.
function entry(): Plugin {
  let html = '';
  return {
    name: 'tree-entry',
    enforce: 'post',
    generateBundle: {
      order: 'post',
      async handler(_options, bundle) {
        // The final bundle includes worker outputs and release notices. Emit
        // deterministic representations before any files or the entry are written.
        for (const output of Object.values(bundle)) {
          if (!output.fileName.startsWith('assets/')) continue;
          const source = output.type === 'chunk' ? output.code : output.source;
          const compressed = await compress(source, { level: 9 });
          if (compressed.length < Buffer.byteLength(source)) {
            this.emitFile({ type: 'asset', fileName: `${output.fileName}.gz`, source: compressed });
          }
        }
        const index = bundle['index.html'];
        if (index?.type !== 'asset') throw new Error('Missing frontend entry document');
        html = String(index.source);
        delete bundle['index.html'];
      },
    },
    writeBundle: {
      order: 'post',
      sequential: true,
      async handler(options) {
        const dir = options.dir!;
        await writeFile(resolve(dir, '.index.tmp'), html);
        await rename(resolve(dir, '.index.tmp'), resolve(dir, 'index.html'));
      },
    },
  };
}

export default defineConfig({
  root,
  publicDir: resolve(root, 'public'),
  plugins: [notices.plugin, entry()],
  resolve: {
    alias: { '@': resolve(root, 'src') },
    // Markdown's worker exports avoid browser-only DOM initialization.
    conditions: ['worker', ...defaultClientConditions],
  },
  worker: {
    format: 'es',
    rolldownOptions: {
      output: {
        codeSplitting: {
          groups: [
            {
              // Lazy routes share runtime modules, never the bootstrap entry.
              name: (id, context) => (context.getModuleInfo(id)?.isEntry ? null : 'canvas-runtime'),
              tags: ['$initial'],
            },
          ],
        },
      },
    },
    plugins: () => [
      {
        name: 'canvas-worker-entry-facade',
        options: (options) => ({ ...options, preserveEntrySignatures: 'strict' }),
      },
      notices.worker(),
    ],
  },
  // Taffy's package-relative WASM URL must not move through dependency optimization.
  optimizeDeps: { include: ['pdfjs-dist', 'zod/v4'], exclude: ['taffy-layout'] },
  build: {
    outDir: process.env.TREE_FRONTEND_OUT || 'dist',
    emptyOutDir: false,
    rolldownOptions: { input: resolve(root, 'index.html') },
  },
});
