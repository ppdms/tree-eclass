import { readFile, readdir } from 'node:fs/promises';
import { relative, resolve, sep } from 'node:path';
import type { Plugin } from 'vite';
import type { OutputBundle, OutputChunk } from 'rolldown';

const licenseName = /^(?:licen[cs]e|copying|notice)(?:[._-].*)?$/i;
const maxDocumentBytes = 128 * 1024;
const maxNoticeBytes = 1024 * 1024;
const maxDocuments = 256;

interface NoticePlugins {
  plugin: Plugin;
  worker: () => Plugin;
}

interface EmittedInputs {
  modules: readonly string[];
  references: readonly string[];
}

interface FileReferences {
  files: Map<string, Set<string>>;
  hooks: Pick<Plugin, 'renderStart' | 'resolveFileUrl'>;
}

function fileReferences(): FileReferences {
  const files = new Map<string, Set<string>>();
  return {
    files,
    hooks: {
      renderStart() {
        files.clear();
      },
      resolveFileUrl: {
        order: 'pre',
        handler({ chunkId, fileName }) {
          let references = files.get(chunkId);
          if (!references) files.set(chunkId, (references = new Set()));
          references.add(fileName);
          return null;
        },
      },
    },
  };
}

function packageDirectory(id: string, root: string): string | null {
  if (id.startsWith('\0')) return null;
  const marker = `${sep}node_modules${sep}`;
  const start = id.lastIndexOf(marker);
  if (start !== -1) {
    const parts = id.slice(start + marker.length).split(sep);
    const count = parts[0]?.startsWith('@') ? 2 : 1;
    return id.slice(0, start + marker.length) + parts.slice(0, count).join(sep);
  }
  const directory = resolve(root, 'vendor/taffy');
  return id.startsWith(directory + sep) ? directory : null;
}

class NoticeInventory {
  readonly #seen = new Set<string>();
  readonly #sections = ['tree-eClass bundled third-party notices\n'];
  #bytes = Buffer.byteLength(this.#sections[0]!);

  constructor(
    readonly root: string,
    readonly watch: (file: string) => void,
  ) {}

  async append(label: string, file: string): Promise<void> {
    if (this.#seen.has(file)) return;
    this.#seen.add(file);
    if (this.#seen.size > maxDocuments) throw new Error('Too many release notice inputs');
    this.watch(file);
    const text = await readFile(file, 'utf8');
    if (!text.trim() || Buffer.byteLength(text) > maxDocumentBytes)
      throw new Error(`Invalid or oversized release notice: ${relative(this.root, file)}`);
    const section = `\n===== ${label} =====\n${text.trimEnd()}\n`;
    this.#bytes += Buffer.byteLength(section);
    if (this.#bytes > maxNoticeBytes) throw new Error('Release notices exceed the bounded inventory');
    this.#sections.push(section);
  }

  source(): string {
    return this.#sections.join('');
  }
}

async function appendPackage(inventory: NoticeInventory, directory: string): Promise<void> {
  const metadataFile = resolve(directory, 'package.json');
  inventory.watch(metadataFile);
  const metadata: { name: string; version: string; license?: string } = JSON.parse(
    await readFile(metadataFile, 'utf8'),
  );
  const label = `${metadata.name} ${metadata.version}`;
  const files = (await readdir(directory, { withFileTypes: true }))
    .filter((file) => file.isFile() && licenseName.test(file.name))
    .map((file) => file.name)
    .sort();
  if (!files.length) throw new Error(`No local license notice for bundled package ${label}`);
  for (const file of files) await inventory.append(`${label} / ${file}`, resolve(directory, file));
  // Data-only Lucide carries its ISC license, not a React-wrapper substitute.
  if (metadata.name === 'lucide' && (metadata.version !== '0.545.0' || metadata.license !== 'ISC'))
    throw new Error('Production icon notices require pinned lucide 0.545.0 (ISC)');
}

async function appendInputs(inventory: NoticeInventory, modules: Set<string>): Promise<void> {
  const packages = new Set<string>();
  for (const id of modules) {
    const directory = packageDirectory(id, inventory.root);
    if (directory) packages.add(directory);
  }
  for (const directory of [...packages].sort()) await appendPackage(inventory, directory);
  for (const file of [...modules].sort()) {
    if (!/\.(?:ttf|otf|woff2?)$/i.test(file)) continue;
    if (packageDirectory(file, inventory.root)) continue;
    await inventory.append(`${relative(inventory.root, file)} / font license`, `${file}.license.txt`);
  }
}

function chunkInputs(chunk: OutputChunk, files: Map<string, Set<string>>): EmittedInputs {
  return {
    modules: chunk.moduleIds,
    references: [...chunk.imports, ...chunk.dynamicImports, ...(files.get(chunk.preliminaryFileName) ?? [])],
  };
}

function emittedModules(
  bundle: OutputBundle,
  workers: Map<string, EmittedInputs>,
  files: Map<string, Set<string>>,
): Set<string> {
  const modules = new Set<string>();
  const visited = new Set<string>();
  const pending: EmittedInputs[] = [];
  for (const output of Object.values(bundle)) {
    if (output.type === 'chunk') pending.push(chunkInputs(output, files));
  }
  while (pending.length) {
    const inputs = pending.pop()!;
    for (const id of inputs.modules) modules.add(id.split('?')[0]!);
    for (const file of inputs.references) {
      if (visited.has(file) || !bundle[file]) continue;
      const worker = workers.get(file);
      if (!worker) continue;
      visited.add(file);
      pending.push(worker);
    }
  }
  // Watch builds also emit Vite's unreferenced cached worker assets. Only live
  // chunk references retain notices; keep cached inputs for worker reintroduction.
  for (const file of workers.keys()) if (!bundle[file]) workers.delete(file);
  return modules;
}

function workerNoticeInputs(workers: Map<string, EmittedInputs>): Plugin {
  const references = fileReferences();
  return {
    name: 'tree-worker-notice-inputs',
    ...references.hooks,
    generateBundle(_options, bundle) {
      for (const output of Object.values(bundle)) {
        if (output.type === 'chunk') workers.set(output.fileName, chunkInputs(output, references.files));
      }
    },
  };
}

/** Collect both bundlers' emitted modules, including lazy worker routes. Imported
 * font bytes need adjacent local notices; a URL or an assumed license is not enough. */
export function releaseNotices(root: string): NoticePlugins {
  const workerModules = new Map<string, EmittedInputs>();
  // Vite's asset metadata can retain file URL references from previous watch
  // outputs. Capture actual current render references before Vite resolves URLs.
  const references = fileReferences();
  return {
    worker: () => workerNoticeInputs(workerModules),
    plugin: {
      name: 'tree-release-notices',
      enforce: 'post',
      ...references.hooks,
      async generateBundle(_options, bundle) {
        const inventory = new NoticeInventory(root, (file) => this.addWatchFile(file));
        await appendInputs(inventory, emittedModules(bundle, workerModules, references.files));
        const reference = this.emitFile({ type: 'asset', name: 'third-party-notices.txt', source: inventory.source() });
        const index = bundle['index.html'];
        if (index?.type !== 'asset') throw new Error('Missing frontend entry for release notices');
        const href = `/${this.getFileName(reference)}`;
        index.source = String(index.source).replace('</head>', `    <link rel="license" href="${href}" />\n  </head>`);
      },
    },
  };
}
