// Start route I/O and native resources while loading the heavier UI runtime.
// Lazy routes share ordinary modules, never this entry's bootstrap state.
import { emit, type Boot, type ToApp } from './protocol';
import { boot as bootApplication } from './workerApp';
import {
  loadWorkerAssets,
  type ApplicationMessage,
  type ReceiveApplication,
  type StartupFonts,
  type WorkerStartup,
} from './workerAssets';

const stamps: WorkerStartup['stamps'] = { workerReadyMs: performance.timeOrigin + performance.now() };
const fontSources = Promise.withResolvers<StartupFonts>();
const layout = Promise.withResolvers<WebAssembly.Module>();
const queued: ApplicationMessage[] = [];
let receiver: ReceiveApplication | null = null;
let failed = false;

function report(error: Error): void {
  if (failed) return;
  failed = true;
  const summary = `${error.name}: ${error.message}`;
  emit({ type: 'error', detail: error.stack?.includes(summary) ? error.stack : `${summary}\n${error.stack ?? ''}` });
}
function mark(key: string): void {
  stamps[key] = performance.timeOrigin + performance.now();
}
const assets = loadWorkerAssets(fontSources.promise, layout.promise, mark);
const runtime = import('../ui/runtime').then((module) => module.UIRuntime);
void assets.catch(report);
void runtime.catch(report);

async function boot(message: Boot): Promise<void> {
  receiver = await bootApplication(message, { fonts: assets, stamps, runtime });
  for (const pending of queued.splice(0)) receiver(pending);
}

addEventListener('message', ({ data }: MessageEvent<ToApp>) => {
  if (failed) return;
  if (data.type === 'fonts') fontSources.resolve(data);
  else if (data.type === 'layout') layout.resolve(data.module);
  else if (data.type === 'boot') void boot(data).catch(report);
  else if (receiver) receiver(data);
  else queued.push(data);
});
