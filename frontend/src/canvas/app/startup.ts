// Start font downloads, layout compilation and the renderer before loading browser input code.
import interUrl from '../../assets/Inter.woff2?url';
import monoUrl from '../../assets/RobotoMono.woff2?url';
import layoutUrl from '../../../vendor/taffy/taffy_wasm_bg.wasm?url';
import { loadEditorFonts } from './editorFonts';
import type { FromApp, ToApp } from './protocol';

const root = document.documentElement.dataset;
function fail(detail: string): void {
  root.state = 'error';
  const error = document.getElementById('error');
  if (error) {
    error.textContent = detail;
    error.style.display = 'block';
  }
  const canvas = document.getElementById('stage');
  if (canvas) canvas.style.cursor = 'default';
}

/** Immutable URLs preserve native font/glyph caches in both document and worker realms. */
function sendFonts(worker: Worker): Promise<void> {
  worker.postMessage({ type: 'fonts', inter: interUrl, mono: monoUrl } satisfies ToApp);
  return loadEditorFonts(interUrl, monoUrl).then(() => {
    root.nativeFontsReadyMs = String(performance.now());
  });
}

function start(): void {
  root.state = 'loading';
  const worker = new Worker(new URL('./worker.ts', import.meta.url), { type: 'module' });
  const earlyError = ({ data }: MessageEvent<FromApp>) => {
    if (data.type !== 'error') return;
    worker.terminate();
    fail(data.detail);
  };
  worker.addEventListener('message', earlyError);
  worker.addEventListener('error', () => fail('Canvas worker failed to load'));
  worker.addEventListener('messageerror', () => fail('Canvas worker message could not be decoded'));
  const abort = (stage: string) => (error: Error) => {
    worker.terminate();
    fail(`Canvas ${stage} failed: ${error.message}`);
  };
  const fonts = sendFonts(worker);
  fonts.catch(abort('font startup'));
  void WebAssembly.compileStreaming(fetch(layoutUrl))
    .then((module) => {
      root.layoutCompiledMs = String(performance.now());
      worker.postMessage({ type: 'layout', module } satisfies ToApp);
    })
    .catch(abort('layout WebAssembly startup'));
  void import('./main')
    .then(({ start }) => {
      if (root.state === 'error') return;
      start(worker, fail, fonts);
      worker.removeEventListener('message', earlyError);
    })
    .catch(abort('browser startup'));
}
try {
  start();
} catch (error) {
  fail(`Canvas startup failed: ${String(error)}`);
}
