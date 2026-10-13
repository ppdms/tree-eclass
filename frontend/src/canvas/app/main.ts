// Browser-only input, native activation and captured page state; rendering stays in the worker.
import budgets from '../../../performance-budgets.json';
import type { FromApp, ToApp } from './protocol';
import { CanvasEditor } from './editor';
import { parseRichHTML } from './htmlParser';
import { BrowserStorageHost, parseReadIds } from './browserStorage';
import { BrowserHistoryHost } from './browserHistory';
import { BrowserClipboardHost } from './browserClipboard';
import { BrowserActivationHost } from './browserActivation';
import { BrowserEnvironmentHost } from './browserEnvironment';
import { BrowserInputHost } from './browserInput';
import { BrowserHost } from './browserHost';
import { startDevelopmentReload } from './developmentReload';

function element<T extends HTMLElement>(id: string, kind: { new (): T }): T {
  const found = document.getElementById(id);
  if (!(found instanceof kind)) throw new Error(`Missing #${id}`);
  return found;
}
const canvas = element('stage', HTMLCanvasElement);
const root = document.documentElement.dataset;
let worker: Worker;
let fail: (detail: string) => void;
let editor: CanvasEditor;
let activation: BrowserActivationHost;
let browser: BrowserHost;
let historyHost: BrowserHistoryHost;
let storage: BrowserStorageHost;
let presentationQueued = false;

function send(message: ToApp, transfer: Transferable[] = []): void {
  worker.postMessage(message, transfer);
}
function receive(message: FromApp): void {
  switch (message.type) {
    case 'error':
      fail(message.detail);
      break;
    case 'navigate':
      historyHost.navigate(message);
      break;
    case 'scroll':
      historyHost.recordScroll(message.offset);
      break;
    case 'readIds':
      try {
        storage.write('local', 'treeeclass:activity-read', JSON.stringify(message.ids));
      } catch {
        /* Local read preferences remain live in the worker when persistence fails. */
      }
      break;
    case 'links':
      activation.links(message.items);
      root.links = JSON.stringify(message.items);
      break;
    case 'fields':
      editor.update(message.items, message.hotspots);
      activation.fields(message.hotspots, message.actions);
      break;
    case 'focus':
      activation.focus(message.target);
      break;
    case 'selection':
      browser.clipboard.mirror(message.text);
      break;
    case 'browserRequest':
      browser.receive(message);
      break;
    case 'browserCancel':
      browser.cancel(message.id);
      break;
    case 'html':
      receiveHTML(message);
      break;
    case 'cursor':
      canvas.style.cursor = message.cursor;
      canvas.title = message.title ?? '';
      break;
    case 'metrics':
      receiveMetrics(message);
      break;
  }
}
function receiveHTML(message: Extract<FromApp, { type: 'html' }>): void {
  try {
    send({ type: 'htmlResult', id: message.id, blocks: parseRichHTML(message.value) });
  } catch {
    send({ type: 'htmlResult', id: message.id, blocks: [], error: 'Native HTML parsing failed' });
  }
}
function receiveMetrics(message: Extract<FromApp, { type: 'metrics' }>): void {
  root.metrics = JSON.stringify(message.values);
  root.nodes = JSON.stringify(
    message.nodes.map((node) => (editor.isSensitive(node.id) ? { ...node, text: '' } : node)),
  );
  root.renderer = message.renderer;
  if (!(message.values.firstContentMs ?? 0) || presentationQueued) return;
  presentationQueued = true;
  void editor.ready.then(
    () => {
      if (root.state !== 'error') root.state = 'ready';
      // Evidence still includes the real input/font readiness barrier and two main-thread RAFs.
      requestAnimationFrame(() =>
        requestAnimationFrame(() => {
          const elapsed = performance.now();
          root.presentedMs = String(elapsed);
          root.interactiveBudgetMs = String(budgets.warmAppInteractiveMs);
          root.interactiveBudgetPassed = String(elapsed <= budgets.warmAppInteractiveMs);
        }),
      );
    },
    (error: Error) => fail(error.message),
  );
}
function createHosts(fonts: Promise<void>): BrowserEnvironmentHost {
  storage = new BrowserStorageHost(send);
  historyHost = new BrowserHistoryHost(send, storage);
  editor = new CanvasEditor(element('overlay', HTMLInputElement), send, fonts, canvas);
  const clipboard = new BrowserClipboardHost(editor);
  activation = new BrowserActivationHost(editor, clipboard, send);
  browser = new BrowserHost(editor, clipboard, historyHost, storage, send);
  return new BrowserEnvironmentHost(canvas, send);
}
export function start(renderWorker: Worker, onError: (detail: string) => void, fonts: Promise<void>): void {
  worker = renderWorker;
  fail = onError;
  worker.addEventListener('message', (event: MessageEvent<FromApp>) => receive(event.data));
  const environment = createHosts(fonts);
  void editor.ready.catch((error: Error) => fail(error.message));
  const offscreen = canvas.transferControlToOffscreen();
  const snapshot = storage.snapshot;
  send(
    {
      type: 'boot',
      canvas: offscreen,
      viewport: environment.viewport,
      url: historyHost.url,
      runtimeSession: root.treeRuntime ?? '',
      readIds: parseReadIds(snapshot.local['treeeclass:activity-read'] ?? null),
      navigationStart: performance.timeOrigin,
      storage: snapshot,
      systemDark: environment.systemDark,
      reducedMotion: environment.reducedMotion,
      visible: environment.visible,
      historyState: history.state,
      historyKey: historyHost.key,
      ...environment.band,
    },
    [offscreen],
  );
  environment.track();
  startDevelopmentReload();
  new BrowserInputHost(canvas, editor, activation, send).track();
}
