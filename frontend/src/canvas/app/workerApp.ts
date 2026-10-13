import type { JsonValue } from '@/lib/display';
import type { UIRuntime } from '../ui/runtime';
import { Renderer } from '../renderer';
import { createCanvasApp, type CanvasApplication, type RouteCommit } from './router';
import { ActivityStore } from '../activity/data';
import { emit, type Boot, type ToApp } from './protocol';
import { configureAPI } from './api';
import { completeHTML } from './html';
import { configureBrowser, completeBrowserResult, writeBrowserStorage } from './browser';
import { configurePreferences, readPreference, receivePreference } from './preferences';
import { receiveActivity } from './activityState';
import { getViewport, updateViewport } from './viewport';
import { markRuntimeChanged } from './runtimeState';
import { setPalette, type ThemeName } from '../ui/palette';
import { anchorId, decodeScrollPositions, scrollStorageKey } from './scrollHistory';
import type { ApplicationMessage, ReceiveApplication, WorkerStartup } from './workerAssets';

const startedAt = performance.timeOrigin + performance.now();
let runtime: UIRuntime | null = null;
let app: CanvasApplication | null = null;
let store: ActivityStore | null = null;
let systemDark = true;

function report(error: Error): void {
  const summary = `${error.name}: ${error.message}`;
  emit({ type: 'error', detail: error.stack?.includes(summary) ? error.stack : `${summary}\n${error.stack ?? ''}` });
}
function isRestoredOffset(scroll: RouteCommit['scroll']): scroll is number {
  return typeof scroll === 'number';
}
function publishRoute(route: RouteCommit): void {
  if (isRestoredOffset(route.scroll)) runtime?.restoreScroll(route.scroll);
  else if (route.scroll !== false && !route.preventScrollReset) {
    runtime?.restoreScroll(route.hashOnly ? null : 0, anchorId(route.url), !route.hashOnly);
  }
  if (!route.native)
    emit({ type: 'navigate', url: route.url, action: route.action, key: route.key, state: route.state });
}
function parseTheme(value: JsonValue): ThemeName {
  return value === 'light' || value === 'dark' || value === 'eink' ? value : 'system';
}
function theme(): void {
  const preference = readPreference<ThemeName>('treeEclass.theme', 'system', parseTheme);
  const resolved =
    preference === 'light' || preference === 'dark' || preference === 'eink'
      ? preference
      : systemDark
        ? 'dark'
        : 'light';
  if (setPalette(resolved) && runtime) {
    runtime.tree.dirty = true;
    runtime.invalidate();
  }
}
function initializeState(message: Boot): void {
  systemDark = message.systemDark;
  updateViewport({
    ...message.viewport,
    top: message.top,
    bottom: message.bottom,
    reducedMotion: message.reducedMotion,
  });
  receiveActivity({ kind: 'visibility', at: Date.now(), visible: message.visible });
  configureAPI(message.runtimeSession, markRuntimeChanged);
  configurePreferences(message.storage, (scope, key, value) => {
    if (scope === 'local' && key === 'treeEclass.theme') theme();
    return writeBrowserStorage(scope, key, value);
  });
  theme();
}
function connectBrowser(owner: UIRuntime): void {
  configureBrowser({
    send: emit,
    error: report,
    scrollPageTo: (offset, options) => owner.scrollPageTo(offset, options),
    scrollAreaTo: (id, offset, options) => owner.scrollAreaTo(id, offset, options),
    scrollToNode: (id) => owner.scrollToNode(id),
    getNodeRect: (id) => owner.getNodeRect(id),
    focusNode: (id) => owner.focusNode(id),
    focusField: (id) => owner.focusField(id),
    capturePointer: (id, pointerId) => owner.capturePointer(id, pointerId),
    releasePointer: (pointerId) => owner.releasePointer(pointerId),
  });
}
export async function boot(message: Boot, startup: WorkerStartup): Promise<ReceiveApplication> {
  initializeState(message);
  const values: UIRuntime['values'] = { applicationReadyMs: startedAt - message.navigationStart };
  const mark = (key: string) => {
    values[key] = performance.timeOrigin + performance.now() - message.navigationStart;
  };
  store = new ActivityStore(message.readIds, (ids) => emit({ type: 'readIds', ids }));
  const positions = decodeScrollPositions(message.storage.session[scrollStorageKey]);
  app = createCanvasApp(
    message.url,
    store,
    publishRoute,
    () => mark('dataReadyMs'),
    message.historyState,
    message.historyKey,
    { positions, read: () => runtime?.scroller.offset ?? 0 },
  );
  // Begin route I/O before synchronous native context creation can wait on the driver.
  const renderer = new Renderer(message.canvas, message.viewport);
  mark('rendererReadyMs');
  const [fonts, Runtime] = await Promise.all([startup.fonts, startup.runtime]);
  for (const key of Object.keys(startup.stamps)) values[key] = startup.stamps[key]! - message.navigationStart;
  runtime = new Runtime(
    renderer,
    fonts,
    {
      metrics: (metrics, nodes, label) => emit({ type: 'metrics', values: metrics, nodes, renderer: label }),
      links: (items) => emit({ type: 'links', items }),
      fields: emit,
      cursor: (cursor, title) => emit({ type: 'cursor', cursor, title }),
      focus: (target) => emit({ type: 'focus', target }),
      selection: (text) => emit({ type: 'selection', text }),
      scroll: (offset) => emit({ type: 'scroll', offset }),
      navigate: app.navigate,
      error: report,
    },
    values,
    message.navigationStart,
  );
  connectBrowser(runtime);
  const restored = positions[message.historyKey];
  runtime.restoreScroll(restored ?? 0, restored === undefined ? anchorId(message.url) : null);
  runtime.mount(app.element);
  return receive;
}
function key(message: Extract<ToApp, { type: 'key' }>): void {
  if (!runtime) return;
  if (
    !message.composing &&
    !message.shift &&
    !message.alt &&
    (message.ctrl || message.meta) &&
    message.key.toLowerCase() === 'k'
  ) {
    if (runtime.getNodeRect('ask-question')) runtime.focusField('ask-question');
    else app?.navigate('/ask?focus=1');
  } else runtime.key(message);
}
function receiveNavigation(message: Extract<ToApp, { type: 'navigate' }>): void {
  app?.receiveNavigation({
    url: message.url,
    key: message.key ?? 'default',
    action: message.action ?? 'POP',
    state: message.state,
  });
}
function auxiliary(message: ToApp): void {
  if (!runtime) return;
  switch (message.type) {
    case 'fieldAction':
      if (runtime.input.fieldAction(message.id, message.instance, message.action) && message.action === 'focus') {
        runtime.focusField(message.id);
      }
      break;
    case 'fieldMetrics':
      runtime.input.metrics(message.items);
      break;
    case 'files':
      runtime.input.files(message.id, message.files);
      break;
    case 'nativeResult':
      runtime.input.result(message.id, message.error);
      break;
    case 'storage':
      receivePreference(message.scope, message.key, message.value);
      theme();
      break;
    case 'activity':
      receiveActivity(message);
      break;
    case 'media':
      systemDark = message.systemDark;
      updateViewport({ ...getViewport(), reducedMotion: message.reducedMotion });
      theme();
      break;
    case 'runtimeChanged':
      markRuntimeChanged();
      break;
    case 'htmlResult':
      completeHTML(message.id, message.blocks, message.error);
      break;
    case 'navigate':
      receiveNavigation(message);
      break;
    case 'readIds':
      store?.replace(message.ids);
      break;
    case 'inspect':
      runtime.inspect();
      break;
  }
}
function receive(message: ApplicationMessage): void {
  if (message.type === 'browserResult') {
    completeBrowserResult(message);
    return;
  }
  // Only boot returns this receiver, after constructing the runtime.
  handle(message, runtime!);
}
function handle(message: ToApp, owner: UIRuntime): void {
  switch (message.type) {
    case 'resize':
      owner.resize(message.viewport);
      break;
    case 'inset':
      owner.insets(message.top, message.bottom);
      break;
    case 'pointer':
      owner.pointer(message);
      break;
    case 'wheel':
      owner.wheel(
        message.x,
        message.y,
        message.shift && !message.dx ? message.dy : message.dx,
        message.shift && !message.dx ? 0 : message.dy,
      );
      break;
    case 'key':
      key(message);
      break;
    case 'edit':
      owner.edit(message);
      break;
    case 'contextMenu':
      owner.contextMenu(message.x, message.y);
      break;
    case 'command':
      if (message.name === 'selectAll') owner.selectAll();
      else owner.clearSelection();
      break;
    default:
      auxiliary(message);
  }
}
