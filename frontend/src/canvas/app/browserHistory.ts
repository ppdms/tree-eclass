import type { FromApp, HistoryState, ToApp } from './protocol';
import type { BrowserStorageHost } from './browserStorage';
import { decodeScrollPositions, scrollStorageKey } from './scrollHistory';
import { hasHistoryKey, isHistoryObject } from './historyState';

function path(value: string): string {
  const url = new URL(value, location.href);
  return url.pathname + url.search + url.hash;
}
function isEntry(state: HistoryState): state is object {
  return (
    isHistoryObject(state) &&
    (hasHistoryKey(state) || 'usr' in state || ('idx' in state && typeof state.idx === 'number'))
  );
}
function entry(state: HistoryState): object {
  // Only router entry envelopes are spread. Arbitrary structured-clone values
  // (including dates, maps, buffers and cyclic objects) remain opaque user state.
  return isEntry(state) ? state : { usr: state };
}
export class BrowserHistoryHost {
  #url: string;
  #key: string;
  #offset = 0;
  readonly #positions: Record<string, number>;
  constructor(
    readonly send: (message: ToApp) => void,
    readonly storage: BrowserStorageHost,
  ) {
    this.#url = path(location.href);
    this.#key = this.key;
    if (!hasHistoryKey(history.state)) history.replaceState({ ...entry(history.state), key: this.#key }, '');
    this.#positions = decodeScrollPositions(storage.read('session', scrollStorageKey));
    this.#offset = this.#positions[this.#key] ?? 0;
    history.scrollRestoration = 'manual';
    this.#title();
    window.addEventListener('popstate', () => this.#publish(true));
    window.addEventListener('hashchange', () => this.#publish(false));
    window.addEventListener('pagehide', () => this.#persist());
  }
  get url(): string {
    return this.#url;
  }
  get key(): string {
    const state: HistoryState = history.state;
    return hasHistoryKey(state) ? state.key : 'default';
  }
  navigate(message: Extract<FromApp, { type: 'navigate' }>): void {
    if (message.action === 'POP') return;
    const url = new URL(message.url, location.href);
    if (url.origin !== location.origin) throw new Error('Worker navigation must remain same-origin');
    const next = path(url.href);
    const action = message.action ?? 'PUSH';
    if (next === this.#url && action === 'PUSH' && (!message.key || message.key === this.#key)) return;
    this.#persist();
    let state: HistoryState =
      message.state === undefined ? (action === 'REPLACE' ? history.state : null) : message.state;
    if (action === 'REPLACE' && isEntry(state) && isEntry(history.state)) {
      state = { ...history.state, ...state };
    }
    if (message.key) {
      state = { ...entry(state), key: message.key };
    }
    if (action === 'REPLACE') history.replaceState(state, '', url.href);
    else history.pushState(state, '', url.href);
    this.#url = next;
    this.#key = this.key;
    this.#positions[this.#key] = this.#offset;
    this.#title();
  }
  recordScroll(offset: number): void {
    this.#offset = offset;
    this.#positions[this.#key] = offset;
  }
  #persist(): void {
    try {
      this.storage.write('session', scrollStorageKey, JSON.stringify(this.#positions));
    } catch {
      /* In-page restoration remains available when session storage cannot be written. */
    }
  }
  #publish(force: boolean): void {
    const next = path(location.href);
    if (!hasHistoryKey(history.state)) {
      history.replaceState({ ...entry(history.state), key: Math.random().toString(36).slice(2, 10) }, '');
    }
    const key = this.key;
    if (!force && next === this.#url && key === this.#key) return;
    this.#persist();
    this.#url = next;
    this.#key = key;
    this.#title();
    this.send({ type: 'navigate', url: next, action: 'POP', state: history.state, key });
  }
  #title(): void {
    const section = new URL(this.#url, location.href).pathname.split('/')[1] || 'activity';
    document.title = `${section.charAt(0).toUpperCase() + section.slice(1)} · tree-eClass`;
  }
}
