import { storageKey } from '@/lib/browserStorage';
import type { StorageScope, StorageSnapshot } from './preferences';
import type { ToApp } from './protocol';
import { scrollStorageKey } from './scrollHistory';

const localKeys = {
  'treeeclass:activity-read': true,
  'ask:model': true,
  'tree-eclass:study:show-controls': true,
  'treeEclass.theme': true,
};
function allowed(scope: StorageScope, key: string): boolean {
  return scope === 'local'
    ? Object.hasOwn(localKeys, key) || key.startsWith('tree-eclass:study:reader:')
    : key === scrollStorageKey || key.startsWith('study-session:') || key.startsWith('study-session-ledger:');
}
export function parseReadIds(value: string | null): (string | number)[] {
  try {
    const parsed: unknown = JSON.parse(value ?? '[]');
    if (!Array.isArray(parsed)) return [];
    if (
      !parsed.every(
        (id): id is string | number => typeof id === 'string' || (typeof id === 'number' && Number.isFinite(id)),
      )
    )
      return [];
    return parsed;
  } catch {
    return [];
  }
}

export class BrowserStorageHost {
  readonly #namespace = storageKey('');
  readonly #storage: Record<StorageScope, Storage | null>;
  readonly #snapshot: StorageSnapshot = { local: {}, session: {} };

  constructor(readonly send: (message: ToApp) => void) {
    this.#storage = { local: this.#capture('local'), session: this.#capture('session') };
    this.#clearDevelopmentStorage();
    this.#collect('local');
    this.#collect('session');
    window.addEventListener('storage', (event) => this.#changed(event));
  }
  get snapshot(): StorageSnapshot {
    return structuredClone(this.#snapshot);
  }
  read(scope: StorageScope, key: string): string | null {
    return this.#snapshot[scope][key] ?? null;
  }
  write(scope: StorageScope, key: string, value: string | null): void {
    if (!allowed(scope, key)) throw new Error('Browser storage key is not allowed');
    const target = this.#storage[scope];
    if (!target) throw new Error('Browser storage is unavailable');
    try {
      if (value === null) target.removeItem(this.#namespace + key);
      else target.setItem(this.#namespace + key, value);
    } catch {
      throw new Error('Browser storage write failed');
    }
    this.#notify(scope, key, value);
  }
  #capture(scope: StorageScope): Storage | null {
    try {
      return scope === 'local' ? window.localStorage : window.sessionStorage;
    } catch {
      return null;
    }
  }
  #clearDevelopmentStorage(): void {
    for (const target of Object.values(this.#storage)) {
      if (!target) continue;
      try {
        for (let index = target.length - 1; index >= 0; index--) {
          const key = target.key(index);
          if (key?.startsWith('tree-eclass:development:') && (!this.#namespace || !key.startsWith(this.#namespace)))
            target.removeItem(key);
        }
      } catch {
        /* Privacy settings may prevent cleanup without disabling the other storage scope. */
      }
    }
  }
  #collect(scope: StorageScope): void {
    const target = this.#storage[scope];
    if (!target) return;
    try {
      for (let index = 0; index < target.length; index++) {
        const storedKey = target.key(index);
        if (storedKey === null || !storedKey.startsWith(this.#namespace)) continue;
        const key = storedKey.slice(this.#namespace.length);
        if (!allowed(scope, key)) continue;
        const value = target.getItem(storedKey);
        if (value !== null) this.#snapshot[scope][key] = value;
      }
    } catch {
      /* An unavailable read leaves only the already captured allowlisted values. */
    }
  }
  #notify(scope: StorageScope, key: string, value: string | null): void {
    if ((this.#snapshot[scope][key] ?? null) === value) return;
    if (value === null) delete this.#snapshot[scope][key];
    else this.#snapshot[scope][key] = value;
    this.send({ type: 'storage', scope, key, value });
    if (scope === 'local' && key === 'treeeclass:activity-read') {
      this.send({ type: 'readIds', ids: parseReadIds(value) });
    }
  }
  #changed(event: StorageEvent): void {
    const scope =
      event.storageArea === this.#storage.local
        ? 'local'
        : event.storageArea === this.#storage.session
          ? 'session'
          : null;
    if (!scope) return;
    if (event.key === null) {
      const previous = this.#snapshot[scope];
      this.#snapshot[scope] = {};
      this.#collect(scope);
      const next = this.#snapshot[scope];
      this.#snapshot[scope] = previous;
      for (const key of new Set([...Object.keys(previous), ...Object.keys(next)])) {
        this.#notify(scope, key, next[key] ?? null);
      }
      return;
    }
    if (!event.key.startsWith(this.#namespace)) return;
    const key = event.key.slice(this.#namespace.length);
    if (allowed(scope, key)) this.#notify(scope, key, event.newValue);
  }
}
