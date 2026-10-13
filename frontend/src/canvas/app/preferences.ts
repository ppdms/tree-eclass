import { useMemo, useSyncExternalStore } from 'react';
import type { JsonValue } from '@/lib/display';

export type StorageScope = 'local' | 'session';
export interface StorageSnapshot {
  local: Record<string, string>;
  session: Record<string, string>;
}
export type PreferenceWrite = (scope: StorageScope, key: string, value: string | null) => Promise<void>;
export type PreferenceParser<T> = (value: JsonValue) => T;

const values = { local: new Map<string, string>(), session: new Map<string, string>() };
const listeners = new Map<string, Set<() => void>>();
let publish: PreferenceWrite | undefined;

export function configurePreferences(snapshot: StorageSnapshot, write: PreferenceWrite): void {
  for (const scope of ['local', 'session'] as const) {
    values[scope].clear();
    for (const [key, value] of Object.entries(snapshot[scope])) values[scope].set(key, value);
  }
  publish = write;
}

export function receivePreference(scope: StorageScope, key: string, value: string | null): void {
  if ((values[scope].get(key) ?? null) === value) return;
  if (value === null) values[scope].delete(key);
  else values[scope].set(key, value);
  for (const listener of listeners.get(`${scope}:${key}`) ?? []) listener();
}

function subscribe(scope: StorageScope, key: string, listener: () => void): () => void {
  const id = `${scope}:${key}`;
  let subscribed = listeners.get(id);
  if (!subscribed) listeners.set(id, (subscribed = new Set()));
  subscribed.add(listener);
  return () => {
    subscribed.delete(listener);
    if (!subscribed.size) listeners.delete(id);
  };
}

function decode<T>(value: string | null, initial: T, parse: PreferenceParser<T>): T {
  if (value === null) return initial;
  let parsed: JsonValue;
  try {
    // Existing browser preferences use both JSON records and unquoted scalar strings.
    parsed = JSON.parse(value);
  } catch {
    parsed = value;
  }
  return parse(parsed);
}

export function readPreference<T>(key: string, initial: T, parse: PreferenceParser<T>): T {
  return decode(values.local.get(key) ?? null, initial, parse);
}

/** Missing values use initial; parsers own invalid-value fallbacks and may throw.
 * Stable parsers avoid decoding again on an otherwise unchanged component render. */
export function usePreference<T>(key: string, initial: T, parse: PreferenceParser<T>): [T, (value: T) => void] {
  const raw = useSyncExternalStore(
    (listener) => subscribe('local', key, listener),
    () => values.local.get(key) ?? null,
  );
  const value = useMemo(() => decode(raw, initial, parse), [raw, initial, parse]);
  return [value, (next) => setPreference(key, next)];
}

function isScalarPreference<T>(value: T): value is T & string {
  return typeof value === 'string';
}

export function setPreference<T>(key: string, value: T): void {
  const encoded = isScalarPreference(value) ? value : JSON.stringify(value);
  if (encoded === undefined) throw new Error('A browser preference must be JSON-serializable');
  receivePreference('local', key, encoded);
  if (!publish) throw new Error('Browser preferences are not initialized');
  // Local preferences remain usable when persistence is unavailable, as in private mode.
  void publish('local', key, encoded).catch(() => undefined);
}

export function sessionValue(key: string): string | null {
  return values.session.get(key) ?? null;
}

/** Sequence/key writes await main-thread storage before a resumable ledger mutation. */
export async function setSessionValue(key: string, value: string | null): Promise<void> {
  if (!publish) throw new Error('Browser preferences are not initialized');
  await publish('session', key, value);
  receivePreference('session', key, value);
}
