import { useSyncExternalStore } from 'react';

let stale = false;
const listeners = new Set<() => void>();

export function markRuntimeChanged(): void {
  if (stale) return;
  stale = true;
  for (const listener of listeners) listener();
}

export function useRuntimeChanged(): boolean {
  return useSyncExternalStore(
    (listener) => {
      listeners.add(listener);
      return () => listeners.delete(listener);
    },
    () => stale,
  );
}
