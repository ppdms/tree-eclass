import * as React from 'react';
import { requestJson } from '../app/api';

interface FolderState {
  open: boolean;
  persisted: boolean;
  revision: number;
  queue: Promise<void>;
  listeners: Set<() => void>;
}

export type FolderStates = Map<string, FolderState>;

/** The tree owns this map so collapsing a parent cannot start a second queue for a child. */
export function useFolderStates(courseId: string | number): FolderStates {
  return React.useMemo(() => new Map<string, FolderState>(), [courseId]);
}

function folderState(states: FolderStates, key: string, initiallyOpen: boolean): FolderState {
  let state = states.get(key);
  if (!state) {
    state = {
      open: initiallyOpen,
      persisted: initiallyOpen,
      revision: 0,
      queue: Promise.resolve(),
      listeners: new Set(),
    };
    states.set(key, state);
  }
  return state;
}

function notify(state: FolderState) {
  for (const listener of state.listeners) listener();
}

function persistToggle(courseId: string | number, folderKey: string, state: FolderState) {
  const next = !state.open;
  const revision = ++state.revision;
  state.open = next;
  notify(state);
  state.queue = state.queue.then(async () => {
    try {
      await requestJson(`/api/courses/${encodeURIComponent(String(courseId))}/folders/collapsed`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ folder_key: folderKey, collapsed: !next }),
      });
      state.persisted = next;
    } catch {
      if (revision === state.revision) {
        state.open = state.persisted;
        notify(state);
      }
    }
  });
}

export function useFolderOpen(
  courseId: string | number,
  folderKey: string,
  initiallyOpen: boolean,
  forceOpen: boolean,
  states: FolderStates,
) {
  const state = folderState(states, folderKey, initiallyOpen);
  const subscribe = React.useCallback(
    (listener: () => void) => {
      state.listeners.add(listener);
      return () => {
        state.listeners.delete(listener);
      };
    },
    [state],
  );
  const snapshot = React.useCallback(() => state.open, [state]);
  const open = React.useSyncExternalStore(subscribe, snapshot, snapshot);
  const toggle = () => {
    if (!forceOpen) persistToggle(courseId, folderKey, state);
  };
  return { visible: forceOpen || open, toggle };
}
