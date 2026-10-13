import type { HistoryState } from './protocol';

export function isHistoryObject(state: HistoryState): state is object {
  return state !== null && typeof state === 'object';
}

export function hasHistoryKey(state: HistoryState): state is { key: string } {
  return isHistoryObject(state) && 'key' in state && typeof state.key === 'string';
}

function hasUserState(state: HistoryState): state is { usr: HistoryState } {
  // The browser's structured-clone boundary guarantees the same value domain
  // for an entry's user payload; only ownership of the wrapper is inspected here.
  return isHistoryObject(state) && 'usr' in state;
}

export function historyUserState(state: HistoryState): HistoryState {
  return hasUserState(state) ? state.usr : state;
}
