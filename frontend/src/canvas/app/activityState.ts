export interface ActivityEvent {
  kind: 'activity' | 'visibility' | 'pagehide';
  at: number;
  visible: boolean;
}

let state = { visible: true, lastInput: 0 };
const listeners = new Set<(event: ActivityEvent) => void>();

export function getActivity(): Readonly<typeof state> {
  return state;
}

export function receiveActivity(event: ActivityEvent): void {
  state = { visible: event.visible, lastInput: event.kind === 'activity' ? event.at : state.lastInput };
  for (const listener of listeners) listener(event);
}

export function subscribeActivity(listener: (event: ActivityEvent) => void): () => void {
  listeners.add(listener);
  return () => listeners.delete(listener);
}
