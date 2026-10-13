import { useSyncExternalStore } from 'react';
import type { Viewport } from '../protocol';

export interface CanvasViewport extends Viewport {
  top: number;
  bottom: number;
  reducedMotion: boolean;
}

let value: CanvasViewport = { width: 1, height: 1, dpr: 1, top: 0, bottom: 1, reducedMotion: false };
const listeners = new Set<() => void>();
const snapshot = () => value;
const subscribe = (listener: () => void) => {
  listeners.add(listener);
  return () => listeners.delete(listener);
};

export function updateViewport(next: CanvasViewport): void {
  if (
    next.width === value.width &&
    next.height === value.height &&
    next.dpr === value.dpr &&
    next.top === value.top &&
    next.bottom === value.bottom &&
    next.reducedMotion === value.reducedMotion
  )
    return;
  value = next;
  for (const listener of listeners) listener();
}

export function getViewport(): CanvasViewport {
  return value;
}

export function useViewport(): CanvasViewport {
  return useSyncExternalStore(subscribe, snapshot, snapshot);
}
