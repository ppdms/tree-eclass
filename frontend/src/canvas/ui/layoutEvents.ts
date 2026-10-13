import { useEffect, useState } from 'react';
import { getNodeRect } from '../app/browser';
import type { Rect } from './types';

const listeners = new Set<() => void>();

export function notifyLayoutObservers(): void {
  for (const listener of listeners) listener();
}

function equal(a: Rect | null, b: Rect | null): boolean {
  return a === b || Boolean(a && b && a.x === b.x && a.y === b.y && a.width === b.width && a.height === b.height);
}

export function useNodeRect(id: string): Rect | null {
  const [rect, setRect] = useState<Rect | null>(() => getNodeRect(id));
  useEffect(() => {
    if (!id) return;
    const update = () => {
      const next = getNodeRect(id);
      setRect((previous) => (equal(previous, next) ? previous : next));
    };
    listeners.add(update);
    update();
    return () => {
      listeners.delete(update);
    };
  }, [id]);
  return rect;
}
