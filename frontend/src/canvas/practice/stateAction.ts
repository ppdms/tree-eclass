import type { SetStateAction } from 'react';

export function isStateUpdater<T>(action: SetStateAction<T>): action is (previous: T) => T {
  return typeof action === 'function';
}
