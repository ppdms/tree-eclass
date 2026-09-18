import { expect, test } from 'bun:test';
import { storageKey } from '../../src/lib/browserStorage';

test('development reader and activity writes do not change stable browser data', () => {
  const previous = Object.getOwnPropertyDescriptor(globalThis, 'document');
  const dataset = { treeStorage: '' };
  Object.defineProperty(globalThis, 'document', { configurable: true, value: { documentElement: { dataset } } });
  try {
    const storage = new Map<string, string>();
    const key = 'tree-eclass:study:reader:synthetic-document';
    storage.set(storageKey(key), 'stable-position');
    dataset.treeStorage = 'tree-eclass:development:baseline-one:';
    storage.set(storageKey(key), 'development-position');
    expect(storage.get(storageKey(key))).toBe('development-position');
    dataset.treeStorage = '';
    expect(storage.get(storageKey(key))).toBe('stable-position');
    dataset.treeStorage = 'tree-eclass:development:baseline-two:';
    expect(storage.get(storageKey(key))).toBeUndefined();
  } finally {
    if (previous) Object.defineProperty(globalThis, 'document', previous);
    else Reflect.deleteProperty(globalThis, 'document');
  }
});
