/** Stable preferences retain their existing keys; each development baseline owns
 * a separate namespace, just as its database and object writes are disposable. */
export function storageKey(key: string): string {
  const namespace = globalThis.document == null ? '' : document.documentElement.dataset.treeStorage || '';
  return namespace + key;
}
