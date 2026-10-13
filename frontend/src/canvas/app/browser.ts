import type { Rect } from '../ui/types';
import type { StorageScope } from './preferences';
import type {
  BrowserCancel,
  BrowserOperation,
  BrowserRequest,
  BrowserResult,
  BrowserValue,
  NativeValidation,
} from './browserProtocol';

export interface ScrollToOptions {
  offsetX?: number;
  behavior?: 'auto' | 'smooth';
}
export interface BrowserPorts {
  send(message: BrowserRequest | BrowserCancel, transfer?: Transferable[]): void;
  scrollPageTo(offset: number, options?: ScrollToOptions): void;
  scrollAreaTo(id: string, offset: number, options?: ScrollToOptions): void;
  scrollToNode(id: string): void;
  getNodeRect(id: string): Rect | null;
  focusNode(id: string): void;
  focusField(id: string): void;
  capturePointer(nodeId: string, pointerId: number): void;
  releasePointer(pointerId: number): void;
  error?(error: Error): void;
}
interface Pending {
  kind: BrowserValue['kind'];
  resolve(value: BrowserValue): void;
  reject(error: Error): void;
  cleanup?(): void;
}
let ports: BrowserPorts | undefined;
let sequence = 0;
const pending = new Map<number, Pending>();

export function configureBrowser(next: BrowserPorts): void {
  if (ports && ports !== next) {
    for (const [id, item] of pending) {
      ports.send({ type: 'browserCancel', id });
      item.cleanup?.();
      item.reject(new Error('Browser bridge was replaced'));
    }
    pending.clear();
  }
  ports = next;
}
function configured(): BrowserPorts {
  if (!ports) throw new Error('Browser bridge is not initialized');
  return ports;
}
function request(
  operation: BrowserOperation,
  kind: BrowserValue['kind'] = 'void',
  signal?: AbortSignal,
  transfer?: Transferable[],
): Promise<BrowserValue> {
  const bridge = configured();
  if (signal?.aborted) return Promise.reject(new DOMException('Image decoding aborted', 'AbortError'));
  const id = ++sequence;
  const { promise, resolve, reject } = Promise.withResolvers<BrowserValue>();
  const abort = () => {
    pending.delete(id);
    signal?.removeEventListener('abort', abort);
    try {
      bridge.send({ type: 'browserCancel', id });
    } finally {
      reject(new DOMException('Image decoding aborted', 'AbortError'));
    }
  };
  signal?.addEventListener('abort', abort, { once: true });
  pending.set(id, { kind, resolve, reject, cleanup: () => signal?.removeEventListener('abort', abort) });
  try {
    bridge.send({ type: 'browserRequest', id, operation }, transfer);
  } catch (error) {
    pending.delete(id);
    signal?.removeEventListener('abort', abort);
    reject(error instanceof Error ? error : new Error('Browser request could not be sent'));
  }
  return promise;
}
export function completeBrowserResult(message: BrowserResult): void {
  const item = pending.get(message.id);
  if (!item) {
    if (message.result.kind === 'image') message.result.bitmap.close();
    return;
  }
  pending.delete(message.id);
  item.cleanup?.();
  if (message.result.kind === 'error') item.reject(new Error(message.result.message));
  else if (message.result.kind !== item.kind) {
    if (message.result.kind === 'image') message.result.bitmap.close();
    item.reject(new Error('Unexpected browser bridge result kind'));
  } else item.resolve(message.result);
}
function notify(operation: BrowserOperation): void {
  void request(operation).catch((error: Error) => {
    if (ports?.error) ports.error(error);
    else
      queueMicrotask(() => {
        throw error;
      });
  });
}
export function downloadBlob(blob: Blob, filename: string): void {
  notify({ kind: 'downloadBlob', blob, filename });
}
export async function copyText(value: string): Promise<void> {
  await request({ kind: 'copyText', value });
}
export interface DecodeImageOptions {
  width?: number;
  height?: number;
  signal?: AbortSignal;
}
export async function decodeImage(source: string | Blob, options: DecodeImageOptions = {}): Promise<ImageBitmap> {
  const result = await request(
    { kind: 'decodeImage', source, width: options.width, height: options.height },
    'image',
    options.signal,
  );
  if (result.kind !== 'image') throw new Error('Expected a decoded native image');
  return result.bitmap;
}
export function reloadPage(): void {
  notify({ kind: 'reloadPage' });
}
export function setUnloadGuard(id: string, enabled: boolean): void {
  notify({ kind: 'setUnloadGuard', id, enabled });
}
export function mirrorSelection(text: string): void {
  notify({ kind: 'mirrorSelection', text });
}
export async function validateNativeFields(ids: readonly string[]): Promise<NativeValidation> {
  const result = await request({ kind: 'validateNativeFields', ids: [...ids] }, 'validation');
  if (result.kind !== 'validation') throw new Error('Expected native validation');
  return result.value;
}
export async function writeBrowserStorage(scope: StorageScope, key: string, value: string | null): Promise<void> {
  await request({ kind: 'writeStorage', scope, key, value });
}
export function scrollPageTo(offset: number, options?: ScrollToOptions): void {
  configured().scrollPageTo(offset, options);
}
export function scrollAreaTo(id: string, offset: number, options?: ScrollToOptions): void {
  configured().scrollAreaTo(id, offset, options);
}
export function scrollToNode(id: string): void {
  configured().scrollToNode(id);
}
export function getNodeRect(id: string): Rect | null {
  return configured().getNodeRect(id);
}
export function focusNode(id: string): void {
  configured().focusNode(id);
}
export function focusField(id: string): void {
  configured().focusField(id);
}
export function capturePointer(nodeId: string, pointerId: number): void {
  configured().capturePointer(nodeId, pointerId);
}
export function releasePointer(pointerId: number): void {
  configured().releasePointer(pointerId);
}
