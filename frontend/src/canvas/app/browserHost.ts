import type { BrowserRequest, BrowserValue } from './browserProtocol';
import type { ToApp } from './protocol';
import type { CanvasEditor } from './editor';
import type { BrowserClipboardHost } from './browserClipboard';
import type { BrowserHistoryHost } from './browserHistory';
import type { BrowserStorageHost } from './browserStorage';
import { BrowserImageHost } from './browserImages';

export class BrowserHost {
  readonly #images = new BrowserImageHost();
  readonly #unloadGuards = new Set<string>();
  readonly #decoding = new Map<number, AbortController>();

  constructor(
    readonly editor: CanvasEditor,
    readonly clipboard: BrowserClipboardHost,
    readonly history: BrowserHistoryHost,
    readonly storage: BrowserStorageHost,
    readonly send: (message: ToApp, transfer?: Transferable[]) => void,
  ) {
    window.addEventListener('beforeunload', (event) => {
      if (!this.#unloadGuards.size) return;
      event.preventDefault();
      event.returnValue = '';
    });
  }
  receive(message: BrowserRequest): void {
    const controller = message.operation.kind === 'decodeImage' ? new AbortController() : undefined;
    if (controller) this.#decoding.set(message.id, controller);
    void this.#execute(message, controller?.signal)
      .then(
        (result) => {
          try {
            this.send(
              { type: 'browserResult', id: message.id, result },
              result.kind === 'image' ? [result.bitmap] : [],
            );
          } catch {
            if (result.kind === 'image') result.bitmap.close();
            this.send({
              type: 'browserResult',
              id: message.id,
              result: { kind: 'error', message: 'Native browser result could not be transferred' },
            });
          }
        },
        () =>
          this.send({
            type: 'browserResult',
            id: message.id,
            result: { kind: 'error', message: failureMessage(message.operation.kind) },
          }),
      )
      .finally(() => this.#decoding.delete(message.id));
  }
  cancel(id: number): void {
    this.#decoding.get(id)?.abort();
  }
  async #execute(request: BrowserRequest, signal?: AbortSignal): Promise<BrowserValue> {
    const operation = request.operation;
    switch (operation.kind) {
      case 'decodeImage':
        return {
          kind: 'image',
          bitmap: await this.#images.decode(operation.source, operation.width, operation.height, signal),
        };
      case 'copyText':
        await this.clipboard.copy(operation.value);
        break;
      case 'downloadBlob':
        download(operation.blob, operation.filename);
        break;
      case 'reloadPage':
        window.setTimeout(() => location.reload(), 0);
        break;
      case 'setUnloadGuard':
        if (operation.enabled) this.#unloadGuards.add(operation.id);
        else this.#unloadGuards.delete(operation.id);
        break;
      case 'mirrorSelection':
        this.clipboard.mirror(operation.text);
        break;
      case 'validateNativeFields':
        return { kind: 'validation', value: this.editor.validate(operation.ids) };
      case 'writeStorage':
        this.storage.write(operation.scope, operation.key, operation.value);
        break;
    }
    return { kind: 'void' };
  }
}
function download(blob: Blob, filename: string): void {
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement('a');
  anchor.href = url;
  anchor.download = filename;
  document.body.append(anchor);
  try {
    anchor.click();
  } finally {
    anchor.remove();
    window.setTimeout(() => URL.revokeObjectURL(url), 60_000);
  }
}
function failureMessage(kind: BrowserRequest['operation']['kind']): string {
  // Native engine diagnostics can contain source URLs or values; never forward them.
  const descriptions = {
    decodeImage: 'Native image decoding failed',
    copyText: 'Native clipboard write failed',
    downloadBlob: 'Native download failed',
    reloadPage: 'Native reload failed',
    setUnloadGuard: 'Native unload guard update failed',
    mirrorSelection: 'Native selection update failed',
    validateNativeFields: 'Native field validation failed',
    writeStorage: 'Browser storage write failed',
  } satisfies Record<BrowserRequest['operation']['kind'], string>;
  return descriptions[kind];
}
