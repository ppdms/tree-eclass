import type { CanvasEditor } from './editor';
import { concealControl } from './browserFields';

export class BrowserClipboardHost {
  readonly #copy = document.createElement('textarea');
  readonly #selection = document.createElement('span');
  #text = '';
  #copying = false;

  constructor(readonly editor: CanvasEditor) {
    concealControl(this.#copy);
    Object.assign(this.#copy.style, {
      display: 'block',
      width: '1px',
      height: '1px',
      left: '0',
      top: '0',
      pointerEvents: 'none',
    });
    Object.assign(this.#selection.style, {
      position: 'fixed',
      left: '0',
      top: '0',
      opacity: '0',
      pointerEvents: 'none',
      whiteSpace: 'pre-wrap',
      userSelect: 'text',
      zIndex: '-1',
    });
    document.body.append(this.#copy, this.#selection);
    document.addEventListener('copy', (event) => {
      if (!event.clipboardData || (!this.#copying && this.editor.activeElement)) return;
      const value = this.#copying ? this.#copy.value : this.#text;
      if (!value) return;
      event.clipboardData.setData('text/plain', value);
      event.preventDefault();
    });
  }
  mirror(text: string): void {
    if (text === this.#text) return;
    this.#text = text;
    this.#selection.textContent = text;
    if (this.editor.activeElement) return;
    const selection = window.getSelection();
    if (!selection) return;
    selection.removeAllRanges();
    if (!text) return;
    const range = document.createRange();
    range.selectNodeContents(this.#selection);
    selection.addRange(range);
  }
  async copy(value: string): Promise<void> {
    if (this.editor.preserveNativeFocus(() => this.#legacyCopy(value))) return;
    if (!navigator.clipboard?.writeText) throw new Error('Native clipboard is unavailable');
    try {
      await navigator.clipboard.writeText(value);
    } catch {
      throw new Error('Native clipboard write failed');
    }
  }
  #legacyCopy(value: string): boolean {
    const active = document.activeElement instanceof HTMLElement ? document.activeElement : null;
    const editor = this.editor.activeElement;
    const start = editor?.selectionStart;
    const end = editor?.selectionEnd;
    const direction = editor?.selectionDirection ?? 'none';
    const selection = window.getSelection();
    const ranges = selection
      ? Array.from({ length: selection.rangeCount }, (_, index) => selection.getRangeAt(index).cloneRange())
      : [];
    this.#copying = true;
    this.#copy.value = value;
    this.#copy.focus({ preventScroll: true });
    this.#copy.select();
    let copied = false;
    try {
      copied = document.execCommand('copy');
    } catch {
      copied = false;
    } finally {
      this.#copying = false;
      this.#copy.value = '';
      active?.focus({ preventScroll: true });
      if (editor && start !== null && start !== undefined && end !== null && end !== undefined) {
        editor.setSelectionRange(start, end, direction);
      }
      selection?.removeAllRanges();
      for (const range of ranges) selection?.addRange(range);
    }
    return copied;
  }
}
