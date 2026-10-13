import { capturePointer, mirrorSelection, releasePointer } from '../../app/browser';
import type { CanvasNodePointer } from '../../ui/types';
import { hitText, lineRange, selectionPayload, wordRange } from './geometry';
import type { PageGeometry } from './geometry';
import type { Bounds, ReaderSelection } from './types';

export type ReaderPointer = CanvasNodePointer;
interface PageHit {
  geometry: PageGeometry;
  box: Bounds;
  nodeId: string;
}
interface Drag {
  page: number;
  pointer: number;
  anchor: number;
  base: [number, number];
  mode: 'char' | 'word' | 'line';
  touch: boolean;
  handle?: 'start' | 'end';
  x: number;
  y: number;
}
interface PendingTouch {
  event: ReaderPointer;
  page: number;
  at: number;
  timer: number;
}

export class ReaderSelectionController {
  selection: ReaderSelection | null = null;
  touch = false;
  readonly pages = new Map<number, PageHit>();
  readonly #documentId: string;
  readonly #changed: (selection: ReaderSelection | null) => void;
  readonly #scroll: (pointerY: number) => void;
  #drag: Drag | null = null;
  #pending: PendingTouch | null = null;
  #scrollTimer: number | undefined;
  #click = { page: 0, time: 0, x: 0, y: 0, count: 0 };
  #anchor = 0;
  #focus = 0;

  constructor(
    documentId: string,
    changed: (selection: ReaderSelection | null) => void,
    scroll: (pointerY: number) => void,
  ) {
    this.#documentId = documentId;
    this.#changed = changed;
    this.#scroll = scroll;
  }

  updatePage(page: number, geometry: PageGeometry, box: Bounds, nodeId: string): void {
    const previous = this.pages.get(page);
    this.pages.set(page, { geometry, box, nodeId });
    if (
      this.selection?.page === page &&
      (!previous ||
        previous.box.x !== box.x ||
        previous.box.y !== box.y ||
        previous.box.width !== box.width ||
        previous.box.height !== box.height)
    )
      this.#emit(page);
  }

  clear(): void {
    this.#cancelTouch();
    this.#endDrag();
    this.selection = null;
    this.touch = false;
    mirrorSelection('');
    this.#changed(null);
  }

  #emit(page: number): void {
    const hit = this.pages.get(page);
    if (!hit) return;
    this.selection = selectionPayload(this.#documentId, page, hit.geometry, this.#anchor, this.#focus, hit.box);
    const text = this.selection ? hit.geometry.text.slice(this.selection.char_start, this.selection.char_end) : '';
    mirrorSelection(text);
    this.#changed(this.selection);
  }

  #at(page: number, x: number, y: number): number | null {
    const hit = this.pages.get(page);
    if (!hit || !hit.box.width || !hit.box.height) return null;
    return hitText(
      hit.geometry,
      ((x - hit.box.x) * hit.geometry.size.width) / hit.box.width,
      ((y - hit.box.y) * hit.geometry.size.height) / hit.box.height,
    );
  }

  #range(page: number, at: number, mode: Drag['mode']): [number, number] {
    const geometry = this.pages.get(page)!.geometry;
    if (mode === 'word') return wordRange(geometry.text, at);
    if (mode === 'line') return lineRange(geometry, at);
    return [at, at];
  }

  pointer(page: number, event: ReaderPointer): boolean | void {
    if (event.phase === 'cancel') {
      this.#cancelTouch();
      this.#endDrag();
      return;
    }
    if (event.phase === 'move') {
      this.#move(page, event);
      return;
    }
    if (event.phase === 'up') {
      this.#move(page, event);
      this.#cancelTouch();
      this.#endDrag();
      return;
    }
    if (event.phase !== 'down' || event.button !== 0 || (event.modified && !event.shift)) return;
    const at = this.#at(page, event.x, event.y);
    if (at === null) return;
    if (event.touch) {
      this.#cancelTouch();
      const timer = self.setTimeout(() => this.#longPress(), 450);
      this.#pending = { event, page, at, timer };
      return;
    }
    this.#beginMouse(page, at, event);
    return true;
  }

  #beginMouse(page: number, at: number, event: ReaderPointer): void {
    const previous = this.#click;
    const repeated =
      previous.page === page &&
      event.time - previous.time < 400 &&
      Math.hypot(event.x - previous.x, event.y - previous.y) < 8;
    const count = repeated ? (previous.count % 3) + 1 : 1;
    this.#click = { page, time: event.time, x: event.x, y: event.y, count };
    const mode = event.shift ? 'char' : count === 3 ? 'line' : count === 2 ? 'word' : 'char';
    const base = this.#range(page, at, mode);
    const shift = event.shift && this.selection?.page === page;
    this.#anchor = shift ? this.#anchor : base[0];
    this.#focus = shift ? at : base[1];
    this.touch = false;
    this.#drag = {
      page,
      pointer: event.id,
      anchor: this.#anchor,
      base: shift ? [this.#anchor, this.#anchor] : base,
      mode,
      touch: false,
      x: event.x,
      y: event.y,
    };
    this.#emit(page);
    this.#startScroll();
  }

  #longPress(): void {
    const pending = this.#pending;
    if (!pending) return;
    this.#pending = null;
    const base = this.#range(pending.page, pending.at, 'word');
    this.#anchor = base[0];
    this.#focus = base[1];
    this.touch = true;
    const { event, page } = pending;
    capturePointer(this.pages.get(page)!.nodeId, event.id);
    this.#drag = { page, pointer: event.id, anchor: base[0], base, mode: 'word', touch: true, x: event.x, y: event.y };
    this.#emit(page);
    this.#startScroll();
  }

  #move(page: number, event: ReaderPointer): void {
    if (this.#pending && Math.hypot(event.x - this.#pending.event.x, event.y - this.#pending.event.y) > 8) {
      this.#cancelTouch();
    }
    const drag = this.#drag;
    if (!drag || drag.pointer !== event.id || drag.page !== page) return;
    drag.x = event.x;
    drag.y = event.y;
    this.#extend(drag);
  }

  #extend(drag: Drag): void {
    const at = this.#at(drag.page, drag.x, drag.y);
    if (at === null) return;
    const previousAnchor = this.#anchor;
    const previousFocus = this.#focus;
    if (drag.handle) {
      if (drag.handle === 'start') this.#anchor = Math.min(at, this.#focus - 1);
      else this.#focus = Math.max(at, this.#anchor + 1);
    } else {
      const range = this.#range(drag.page, at, drag.mode);
      this.#anchor = at < drag.base[0] ? drag.base[1] : drag.anchor;
      this.#focus = at < drag.base[0] ? range[0] : range[1];
    }
    if (previousAnchor !== this.#anchor || previousFocus !== this.#focus) this.#emit(drag.page);
  }

  handle(page: number, end: 'start' | 'end', event: ReaderPointer): boolean | void {
    if (event.phase === 'down' && this.selection?.page === page) {
      this.#anchor = this.selection.char_start;
      this.#focus = this.selection.char_end;
      this.#drag = {
        page,
        pointer: event.id,
        anchor: this.#anchor,
        base: [this.#anchor, this.#focus],
        mode: 'char',
        touch: true,
        handle: end,
        x: event.x,
        y: event.y,
      };
      this.#startScroll();
      return true;
    }
    return this.pointer(page, event);
  }

  #startScroll(): void {
    clearInterval(this.#scrollTimer);
    this.#scrollTimer = self.setInterval(() => {
      if (!this.#drag) return;
      this.#scroll(this.#drag.y);
      this.#extend(this.#drag);
    }, 32);
  }

  #endDrag(): void {
    if (this.#drag) releasePointer(this.#drag.pointer);
    this.#drag = null;
    clearInterval(this.#scrollTimer);
    this.#scrollTimer = undefined;
  }

  #cancelTouch(): void {
    clearTimeout(this.#pending?.timer);
    this.#pending = null;
  }

  key(key: string, shift: boolean): boolean {
    if (key === 'Escape') {
      this.clear();
      return true;
    }
    const selected = this.selection;
    if (!selected || !shift || !['ArrowLeft', 'ArrowRight', 'Home', 'End'].includes(key)) return false;
    const geometry = this.pages.get(selected.page)?.geometry;
    if (!geometry) return false;
    if (key === 'ArrowLeft') this.#focus = Math.max(0, this.#focus - 1);
    if (key === 'ArrowRight') this.#focus = Math.min(geometry.text.length, this.#focus + 1);
    if (key === 'Home') this.#focus = lineRange(geometry, this.#focus)[0];
    if (key === 'End') this.#focus = lineRange(geometry, this.#focus)[1];
    if (/[\uDC00-\uDFFF]/.test(geometry.text[this.#focus] ?? '')) {
      this.#focus += key === 'ArrowLeft' ? -1 : 1;
    }
    this.#emit(selected.page);
    return true;
  }

  destroy(): void {
    this.#cancelTouch();
    this.#endDrag();
    this.pages.clear();
    mirrorSelection('');
  }
}
