import type { NativeControl } from './browserFields';
import type { NativeFieldDescriptor } from './browserProtocol';
import type { NativeEditInput } from './protocol';
import { controlLayoutKey } from './browserFields';

type Geometry = NonNullable<NativeEditInput['geometry']>;
type Rect = NonNullable<Geometry['caret']>;
const typography = [
  'font-family',
  'font-size',
  'font-weight',
  'font-style',
  'font-stretch',
  'font-kerning',
  'font-feature-settings',
  'font-variation-settings',
  'font-optical-sizing',
  'font-variant',
  'line-height',
  'letter-spacing',
  'word-spacing',
  'text-align',
  'text-indent',
  'text-transform',
  'direction',
  'tab-size',
  'word-break',
  'overflow-wrap',
] as const;

/**
 * Native controls expose selection offsets, not their internal text rectangles. This browser-layout
 * mirror measures those offsets; it never handles editing, hit testing, or grapheme segmentation.
 * DOM Range cannot expose the control's caret affinity at a bidi or soft-wrap boundary.
 */
export class NativeFieldGeometry {
  readonly #mirror = document.createElement('div');
  readonly #text = document.createTextNode('');
  readonly #range = document.createRange();
  #key = '';
  #value: Geometry | null = null;

  constructor() {
    Object.assign(this.#mirror.style, {
      position: 'fixed',
      left: '-100000px',
      top: '0',
      visibility: 'hidden',
      pointerEvents: 'none',
      margin: '0',
      padding: '0',
      border: '0',
      boxSizing: 'content-box',
    });
    this.#mirror.setAttribute('aria-hidden', 'true');
    this.#mirror.append(this.#text);
    document.body.append(this.#mirror);
  }

  invalidate(): void {
    this.#key = '';
  }

  measure(control: NativeControl, field: NativeFieldDescriptor, composition: [number, number] | null): Geometry {
    const value = field.sensitive || field.type === 'password' ? '•'.repeat(control.value.length) : control.value;
    const start = control.selectionStart;
    const end = control.selectionEnd;
    const key = JSON.stringify([value, start, end, composition, controlLayoutKey(field)]);
    if (key === this.#key && this.#value) return this.#value;
    const style = getComputedStyle(control);
    for (const property of typography) this.#mirror.style.setProperty(property, style.getPropertyValue(property));
    const contentX = Number.parseFloat(style.paddingLeft) || 0;
    const paddingRight = Number.parseFloat(style.paddingRight) || 0;
    const paddingTop = Number.parseFloat(style.paddingTop) || 0;
    const paddingBottom = Number.parseFloat(style.paddingBottom) || 0;
    const lineHeight = Number.parseFloat(style.lineHeight) || field.fontSize * 1.2;
    const contentY = field.multiline
      ? paddingTop
      : paddingTop + (control.clientHeight - paddingTop - paddingBottom - lineHeight) / 2;
    const contentWidth = Math.max(0, control.clientWidth - contentX - paddingRight);
    Object.assign(this.#mirror.style, {
      width: `${contentWidth}px`,
      whiteSpace: field.multiline ? 'pre-wrap' : 'pre',
      overflowWrap: field.multiline ? 'break-word' : 'normal',
    });
    // A zero-width terminal glyph gives empty/trailing-newline lines a measurable browser line box.
    this.#text.data = value === '' || value.endsWith('\n') ? `${value}\u200b` : value;
    const origin = this.#mirror.getBoundingClientRect();
    const offset = { x: contentX - origin.x, y: contentY - origin.y };
    this.#value = {
      caret: start !== null && start === end ? this.#caret(start, offset) : null,
      selection: start !== null && end !== null ? this.#rects(start, end, offset) : [],
      composition: composition ? this.#rects(composition[0], composition[1], offset) : [],
      contentX,
      contentY,
      contentWidth,
      contentHeight: control.clientHeight,
    };
    this.#key = key;
    return this.#value;
  }

  #setRange(start: number, end: number): void {
    const length = this.#text.length;
    this.#range.setStart(this.#text, Math.max(0, Math.min(start, length)));
    this.#range.setEnd(this.#text, Math.max(0, Math.min(end, length)));
  }

  #rects(start: number, end: number, offset: { x: number; y: number }): Rect[] {
    if (start === end) return [];
    this.#setRange(start, end);
    return [...this.#range.getClientRects()].map((rect) => this.#rect(rect, offset));
  }

  #caret(position: number, offset: { x: number; y: number }): Rect | null {
    this.#setRange(position, position);
    const rect = this.#range.getClientRects()[0];
    return rect && rect.height > 0 ? { ...this.#rect(rect, offset), width: 1.5 } : null;
  }

  #rect(rect: DOMRect, offset: { x: number; y: number }): Rect {
    return { x: rect.x + offset.x, y: rect.y + offset.y, width: rect.width, height: rect.height };
  }
}
