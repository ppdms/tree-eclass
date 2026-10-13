import type { NativeFieldDescriptor } from './browserProtocol';

export type NativeControl = HTMLInputElement | HTMLTextAreaElement;
// The canvas faces' bytes under their own names; missing glyphs use the same system fallback.
const interFamily = "'Inter Spike', sans-serif";
const monoFamily = "'Roboto Mono Spike', monospace";

export function concealControl(control: NativeControl): void {
  Object.assign(control.style, {
    position: 'fixed',
    display: 'none',
    boxSizing: 'border-box',
    margin: '0',
    border: '0',
    outline: '0',
    background: 'transparent',
    color: 'transparent',
    caretColor: 'transparent',
    webkitTextFillColor: 'transparent',
    fontFamily: "'Inter Spike', sans-serif",
    opacity: '0',
    resize: 'none',
    overflow: 'auto',
    zIndex: '1',
    appearance: 'none',
  });
  control.tabIndex = -1;
}
export function configureControl(control: NativeControl, field: NativeFieldDescriptor, validation = false): void {
  if (control instanceof HTMLInputElement) {
    const type = field.type ?? 'text';
    const nativeType =
      !validation && field.sensitive
        ? 'password'
        : !validation && (type === 'email' || type === 'number')
          ? 'text'
          : type;
    if (control.type !== nativeType) control.type = nativeType;
  } else control.rows = field.rows ?? 3;
  control.inputMode =
    field.type === 'number'
      ? 'decimal'
      : field.type === 'email'
        ? 'email'
        : field.type === 'url'
          ? 'url'
          : field.type === 'search'
            ? 'search'
            : 'text';
  control.placeholder = field.placeholder;
  control.required = field.required ?? false;
  control.readOnly = field.readOnly ?? false;
  control.disabled = field.disabled ?? false;
  control.setCustomValidity('');
  control.setAttribute('autocomplete', field.autoComplete ?? 'off');
  for (const key of ['min', 'max', 'step', 'maxlength'] as const) {
    const value = key === 'maxlength' ? field.maxLength : field[key];
    if (value === undefined) control.removeAttribute(key);
    else control.setAttribute(key, String(value));
  }
  control.spellcheck = !(field.sensitive || field.type === 'password');
  if (field.sensitive || field.type === 'password') control.setAttribute('autocorrect', 'off');
  else control.removeAttribute('autocorrect');
  Object.assign(control.style, {
    fontFamily: field.mono ? monoFamily : interFamily,
    fontSize: `${field.fontSize}px`,
    fontWeight: String(field.fontWeight ?? 400),
    lineHeight: `${field.lineHeight ?? field.fontSize * 1.2}px`,
    paddingLeft: `${field.inset}px`,
    paddingRight: `${field.paddingRight ?? field.inset}px`,
    paddingTop: `${field.paddingY ?? 0}px`,
    paddingBottom: `${field.paddingBottom ?? field.paddingY ?? 0}px`,
    textAlign: field.textAlign ?? 'left',
  });
}

export function controlLayoutKey(field: NativeFieldDescriptor): string {
  return JSON.stringify([
    field.width,
    field.height,
    field.fontSize,
    field.fontWeight,
    field.lineHeight,
    field.inset,
    field.paddingY,
    field.paddingRight,
    field.paddingBottom,
    field.textAlign,
    field.rows,
    field.mono,
    field.multiline,
    field.type,
    field.sensitive,
  ]);
}

/** Keep the native hit target's clipping identical to the worker's exposed hotspot pieces. */
export function placeControl(
  control: NativeControl,
  field: NativeFieldDescriptor,
  canvas: DOMRect,
  spots: readonly { x: number; y: number; width: number; height: number }[],
): void {
  Object.assign(control.style, {
    left: `${canvas.left + field.x}px`,
    top: `${canvas.top + field.y}px`,
    width: `${Math.max(1, field.width)}px`,
    height: `${Math.max(1, field.height)}px`,
    display: 'block',
    pointerEvents: spots.length && !field.disabled ? 'auto' : 'none',
  });
  const paths = spots.map((spot) => {
    const x = Math.max(0, spot.x - field.x);
    const y = Math.max(0, spot.y - field.y);
    const right = Math.min(field.width, spot.x + spot.width - field.x);
    const bottom = Math.min(field.height, spot.y + spot.height - field.y);
    return `M${x} ${y}H${right}V${bottom}H${x}Z`;
  });
  control.style.clipPath = paths.length ? `path('${paths.join(' ')}')` : 'inset(100%)';
}

export class NativeFieldMetrics {
  readonly #measure = document.createElement('textarea');
  readonly #cache = new Map<string, { key: string; scrollHeight: number; clientHeight: number }>();

  constructor() {
    concealControl(this.#measure);
    Object.assign(this.#measure.style, { display: 'block', left: '-100000px', top: '0', pointerEvents: 'none' });
    document.body.append(this.#measure);
  }
  invalidate(): void {
    this.#cache.clear();
  }
  measure(
    fields: readonly NativeFieldDescriptor[],
    retain = true,
  ): { id: string; instance: number; scrollHeight: number; clientHeight: number }[] {
    const items: { id: string; instance: number; scrollHeight: number; clientHeight: number }[] = [];
    const current = new Set<string>();
    for (const field of fields) {
      if (!field.multiline || !field.autoResize) continue;
      current.add(field.id);
      const value = field.sensitive || field.type === 'password' ? '•'.repeat(field.value.length) : field.value;
      const key = JSON.stringify([
        field.instance,
        value,
        field.width,
        field.height,
        field.fontSize,
        field.fontWeight,
        field.lineHeight,
        field.inset,
        field.paddingY,
        field.paddingRight,
        field.paddingBottom,
        field.textAlign,
        field.rows,
        field.mono,
      ]);
      if (this.#cache.get(field.id)?.key === key) continue;
      configureControl(this.#measure, field);
      this.#measure.style.width = `${Math.max(1, field.width)}px`;
      this.#measure.style.height = '0px';
      this.#measure.value = value;
      const scrollHeight = this.#measure.scrollHeight;
      this.#measure.style.height = `${Math.max(1, field.height)}px`;
      const clientHeight = this.#measure.clientHeight;
      this.#cache.set(field.id, { key, scrollHeight, clientHeight });
      items.push({ id: field.id, instance: field.instance, scrollHeight, clientHeight });
    }
    this.#measure.value = '';
    if (retain) for (const id of this.#cache.keys()) if (!current.has(id)) this.#cache.delete(id);
    return items;
  }
}
