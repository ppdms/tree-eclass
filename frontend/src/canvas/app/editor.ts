import { concealControl, configureControl, controlLayoutKey, NativeFieldMetrics, placeControl } from './browserFields';
import type { NativeControl } from './browserFields';
import type { NativeFieldDescriptor, NativeHotspot, NativeValidation } from './browserProtocol';
import type { NativeEditInput, ToApp } from './protocol';
import { NativeFieldGeometry } from './nativeFieldGeometry';
import { nativeValidationMessage, showNativePicker } from './nativeFieldActions';

export class CanvasEditor {
  readonly ready: Promise<void>;
  readonly #textarea = document.createElement('textarea');
  readonly #validationInput = document.createElement('input');
  readonly #validationArea = document.createElement('textarea');
  readonly #metrics = new NativeFieldMetrics();
  readonly #geometry = new NativeFieldGeometry();
  readonly #controls = new Map<string, NativeControl>();
  readonly #owners = new WeakMap<NativeControl, string>();
  #fields = new Map<string, NativeFieldDescriptor>();
  #spots: NativeHotspot[] = [];
  #active: NativeFieldDescriptor | null = null;
  #control: NativeControl;
  #composition: [number, number] | null = null;
  #compositionStart = 0;
  #last: NativeEditInput | null = null;
  #polling = false;
  #borrowingFocus = false;
  #layoutDirty = true;
  readonly #revisions = new Map<string, number>();

  constructor(
    readonly input: HTMLInputElement,
    readonly send: (message: ToApp) => void,
    fonts: Promise<void>,
    readonly canvas: HTMLCanvasElement,
  ) {
    this.#control = input;
    concealControl(input);
    concealControl(this.#textarea);
    document.body.append(this.#textarea);
    this.#listen(input);
    this.#listen(this.#textarea);
    this.ready = fonts.then(() => this.#fontsChanged());
  }
  #fontsChanged(): void {
    this.#metrics.invalidate();
    this.#geometry.invalidate();
    this.#layoutDirty = true;
    const items = this.#metrics.measure([...this.#fields.values()]);
    if (items.length) this.send({ type: 'fieldMetrics', items });
  }
  get activeElement(): NativeControl | null {
    return this.#active ? this.#control : null;
  }
  owns(target: EventTarget | null): boolean {
    return (target instanceof HTMLInputElement || target instanceof HTMLTextAreaElement) && this.#owners.has(target);
  }
  isSensitive(id: string): boolean {
    const field = this.#fields.get(id);
    return !!field && (field.sensitive === true || field.type === 'password');
  }
  preserveNativeFocus<T>(operation: () => T): T {
    this.#borrowingFocus = true;
    try {
      return operation();
    } finally {
      this.#borrowingFocus = false;
    }
  }
  update(fields: NativeFieldDescriptor[], spots: NativeHotspot[]): void {
    // A semantic field ID may be reused by a fresh React screen. Its old native draft
    // and edit clock must not become the new screen's initial value.
    for (const field of fields) {
      const previous = this.#fields.get(field.id);
      if (previous && previous.instance !== field.instance) {
        if (this.#active?.id === field.id) this.blur();
        this.#revisions.delete(field.id);
        this.#removeControl(field.id);
      }
    }
    const previous = this.#active;
    this.#fields.clear();
    for (const field of fields) this.#fields.set(field.id, field);
    for (const id of this.#revisions.keys()) if (!this.#fields.has(id)) this.#revisions.delete(id);
    this.#spots = spots;
    const active = this.#active && this.#fields.get(this.#active.id);
    if (this.#active && (!active || active.disabled || !!active.multiline !== !!this.#active.multiline)) this.blur();
    else if (active) {
      this.#active = active;
      if (!previous || controlLayoutKey(previous) !== controlLayoutKey(active)) this.#layoutDirty = true;
      if (this.#acknowledged(active) && !this.#composition && this.#control.value !== active.value) {
        const start = this.#control.selectionStart;
        const end = this.#control.selectionEnd;
        const direction = this.#control.selectionDirection ?? 'none';
        const left = this.#control.scrollLeft;
        const top = this.#control.scrollTop;
        this.#control.value = active.value;
        if (start !== null && end !== null) this.#selection(start, end, direction);
        this.#control.scrollLeft = left;
        this.#control.scrollTop = top;
      }
    }
    this.#overlays(fields);
    const items = this.#metrics.measure(fields);
    if (items.length) this.send({ type: 'fieldMetrics', items });
  }
  activate(x: number, y: number): boolean {
    for (let index = this.#spots.length - 1; index >= 0; index--) {
      const spot = this.#spots[index]!;
      if (spot.disabled || !['text', 'focus-field'].includes(spot.kind)) continue;
      if (x < spot.x || x > spot.x + spot.width || y < spot.y || y > spot.y + spot.height) continue;
      return this.activateId(spot.fieldId ?? spot.id, true);
    }
    return false;
  }
  /** This runs inside the actual tap/key handler, without awaiting a worker message. */
  activateId(id: string, userActivation = false): boolean {
    return this.#activate(id, userActivation, false);
  }
  #activate(id: string, userActivation: boolean, nativePointer: boolean): boolean {
    const field = this.#fields.get(id);
    if (!field || field.disabled) return false;
    if (this.#active?.id === id) {
      this.#control.focus({ preventScroll: true });
      if (userActivation) showNativePicker(this.#control, field);
      return true;
    }
    this.blur();
    this.#active = field;
    this.#control = this.#ensure(field);
    configureControl(this.#control, field);
    if (this.#acknowledged(field) && this.#control.value !== field.value) this.#control.value = field.value;
    this.#place(field);
    this.#control.focus({ preventScroll: true });
    if (!nativePointer) this.#selection(field.value.length, field.value.length);
    this.#layoutDirty = true;
    if (userActivation) showNativePicker(this.#control, field);
    this.send({ type: 'fieldAction', id, instance: field.instance, action: 'focus' });
    this.#sync();
    this.#poll();
    return true;
  }
  blur(): void {
    if (this.#active) this.#control.blur();
  }
  validate(ids: readonly string[]): NativeValidation {
    const invalid: NativeValidation['invalid'] = [];
    for (const id of ids) {
      const field = this.#fields.get(id);
      if (!field) throw new Error('A native validation field is unavailable');
      const control = field.multiline ? this.#validationArea : this.#validationInput;
      const draft = this.#active?.id === id ? this.#control.value : field.value;
      const message = nativeValidationMessage(control, field, draft);
      if (message !== null) invalid.push({ id, message });
    }
    if (invalid[0]) this.activateId(invalid[0].id);
    return { valid: invalid.length === 0, invalid };
  }
  #listen(control: NativeControl): void {
    control.addEventListener('pointerdown', (event) => {
      const id = this.#owners.get(control);
      if (id && event instanceof PointerEvent && event.isPrimary && event.button === 0) this.#activate(id, false, true);
      // The trusted event's native default action places/extends the selection after synchronous focus.
    });
    control.addEventListener('focus', () => {
      const id = this.#owners.get(control);
      if (id && !this.#borrowingFocus) this.#activate(id, false, true);
    });
    for (const type of ['input', 'select', 'scroll']) {
      control.addEventListener(type, () => {
        if (control === this.#control) this.#sync();
      });
    }
    control.addEventListener('blur', () => {
      if (control === this.#control) this.#finish();
    });
    control.addEventListener('keydown', (event) => {
      if (event instanceof KeyboardEvent) this.#key(event);
    });
    control.addEventListener('compositionstart', () => {
      if (control !== this.#control || !this.#active) return;
      this.#compositionStart = control.selectionStart ?? control.value.length;
      this.#composition = [this.#compositionStart, this.#compositionStart];
      this.#sync();
    });
    control.addEventListener('compositionupdate', (event) => {
      if (control !== this.#control || !this.#active || !(event instanceof CompositionEvent)) return;
      this.#composition = [this.#compositionStart, this.#compositionStart + event.data.length];
      this.#sync();
    });
    control.addEventListener('compositionend', () => {
      if (control !== this.#control || !this.#active) return;
      this.#composition = null;
      this.#sync();
    });
  }
  #key(event: KeyboardEvent): void {
    const field = this.#active;
    if (!field || event.isComposing || this.#composition) return;
    if (event.key === 'Enter' && ['date', 'time', 'datetime-local'].includes(field.type ?? '')) {
      event.preventDefault();
      showNativePicker(this.#control, field);
      return;
    }
    if (field.type === 'number' && !field.readOnly && ['ArrowUp', 'ArrowDown'].includes(event.key)) {
      event.preventDefault();
      this.#stepNumber(field, event.key === 'ArrowUp');
      return;
    }
    const submit = field.multiline ? field.submitOnEnter === true && !event.shiftKey : field.submitOnEnter !== false;
    if (event.key === 'Enter' && submit) {
      event.preventDefault();
      this.#sync();
      this.send({ type: 'fieldAction', id: field.id, instance: field.instance, action: 'submit' });
    }
    if (event.key === 'Escape') this.blur();
    // Tab is routed by the canvas focus scope; it is never browser DOM traversal.
    if (event.key === 'Tab') {
      event.preventDefault();
      this.blur();
    }
  }
  #stepNumber(field: NativeFieldDescriptor, up: boolean): void {
    const input = this.#validationInput;
    configureControl(input, field, true);
    input.value = this.#control.value;
    try {
      if (up) input.stepUp();
      else input.stepDown();
      this.#control.value = input.value;
      this.#selection(input.value.length, input.value.length);
      this.#sync();
    } catch {
      /* Native step='any' does not define arrow stepping. */
    } finally {
      input.value = '';
    }
  }
  #selection(start: number, end: number, direction: 'forward' | 'backward' | 'none' = 'none'): void {
    if (this.#control.selectionStart === null) return;
    this.#control.setSelectionRange(start, end, direction);
  }
  #place(field: NativeFieldDescriptor): void {
    placeControl(
      this.#controls.get(field.id)!,
      field,
      this.canvas.getBoundingClientRect(),
      this.#spots.filter((item) => item.id === field.id && item.kind === 'text'),
    );
  }
  #acknowledged(field: NativeFieldDescriptor): boolean {
    return (field.editRevision ?? 0) >= (this.#revisions.get(field.id) ?? 0);
  }
  #ensure(field: NativeFieldDescriptor): NativeControl {
    const existing = this.#controls.get(field.id);
    if (existing) return existing;
    const spare = field.multiline ? this.#textarea : this.input;
    const control = !this.#owners.has(spare)
      ? spare
      : field.multiline
        ? document.createElement('textarea')
        : document.createElement('input');
    if (control !== spare) {
      concealControl(control);
      this.#listen(control);
    }
    if (!control.isConnected) document.body.append(control);
    this.#controls.set(field.id, control);
    this.#owners.set(control, field.id);
    control.value = field.value;
    return control;
  }
  #overlays(fields: readonly NativeFieldDescriptor[]): void {
    for (const [id, control] of this.#controls) {
      const field = this.#fields.get(id);
      if (field && !!field.multiline === control instanceof HTMLTextAreaElement) continue;
      this.#removeControl(id);
    }
    for (const field of fields) {
      const exposed = this.#spots.some((spot) => spot.id === field.id && spot.kind === 'text');
      if (!exposed && !this.#controls.has(field.id)) continue;
      const control = this.#ensure(field);
      configureControl(control, field);
      if (this.#active?.id !== field.id && this.#acknowledged(field) && control.value !== field.value) {
        control.value = field.value;
      }
      this.#place(field);
    }
  }
  #removeControl(id: string): void {
    const control = this.#controls.get(id);
    if (!control) return;
    this.#controls.delete(id);
    this.#owners.delete(control);
    control.value = '';
    if (control === this.input || control === this.#textarea) control.style.display = 'none';
    else control.remove();
  }
  #poll(): void {
    if (this.#polling) return;
    this.#polling = true;
    const step = () => {
      if (!this.#active) {
        this.#polling = false;
        return;
      }
      this.#sync();
      requestAnimationFrame(step);
    };
    requestAnimationFrame(step);
  }
  #sync(): void {
    const field = this.#active;
    if (!field) return;
    const control = this.#control;
    const value = control.value;
    const start = control.selectionStart;
    const end = control.selectionEnd;
    const direction = control.selectionDirection;
    const scrollLeft = control.scrollLeft;
    const scrollTop = control.scrollTop;
    const last = this.#last;
    if (
      last &&
      last.value === value &&
      last.selectionStart === start &&
      last.selectionEnd === end &&
      last.selectionDirection === direction &&
      last.composition === this.#composition &&
      last.scrollLeft === scrollLeft &&
      last.scrollTop === scrollTop &&
      !this.#layoutDirty
    )
      return;
    const geometry = this.#geometry.measure(control, field, this.#composition);
    this.#layoutDirty = false;
    const next: NativeEditInput = {
      type: 'edit',
      field: field.id,
      instance: field.instance,
      value,
      active: true,
      selectionStart: start,
      selectionEnd: end,
      selectionDirection: direction,
      composition: this.#composition,
      scrollLeft,
      scrollTop,
      geometry,
      revision: (this.#revisions.get(field.id) ?? 0) + 1,
    };
    this.#revisions.set(field.id, next.revision);
    this.#last = next;
    this.send(next);
    if (field.autoResize) {
      const items = this.#metrics.measure([{ ...field, value: control.value }], false);
      if (items.length) this.send({ type: 'fieldMetrics', items });
    }
  }
  #finish(): void {
    if (this.#borrowingFocus) return;
    const field = this.#active;
    if (!field) return;
    this.#sync();
    if (this.#last) this.send({ ...this.#last, active: false, composition: null });
    this.send({ type: 'fieldAction', id: field.id, instance: field.instance, action: 'blur' });
    this.#active = null;
    this.#composition = null;
    this.#last = null;
  }
}
