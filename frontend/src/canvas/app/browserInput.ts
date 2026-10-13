import type { CanvasEditor } from './editor';
import type { BrowserActivationHost } from './browserActivation';
import type { ToApp } from './protocol';

type Point = { x: number; y: number };
type Press = Point & { id: number; active: boolean; dragged: boolean; identity: string | null };
export class BrowserInputHost {
  #press: Press | null = null;
  constructor(
    readonly canvas: HTMLCanvasElement,
    readonly editor: CanvasEditor,
    readonly activation: BrowserActivationHost,
    readonly send: (message: ToApp) => void,
  ) {}
  track(): void {
    this.canvas.addEventListener('pointerdown', (event) => this.#down(event));
    this.canvas.addEventListener('pointermove', (event) => this.#move(event));
    this.canvas.addEventListener('pointerup', (event) => this.#up(event));
    this.canvas.addEventListener('pointercancel', (event) => this.#cancel(event));
    this.canvas.addEventListener('lostpointercapture', (event) => this.#cancel(event));
    this.canvas.addEventListener('click', (event) => this.#click(event));
    this.canvas.addEventListener('auxclick', (event) => this.#click(event));
    this.canvas.addEventListener('contextmenu', (event) => {
      event.preventDefault();
      this.send({ type: 'contextMenu', ...this.#local(event) });
    });
    document.addEventListener('wheel', (event) => this.#wheel(event), { passive: false });
    document.addEventListener('keydown', (event) => this.#key(event));
    window.addEventListener('blur', () => {
      const press = this.#press;
      if (press?.active)
        this.send({
          type: 'pointer',
          phase: 'cancel',
          id: press.id,
          x: press.x,
          y: press.y,
          time: performance.timeOrigin + performance.now(),
          touch: false,
          shift: false,
          modified: false,
          button: 0,
        });
      this.#press = null;
    });
  }
  #local(event: MouseEvent): Point {
    const bounds = this.canvas.getBoundingClientRect();
    return { x: event.clientX - bounds.left, y: event.clientY - bounds.top };
  }
  #pointer(phase: Extract<ToApp, { type: 'pointer' }>['phase'], event: PointerEvent, at = this.#local(event)): void {
    this.send({
      type: 'pointer',
      phase,
      id: event.pointerId,
      ...at,
      time: performance.timeOrigin + event.timeStamp,
      touch: event.pointerType === 'touch',
      shift: event.shiftKey,
      modified: event.ctrlKey || event.metaKey || event.altKey,
      button: event.button,
    });
  }
  #down(event: PointerEvent): void {
    if (!event.isPrimary || ![0, 1].includes(event.button) || this.#press?.active) return;
    const at = this.#local(event);
    this.#press = {
      ...at,
      id: event.pointerId,
      active: true,
      dragged: false,
      identity: this.activation.identityAt(at.x, at.y),
    };
    if (event.button === 0 && !event.ctrlKey && !event.metaKey && !event.altKey) this.editor.activate(at.x, at.y);
    this.canvas.setPointerCapture(event.pointerId);
    this.#pointer('down', event, at);
  }
  #move(event: PointerEvent): void {
    if (!event.isPrimary) return;
    const at = this.#local(event);
    const press = this.#press;
    if (press?.active && press.id === event.pointerId) {
      press.dragged ||= (at.x - press.x) ** 2 + (at.y - press.y) ** 2 > 100;
      this.#pointer('move', event, at);
    } else if (!press?.active && event.pointerType !== 'touch') this.#pointer('hover', event, at);
  }
  #up(event: PointerEvent): void {
    if (!this.#press?.active || this.#press.id !== event.pointerId) return;
    this.#press.active = false;
    this.#pointer('up', event);
    if (this.canvas.hasPointerCapture(event.pointerId)) this.canvas.releasePointerCapture(event.pointerId);
  }
  #cancel(event: PointerEvent): void {
    if (!this.#press?.active || this.#press.id !== event.pointerId) return;
    this.#press = null;
    this.#pointer('cancel', event);
  }
  #click(event: MouseEvent): void {
    const press = this.#press;
    this.#press = null;
    if (!press || press.active || press.dragged || ![0, 1].includes(event.button)) return;
    const at = this.#local(event);
    if (
      (at.x - press.x) ** 2 + (at.y - press.y) ** 2 > 100 ||
      press.identity !== this.activation.identityAt(at.x, at.y)
    )
      return;
    if (this.activation.at(at.x, at.y, event)) event.preventDefault();
  }
  #wheel(event: WheelEvent): void {
    const target = event.target;
    if (event.ctrlKey || event.defaultPrevented || (target !== this.canvas && !this.editor.owns(target))) return;
    event.preventDefault();
    const unit = event.deltaMode === 1 ? 16 : event.deltaMode === 2 ? this.canvas.clientHeight : 1;
    let dx = event.deltaX * unit;
    let dy = event.deltaY * unit;
    if (event.shiftKey && !dx) {
      dx = dy;
      dy = 0;
    }
    // Native fields sit above the canvas. Consume their own scroll, then chain the
    // remainder to the worker; the non-scrolling DOM body cannot do that for us.
    if (target instanceof HTMLInputElement || target instanceof HTMLTextAreaElement) {
      const left = target.scrollLeft;
      const top = target.scrollTop;
      target.scrollLeft += dx;
      if (target instanceof HTMLTextAreaElement) target.scrollTop += dy;
      dx -= target.scrollLeft - left;
      dy -= target.scrollTop - top;
    }
    if (dx || dy) this.send({ type: 'wheel', ...this.#local(event), dx, dy, shift: false });
  }
  #key(event: KeyboardEvent): void {
    const editing = this.editor.owns(event.target);
    if (editing && !['Tab', 'Escape'].includes(event.key)) return;
    const command = event.ctrlKey || event.metaKey;
    if (!editing && command && event.key.toLowerCase() === 'a') {
      event.preventDefault();
      this.send({ type: 'command', name: 'selectAll' });
      return;
    }
    if (!editing && command && ['c', 'x', 'v'].includes(event.key.toLowerCase())) return;
    const routed = [
      'Tab',
      'Escape',
      'Enter',
      ' ',
      'ArrowLeft',
      'ArrowRight',
      'ArrowUp',
      'ArrowDown',
      'Home',
      'End',
      'PageUp',
      'PageDown',
    ];
    const ask = !editing && command && !event.altKey && !event.shiftKey && event.key.toLowerCase() === 'k';
    if (!ask && !routed.includes(event.key) && (event.ctrlKey || event.metaKey || event.altKey)) return;
    const nativeHandled = !editing && this.activation.key(event);
    if (ask || nativeHandled || routed.includes(event.key)) event.preventDefault();
    this.send({
      type: 'key',
      key: event.key,
      code: event.code,
      shift: event.shiftKey,
      ctrl: event.ctrlKey,
      meta: event.metaKey,
      alt: event.altKey,
      repeat: event.repeat,
      composing: event.isComposing,
      nativeHandled,
    });
  }
}
