import type { FocusDescriptor } from '../ui/interaction';
import type { NativeHotspot } from './browserProtocol';
import type { FromApp, ToApp } from './protocol';
import type { CanvasEditor } from './editor';
import type { BrowserClipboardHost } from './browserClipboard';

type Link = Extract<FromApp, { type: 'links' }>['items'][number];
type Modifiers = { ctrlKey: boolean; metaKey: boolean; shiftKey: boolean; altKey: boolean; button?: number };
const safeProtocols = {
  'http:': true,
  'https:': true,
  'irc:': true,
  'ircs:': true,
  'mailto:': true,
  'xmpp:': true,
};
export class BrowserActivationHost {
  #links: Link[] = [];
  #spots: NativeHotspot[] = [];
  #actions: NativeHotspot[] = [];
  #focused: FocusDescriptor | null = null;
  readonly #files = new Map<string, { input: HTMLInputElement; resetKey: string | number | undefined }>();

  constructor(
    readonly editor: CanvasEditor,
    readonly clipboard: BrowserClipboardHost,
    readonly send: (message: ToApp) => void,
  ) {}
  links(items: Link[]): void {
    this.#links = items;
  }
  fields(spots: NativeHotspot[], actions: NativeHotspot[]): void {
    this.#spots = spots;
    this.#actions = actions;
    const current = new Set<string>();
    for (const spot of actions) {
      if (spot.kind !== 'file') continue;
      current.add(spot.id);
      const file = this.#file(spot);
      file.input.accept = spot.accept ?? '';
      file.input.multiple = spot.multiple ?? false;
      file.input.disabled = spot.disabled ?? false;
      if (file.resetKey !== spot.resetKey) {
        file.input.value = '';
        file.resetKey = spot.resetKey;
      }
    }
    for (const [id, file] of this.#files) {
      if (current.has(id)) continue;
      file.input.remove();
      this.#files.delete(id);
    }
  }
  focus(target: FocusDescriptor | null): void {
    this.#focused = target;
    if (!target) this.editor.blur();
    else if (target.nativeKind === 'text') this.editor.activateId(target.id);
    else this.editor.blur();
  }
  identityAt(x: number, y: number): string | null {
    for (let index = this.#spots.length - 1; index >= 0; index--) {
      const spot = this.#spots[index]!;
      if (!spot.disabled && x >= spot.x && x <= spot.x + spot.width && y >= spot.y && y <= spot.y + spot.height) {
        return `native:${spot.kind}:${spot.id}`;
      }
    }
    for (let index = this.#links.length - 1; index >= 0; index--) {
      const link = this.#links[index]!;
      if (x >= link.x && x <= link.x + link.width && y >= link.y && y <= link.y + link.height) {
        return `link:${link.href}:${link.target ?? '_self'}:${link.download ?? ''}`;
      }
    }
    return null;
  }
  at(x: number, y: number, event: Modifiers): boolean {
    if ((event.button ?? 0) === 0 && this.editor.activate(x, y)) return true;
    for (let index = this.#spots.length - 1; index >= 0; index--) {
      const spot = this.#spots[index]!;
      if (spot.disabled || x < spot.x || x > spot.x + spot.width || y < spot.y || y > spot.y + spot.height) continue;
      if ((event.button ?? 0) === 0 && this.#native(spot)) return true;
    }
    for (let index = this.#links.length - 1; index >= 0; index--) {
      const link = this.#links[index]!;
      if (x >= link.x && x <= link.x + link.width && y >= link.y && y <= link.y + link.height) {
        return this.#link(link, event);
      }
    }
    return false;
  }
  key(event: KeyboardEvent): boolean {
    const focused = this.#focused;
    if (!focused || event.repeat || event.isComposing || !['Enter', ' '].includes(event.key)) return false;
    if (focused.href && event.key === 'Enter') return this.#link({ ...focused, href: focused.href }, event);
    const spot = this.#actions.find((item) => item.id === focused.id && !item.disabled);
    if (spot) {
      if (spot.kind === 'text') return this.editor.activateId(spot.id, true);
      return this.#native(spot);
    }
    if (focused.nativeKind === 'text') return this.editor.activateId(focused.id, true);
    return false;
  }
  #native(spot: NativeHotspot): boolean {
    if (spot.kind === 'file') this.#file(spot).input.click();
    else if (spot.kind === 'copy') {
      void this.clipboard.copy(spot.value ?? '').then(
        () => this.send({ type: 'nativeResult', id: spot.id, error: null }),
        () => this.send({ type: 'nativeResult', id: spot.id, error: 'Native clipboard write failed' }),
      );
    } else if (spot.kind === 'focus-field') return this.editor.activateId(spot.fieldId ?? spot.id, true);
    else return false;
    return true;
  }
  #file(spot: NativeHotspot): { input: HTMLInputElement; resetKey: string | number | undefined } {
    let file = this.#files.get(spot.id);
    if (file) return file;
    const input = document.createElement('input');
    input.type = 'file';
    input.tabIndex = -1;
    input.style.display = 'none';
    input.addEventListener('change', () =>
      this.send({ type: 'files', id: spot.id, files: Array.from(input.files ?? []) }),
    );
    document.body.append(input);
    file = { input, resetKey: spot.resetKey };
    this.#files.set(spot.id, file);
    return file;
  }
  #link(link: Pick<Link, 'href' | 'internal' | 'target' | 'download' | 'rel'>, event: Modifiers): boolean {
    const modified = event.ctrlKey || event.metaKey || event.shiftKey || event.altKey || event.button === 1;
    if (link.internal && !modified && link.target !== '_blank' && link.download === undefined) return false;
    let url: URL;
    try {
      url = new URL(link.href, location.href);
    } catch {
      return false;
    }
    if (!Object.hasOwn(safeProtocols, url.protocol)) return false;
    const anchor = document.createElement('a');
    anchor.href = url.href;
    anchor.target = link.target ?? '_self';
    anchor.rel = link.rel ?? 'noopener';
    if (link.download !== undefined) anchor.download = link.download;
    if (event.button === 1) anchor.target = '_blank';
    document.body.append(anchor);
    anchor.dispatchEvent(
      new MouseEvent('click', {
        bubbles: true,
        cancelable: true,
        view: window,
        ctrlKey: event.ctrlKey,
        metaKey: event.metaKey,
        shiftKey: event.shiftKey,
        altKey: event.altKey,
        button: 0,
      }),
    );
    anchor.remove();
    return true;
  }
}
