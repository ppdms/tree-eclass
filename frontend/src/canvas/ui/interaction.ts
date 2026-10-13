import type { PointerInput } from '../protocol';
import type { Scroller } from '../scroll';
import { CanvasFocus } from './focus';
import { keyboard } from './interaction-keyboard';
import { InteractionScroll, type TouchScroll } from './interaction-scroll';
import {
  activateTarget,
  alive,
  ancestry,
  byId,
  changeWithin,
  dispatchContextMenu,
  dispatchPointer,
  draggable,
  inlineAt,
  isControl,
  isControlText,
  linkIdentity,
  within,
} from './interaction-targets';
import type { UIPainter } from './paint';
import { hit, hitText } from './query';
import { activate } from './react';
import { CanvasSelection } from './selection';
import type { UINode, UITree } from './tree';
import type { CanvasKey, LinkRect } from './types';

export interface FocusDescriptor {
  id: string;
  href?: string;
  internal?: boolean;
  target?: '_blank' | '_self';
  download?: string;
  rel?: string;
  nativeKind?: 'text' | 'file' | 'copy';
}

export interface InteractionEvents {
  cursor(value: 'pointer' | 'default' | 'text', title?: string): void;
  navigate(href: string): void;
  focus(target: FocusDescriptor | null): void;
  selection(text: string): void;
  invalidate(): void;
}

export type CanvasPointer = PointerInput & { modified?: boolean; button?: number; nativeHandled?: boolean };
interface Press {
  input: CanvasPointer;
  last: CanvasPointer;
  target: UINode | null;
  link?: LinkRect;
  drag: UINode | null;
  dragging: boolean;
  capture: UINode | null;
  observers: UINode[];
  consumed: boolean;
  scroll: TouchScroll | null;
  selecting: boolean;
  moved: boolean;
  text: UINode | null;
}

export class Interaction {
  readonly focus: CanvasFocus;
  readonly selection: CanvasSelection;
  readonly scrolling: InteractionScroll;
  #down: Press | null = null;
  #hovered: UINode[] = [];
  #hoverPoint: CanvasPointer | null = null;
  #cursor = '';
  #longPress: number | undefined;
  #autoscroll: number | undefined;

  constructor(
    readonly tree: UITree,
    readonly scroller: Scroller,
    readonly painter: UIPainter,
    readonly events: InteractionEvents,
  ) {
    this.focus = new CanvasFocus(tree, painter, events, scroller);
    this.selection = new CanvasSelection(tree, painter, events);
    this.scrolling = new InteractionScroll(tree, scroller);
  }

  pointer(input: CanvasPointer): void {
    const target = hit(this.tree, input.x, input.y) ?? hitText(this.tree, input.x, input.y);
    if (input.phase === 'hover') {
      this.#hover(input, target);
      return;
    }
    if (!input.touch && input.phase !== 'cancel') this.#hover({ ...input, phase: 'hover' }, target, false);
    if (input.phase === 'down') {
      if (!this.#down) this.#begin(input, target);
      return;
    }
    const down = this.#down;
    if (!down || down.input.id !== input.id) return;
    down.last = input;
    if (input.phase === 'move') this.#move(down, input, target);
    else if (input.phase === 'up' || input.phase === 'cancel') this.#end(down, input, target);
    this.events.invalidate();
  }

  key(event: CanvasKey): boolean {
    if (event.key === 'Escape' && this.#down) {
      this.cancel();
      return true;
    }
    if (this.focus.node && !alive(this.tree, this.focus.node)) this.focus.set(null, true, false);
    const handled = keyboard(this, event);
    this.events.invalidate();
    return handled;
  }

  wheel(x: number, y: number, dx: number, dy: number): void {
    const target = hit(this.tree, x, y) ?? hitText(this.tree, x, y);
    this.scrolling.wheel(target, dx, dy);
    this.events.invalidate();
  }

  contextMenu(x: number, y: number): void {
    this.cancel();
    const node = hit(this.tree, x, y) ?? hitText(this.tree, x, y);
    if (dispatchContextMenu(this.tree, node, x, y)) this.events.invalidate();
  }

  capturePointer(nodeId: string, pointerId: number): void {
    const down = this.#down;
    const node = byId(this.tree, nodeId);
    if (!down || down.input.id !== pointerId || !node || !node.props.onPointer) return;
    if (this.tree.activeModal && !within(node, this.tree.activeModal)) return;
    this.#capture(down, node);
    this.events.invalidate();
  }

  releasePointer(pointerId: number): void {
    const down = this.#down;
    if (!down || down.input.id !== pointerId) return;
    down.capture = null;
    down.consumed = true;
    this.painter.pressed = null;
    this.events.invalidate();
  }

  focusNode(id: string): void {
    this.focus.focusNode(id);
  }
  focusField(id: string): void {
    this.focus.focusNode(id, true);
  }
  selectAll(): void {
    this.selection.selectAll();
  }
  clearSelection(): void {
    this.selection.clear();
  }

  cancel(): void {
    const down = this.#down;
    this.#down = null;
    this.#clearTimers();
    if (down) {
      this.#notify(down, { ...down.last, phase: 'cancel' });
      if (down.dragging && alive(this.tree, down.drag)) {
        const handler = down.drag.props.onDragEnd;
        if (handler) activate(() => handler(true));
      }
      if (down.scroll) this.scrolling.end(down.scroll, performance.timeOrigin + performance.now(), true);
    }
    this.painter.pressed = null;
    this.events.invalidate();
  }

  synchronize(): void {
    this.focus.synchronize();
    const down = this.#down;
    if (
      down &&
      ((down.target && !alive(this.tree, down.target)) ||
        (down.capture && !alive(this.tree, down.capture)) ||
        (down.drag && !alive(this.tree, down.drag)))
    )
      this.cancel();
    this.selection.synchronize();
    if (this.#down?.selecting) this.selection.extend(this.#down.last.x, this.#down.last.y);
    if (this.#hoverPoint) {
      const input = this.#hoverPoint;
      this.#hover(input, hit(this.tree, input.x, input.y) ?? hitText(this.tree, input.x, input.y), false);
    }
  }

  #begin(input: CanvasPointer, target: UINode | null): void {
    for (const root of this.tree.fixedRoots) {
      const dismiss = root.props.onOutsidePointer;
      if (dismiss && !within(target, root)) activate(() => dismiss({ targetId: target?.props.id }));
    }
    const button = input.button ?? 0;
    const link = inlineAt(target, input.x, input.y);
    const text = hitText(this.tree, input.x, input.y);
    const drag = input.modified || input.touch || button !== 0 ? null : draggable(target);
    const down: Press = {
      input,
      last: input,
      target,
      link,
      text,
      drag,
      dragging: false,
      capture: null,
      observers: [],
      consumed: button !== 0,
      scroll: null,
      selecting: false,
      moved: false,
    };
    this.#down = down;
    this.painter.point(input);
    this.focus.focusPointer(target, link);
    if (!input.shift && button === 0) this.selection.clear();
    this.#observe(down);
    if (down.consumed || down.capture || this.#down !== down) return;
    this.painter.pressed = target;
    if (input.touch) {
      down.scroll = this.scrolling.start(target, input);
      if (text && !isControlText(text)) this.#longPress = self.setTimeout(() => this.#touchSelect(down), 450);
    } else if (text && !drag && !isControlText(text) && (!input.modified || input.shift)) {
      down.selecting = this.selection.begin(
        text,
        input.x,
        input.y,
        this.selection.granularity(input, text),
        input.shift,
      );
      if (down.selecting) this.#startAutoscroll();
    }
    this.events.invalidate();
  }

  #observe(down: Press): void {
    for (const node of ancestry(down.target)) {
      if (!alive(this.tree, node) || !node.props.onPointer) continue;
      down.observers.push(node);
      if (dispatchPointer(node, down.input)) this.#capture(down, node);
      if (down.capture || this.#down !== down) break;
    }
  }

  #move(down: Press, input: CanvasPointer, target: UINode | null): void {
    const distance = Math.hypot(input.x - down.input.x, input.y - down.input.y);
    if (distance > 10) {
      down.moved = true;
      this.painter.pressed = null;
      this.#clearLongPress();
    }
    this.#notify(down, input);
    if (this.#down !== down || down.consumed || down.capture) return;
    if (down.selecting) {
      this.selection.extend(input.x, input.y);
      return;
    }
    if (down.scroll) this.scrolling.move(down.scroll, input);
    if (!down.drag || distance < 6 || !alive(this.tree, down.drag)) return;
    if (!down.dragging) {
      down.dragging = true;
      const handler = down.drag.props.onDragStart;
      if (handler) activate(handler);
    }
    const over = draggable(target);
    const source = down.drag.props.dragId;
    const handler = over?.props.onDragOver;
    if (source && over && alive(this.tree, over) && over.props.dragId !== source && handler) {
      activate(() => handler(source));
    }
  }

  #end(down: Press, input: CanvasPointer, target: UINode | null): void {
    this.#clearTimers();
    this.#notify(down, input);
    if (this.#down !== down) return;
    if (down.scroll) this.scrolling.end(down.scroll, input.time, input.phase === 'cancel');
    if (down.dragging && alive(this.tree, down.drag)) {
      const handler = down.drag.props.onDragEnd;
      if (handler) activate(() => handler(input.phase === 'cancel'));
    } else if (
      input.phase === 'up' &&
      !down.consumed &&
      !down.capture &&
      !down.moved &&
      !this.selection.hasRange &&
      target === down.target &&
      this.painter.pressed &&
      target
    ) {
      const link = inlineAt(target, input.x, input.y);
      if ((!link && !down.link) || (link && down.link && linkIdentity(link) === linkIdentity(down.link))) {
        activateTarget(
          this.tree,
          this.events,
          { node: target, link },
          !!input.modified || input.shift,
          input.nativeHandled,
        );
      }
    }
    this.#down = null;
    this.painter.pressed = null;
    if (input.touch) this.painter.point(null);
  }

  #capture(down: Press, node: UINode): void {
    if (down.capture === node) return;
    for (const observer of down.observers) {
      if (observer !== node && alive(this.tree, observer)) dispatchPointer(observer, { ...down.last, phase: 'cancel' });
    }
    if (down.scroll) this.scrolling.end(down.scroll, down.last.time, true);
    down.scroll = null;
    down.capture = node;
    down.observers = [node];
    down.consumed = true;
    down.selecting = false;
    // A reader may already have mirrored its exact PDF selection in the capturing callback.
    this.selection.clear(false);
    this.#clearTimers();
    this.painter.pressed = null;
  }

  #notify(down: Press, input: CanvasPointer): void {
    const nodes = down.capture ? [down.capture] : down.observers;
    for (const node of nodes) {
      if (!alive(this.tree, node)) continue;
      if (dispatchPointer(node, input) && input.phase === 'move') this.#capture(down, node);
      if (down.capture) break;
    }
  }

  #hover(input: CanvasPointer, target: UINode | null, dispatch = true): void {
    this.#hoverPoint = input;
    this.painter.point(target ? input : null);
    const ancestors = ancestry(target);
    changeWithin(this.#hovered, ancestors, 'hover');
    this.#hovered = ancestors;
    this.painter.hovered = target;
    const link = inlineAt(target, input.x, input.y);
    const cursor =
      target?.props.field || (target?.kind === 'text' && !isControl(target) && !link)
        ? 'text'
        : target && (isControl(target) || link || target.props.dragId)
          ? 'pointer'
          : 'default';
    const title = link?.title ?? ancestors.find((node) => node.props.title)?.props.title;
    const signature = `${cursor}:${title ?? ''}`;
    if (signature !== this.#cursor) {
      this.#cursor = signature;
      this.events.cursor(cursor, title);
    }
    if (dispatch)
      for (const node of ancestors) {
        if (alive(this.tree, node) && dispatchPointer(node, input)) break;
      }
    if (dispatch) this.events.invalidate();
  }

  #touchSelect(down: Press): void {
    this.#longPress = undefined;
    if (this.#down !== down || down.moved || !down.text || !alive(this.tree, down.text)) return;
    if (down.scroll) this.scrolling.end(down.scroll, performance.timeOrigin + performance.now(), true);
    down.scroll = null;
    down.selecting = this.selection.begin(down.text, down.last.x, down.last.y, 'word', false);
    this.painter.pressed = null;
    this.events.invalidate();
  }

  #startAutoscroll(): void {
    if (this.#autoscroll !== undefined) return;
    this.#autoscroll = self.setTimeout(() => {
      this.#autoscroll = undefined;
      const down = this.#down;
      if (!down?.selecting || !down.text || !alive(this.tree, down.text)) return;
      if (this.scrolling.autoscroll(down.text, down.last.x, down.last.y)) this.events.invalidate();
      this.#startAutoscroll();
    }, 16);
  }

  #clearLongPress(): void {
    clearTimeout(this.#longPress);
    this.#longPress = undefined;
  }

  #clearTimers(): void {
    this.#clearLongPress();
    clearTimeout(this.#autoscroll);
    this.#autoscroll = undefined;
  }
}
