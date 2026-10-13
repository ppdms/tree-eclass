import type { Scroller } from '../scroll';
import { visibleRect } from './geometry';
import type { UINode, UITree } from './tree';
import type { CanvasPointer } from './interaction';
import { activate } from './react';

export interface TouchScroll {
  x: Scroller | null;
  y: Scroller | null;
}

export class InteractionScroll {
  constructor(
    readonly tree: UITree,
    readonly root: Scroller,
  ) {}

  wheel(node: UINode | null, dx: number, dy: number): boolean {
    let changed = false;
    for (; node; node = node.parent) {
      const scroll = node.scrolling;
      if (scroll) {
        if (dx || dy) this.#intent(node);
        const x = scroll.x.offset;
        const y = scroll.y.offset;
        if (node.props.scroll?.axis !== 'vertical') scroll.x.wheel(dx);
        if (node.props.scroll?.axis !== 'horizontal') scroll.y.wheel(dy);
        dx -= scroll.x.offset - x;
        dy -= scroll.y.offset - y;
        changed ||= x !== scroll.x.offset || y !== scroll.y.offset;
      }
      if (!dx && !dy) return changed;
      if (node.props.fixed && node.props.blockPointer) return changed;
    }
    if (!this.tree.activeModal) {
      const offset = this.root.offset;
      this.root.wheel(dy);
      changed ||= this.root.offset !== offset;
    }
    return changed;
  }

  start(node: UINode | null, input: CanvasPointer): TouchScroll {
    let x: Scroller | null = null;
    let y: Scroller | null = null;
    let blocked = false;
    for (; node; node = node.parent) {
      const scroll = node.scrolling;
      if (scroll) {
        const axis = node.props.scroll?.axis ?? 'vertical';
        this.#intent(node);
        if (!x && axis !== 'vertical' && scroll.contentWidth > scroll.viewportWidth) x = scroll.x;
        if (!y && axis !== 'horizontal' && scroll.contentHeight > scroll.viewportHeight) y = scroll.y;
      }
      if (node.props.fixed && node.props.blockPointer) {
        blocked = true;
        break;
      }
    }
    if (!y && !blocked && !this.tree.activeModal) y = this.root;
    x?.dragStart(input.x, input.time);
    y?.dragStart(input.y, input.time);
    return { x, y };
  }

  move(scroll: TouchScroll, input: CanvasPointer): void {
    scroll.x?.dragMove(input.x, input.time);
    scroll.y?.dragMove(input.y, input.time);
  }

  end(scroll: TouchScroll, time: number, cancelled: boolean): void {
    if (cancelled) {
      scroll.x?.cancelDrag(time);
      scroll.y?.cancelDrag(time);
    } else {
      scroll.x?.dragEnd(time);
      scroll.y?.dragEnd(time);
    }
  }

  key(node: UINode | null, key: string, shift: boolean): boolean {
    let area = node;
    while (area && !area.scrolling) area = area.parent;
    const scroll = area?.scrolling;
    const vertical = scroll?.y ?? (this.tree.activeModal ? null : this.root);
    const horizontal = scroll?.x;
    if (!vertical && !horizontal) return false;
    const extent = scroll?.viewportHeight ?? this.tree.root.clip.height;
    const step = key === 'PageDown' || key === 'PageUp' || key === ' ' ? extent * 0.9 : 48;
    const direction = key === 'ArrowUp' || key === 'PageUp' || (key === ' ' && shift) ? -1 : 1;
    if (key === 'ArrowLeft' || key === 'ArrowRight') {
      if (!horizontal || area?.props.scroll?.axis === 'vertical') return false;
      return this.wheel(area, key === 'ArrowLeft' ? -48 : 48, 0);
    }
    if (['ArrowUp', 'ArrowDown', 'PageUp', 'PageDown', ' '].includes(key)) {
      this.wheel(area, 0, step * direction);
      return true;
    }
    if (key !== 'Home' && key !== 'End') return false;
    const horizontalOnly = area?.props.scroll?.axis === 'horizontal';
    const axis = horizontalOnly ? horizontal : vertical;
    if (!axis) return false;
    const max =
      horizontalOnly && scroll
        ? scroll.contentWidth - scroll.viewportWidth
        : scroll
          ? scroll.contentHeight - scroll.viewportHeight
          : this.tree.root.rect.height - extent;
    if (area) this.#intent(area);
    axis.wheel((key === 'Home' ? 0 : max) - axis.offset);
    return true;
  }

  #intent(node: UINode): void {
    const handler = node.props.scroll?.onUserScroll;
    if (handler) activate(handler);
  }

  autoscroll(node: UINode, x: number, y: number): boolean {
    let area: UINode | null = node;
    while (area && !area.scrolling) area = area.parent;
    const rect = area ? visibleRect(area) : this.tree.root.clip;
    if (!rect) return false;
    const edge = 32;
    const speed = (at: number, start: number, extent: number) =>
      at < start + edge
        ? -Math.min(32, (start + edge - at) * 0.5)
        : at > start + extent - edge
          ? Math.min(32, (at - start - extent + edge) * 0.5)
          : 0;
    return this.wheel(
      area,
      area?.props.scroll?.axis !== 'vertical' ? speed(x, rect.x, rect.width) : 0,
      speed(y, rect.y, rect.height),
    );
  }
}
