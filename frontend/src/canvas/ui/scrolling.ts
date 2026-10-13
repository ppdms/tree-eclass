import { Scroller } from '../scroll';
import type { UINode } from './tree';
import type { ScrollMetrics } from './types';

export class NodeScroll {
  readonly x = new Scroller();
  readonly y = new Scroller();
  contentWidth = 0;
  contentHeight = 0;
  viewportWidth = 0;
  viewportHeight = 0;
  private previous = '';

  bounds(node: UINode, now: number): void {
    this.viewportWidth = node.rect.width;
    this.viewportHeight = node.rect.height;
    this.contentWidth = this.viewportWidth;
    this.contentHeight = this.viewportHeight;
    for (const child of node.children) {
      if (!child.shown || child.props.fixed) continue;
      this.contentWidth = Math.max(this.contentWidth, child.rect.x + child.rect.width - node.rect.x);
      this.contentHeight = Math.max(this.contentHeight, child.rect.y + child.rect.height - node.rect.y);
    }
    const axis = node.props.scroll?.axis ?? 'vertical';
    this.x.setBounds(0, axis === 'vertical' ? 0 : this.contentWidth - this.viewportWidth, this.viewportWidth, now);
    this.y.setBounds(0, axis === 'horizontal' ? 0 : this.contentHeight - this.viewportHeight, this.viewportHeight, now);
  }

  step(now: number): boolean {
    const x = this.x.step(now);
    const y = this.y.step(now);
    return x || y || this.x.dragging || this.y.dragging;
  }

  publish(node: UINode): void {
    const metrics: ScrollMetrics = {
      offset: this.y.offset,
      offsetX: this.x.offset,
      contentWidth: this.contentWidth,
      contentHeight: this.contentHeight,
      viewportWidth: this.viewportWidth,
      viewportHeight: this.viewportHeight,
    };
    const signature = JSON.stringify(metrics);
    if (signature === this.previous) return;
    this.previous = signature;
    node.props.scroll?.onScroll?.(metrics.offset);
    node.props.scroll?.onScrollMetrics?.(metrics);
  }
}
