import type { CanvasViewport } from '../app/viewport';
import type { UINode, UITree } from './tree';
import type { Rect } from './types';

export function intersect(a: Rect, b: Rect): Rect {
  const x = Math.max(a.x, b.x);
  const y = Math.max(a.y, b.y);
  return {
    x,
    y,
    width: Math.max(0, Math.min(a.x + a.width, b.x + b.width) - x),
    height: Math.max(0, Math.min(a.y + a.height, b.y + b.height) - y),
  };
}

export function visibleRect(node: UINode): Rect | null {
  if (!node.shown) return null;
  const rect = intersect(node.screen, node.clip);
  return rect.width > 0 && rect.height > 0 ? rect : null;
}

export function isDescendant(node: UINode, ancestor: UINode): boolean {
  for (let current: UINode | null = node; current; current = current.parent) {
    if (current === ancestor) return true;
  }
  return false;
}

export function localRect(node: UINode, rect: Rect): Rect {
  return { x: node.screen.x + rect.x, y: node.screen.y + rect.y, width: rect.width, height: rect.height };
}

function position(node: UINode, dx: number, dy: number, clip: Rect): void {
  if (node.kind === 'raw' || !node.shown) return;
  node.screen.x = node.rect.x - dx;
  node.screen.y = node.rect.y - dy;
  node.screen.width = node.rect.width;
  node.screen.height = node.rect.height;
  node.clip = clip;
  const clipped = node.props.scroll || node.style.overflow === 'hidden' ? intersect(clip, node.screen) : clip;
  const x = dx + (node.scrolling?.x.offset ?? 0);
  const y = dy + (node.scrolling?.y.offset ?? 0);
  for (const child of node.children) {
    if (!child.props.fixed) position(child, x, y, clipped);
  }
}

export function updateGeometry(tree: UITree, scroll: number, viewport: CanvasViewport): void {
  const clip = { x: 0, y: viewport.top, width: viewport.width, height: viewport.bottom - viewport.top };
  position(tree.root, 0, scroll, clip);
  for (const fixed of tree.fixedRoots) position(fixed, 0, 0, clip);
}

export function publishGeometry(tree: UITree): void {
  for (const node of tree.nodes.values()) {
    if (!node.shown) continue;
    node.scrolling?.publish(node);
    const callback = node.props.onLayout;
    if (!callback) continue;
    const rect = node.screen;
    const previous = node.notifiedRect;
    if (
      previous &&
      previous.x === rect.x &&
      previous.y === rect.y &&
      previous.width === rect.width &&
      previous.height === rect.height &&
      node.layoutCallback === callback
    )
      continue;
    node.layoutCallback = callback;
    node.notifiedRect = { ...rect };
    callback({ ...rect });
  }
}
