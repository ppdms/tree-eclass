import type { UINode, UITree } from './tree';
import type { LinkRect, NodeSnapshot, Rect } from './types';
import { intersect, isDescendant, localRect, visibleRect } from './geometry';

export function contains(rect: Rect, x: number, y: number): boolean {
  return (
    rect.width > 0 &&
    rect.height > 0 &&
    x >= rect.x &&
    x <= rect.x + rect.width &&
    y >= rect.y &&
    y <= rect.y + rect.height
  );
}

function responds(node: UINode): boolean {
  const p = node.props;
  return Boolean(
    p.onClick ||
    p.href ||
    p.field ||
    p.nativeAction ||
    p.dragId ||
    p.onPointer ||
    p.onContextMenu ||
    p.scroll ||
    p.onHoverChange ||
    p.onFocusWithinChange ||
    p.blockPointer ||
    node.inlineLinks.length,
  );
}

function hitNode(node: UINode, x: number, y: number, text: boolean): UINode | null {
  if (!node.shown || node.kind === 'raw' || !contains(node.clip, x, y)) return null;
  const inside = contains(node.screen, x, y);
  if (!inside && (node.props.scroll || node.style.overflow === 'hidden')) return null;
  for (let index = node.children.length - 1; index >= 0; index--) {
    const child = node.children[index]!;
    if (child.props.fixed) continue;
    const found = hitNode(child, x, y, text);
    if (found) return found;
  }
  if (!inside) return null;
  if (text) return node.kind === 'text' && !node.props.field && !node.disabled ? node : null;
  return responds(node) ? node : null;
}

function hitLayers(tree: UITree, x: number, y: number, text: boolean): UINode | null {
  for (let index = tree.fixedRoots.length - 1; index >= 0; index--) {
    const node = tree.fixedRoots[index]!;
    const found = hitNode(node, x, y, text);
    if (found) return found;
    if (node.shown && contains(node.screen, x, y)) return null;
  }
  return tree.activeModal ? null : hitNode(tree.root, x, y, text);
}

export function hit(tree: UITree, x: number, y: number): UINode | null {
  return hitLayers(tree, x, y, false);
}

export function hitText(tree: UITree, x: number, y: number): UINode | null {
  return hitLayers(tree, x, y, true);
}

export function focusableNodes(tree: UITree, scope?: UINode | null): UINode[] {
  const within = scope === undefined ? (tree.activeModal ?? tree.focusRoots.at(-1)) : scope;
  const nodes: UINode[] = [];
  for (const node of orderedNodes(within ?? tree.root)) {
    if (!node.shown || node.disabled) continue;
    const p = node.props;
    // A scope's own scrim keeps pointer focus (and its Escape handler) but is never a Tab stop.
    if (node === within && p.focusScope) continue;
    if (p.focusable === false || p.nativeAction?.kind === 'focus-field') continue;
    if (p.focusable || p.onClick || p.href || p.field || p.nativeAction || node.inlineLinks.length) nodes.push(node);
  }
  return nodes;
}

export function selectableText(tree: UITree): UINode[] {
  const within = tree.activeModal ?? tree.focusRoots.at(-1);
  return [...orderedNodes(within ?? tree.root)].filter(
    (node) => node.shown && !node.disabled && node.kind === 'text' && !node.props.field,
  );
}

function* orderedNodes(node: UINode): Generator<UINode> {
  yield node;
  for (const child of node.children) yield* orderedNodes(child);
}

/** Clip native activations to exactly the visible portion not covered by later fixed layers. */
export function exposedRects(tree: UITree, node: UINode, rect: Rect = node.screen): Rect[] {
  if (!node.shown || (tree.activeModal && !isDescendant(node, tree.activeModal))) return [];
  const clipped = intersect(rect, node.clip);
  if (clipped.width <= 0 || clipped.height <= 0) return [];
  let pieces = [clipped];
  let owner = node;
  while (owner.parent && !owner.props.fixed) owner = owner.parent;
  const start = node.fixed ? tree.fixedRoots.indexOf(owner) + 1 : 0;
  for (let index = start; index < tree.fixedRoots.length; index++) {
    const cover = tree.fixedRoots[index]!;
    if (cover.shown && !isDescendant(node, cover)) pieces = pieces.flatMap((piece) => subtract(piece, cover.screen));
  }
  return pieces;
}

export function visibleLinks(tree: UITree): LinkRect[] {
  const links: LinkRect[] = [];
  for (const node of tree.nodes.values()) {
    if (!node.shown || node.disabled) continue;
    if (node.props.href) {
      const link = {
        href: node.props.href,
        internal: !node.props.external,
        target: node.props.target,
        rel: node.props.rel,
        download: node.props.download,
        title: node.props.title ?? node.props.label,
      };
      for (const rect of exposedRects(tree, node)) links.push({ ...rect, ...link });
    }
    for (const link of node.inlineLinks) {
      for (const rect of exposedRects(tree, node, localRect(node, link))) links.push({ ...link, ...rect });
    }
  }
  return links;
}

function subtract(rect: Rect, cover: Rect): Rect[] {
  const overlap = intersect(rect, cover);
  if (overlap.width <= 0 || overlap.height <= 0) return [rect];
  const pieces: Rect[] = [];
  const right = overlap.x + overlap.width;
  const bottom = overlap.y + overlap.height;
  if (rect.y < overlap.y) pieces.push({ ...rect, height: overlap.y - rect.y });
  if (bottom < rect.y + rect.height) pieces.push({ ...rect, y: bottom, height: rect.y + rect.height - bottom });
  if (rect.x < overlap.x) pieces.push({ x: rect.x, y: overlap.y, width: overlap.x - rect.x, height: overlap.height });
  if (right < rect.x + rect.width) {
    pieces.push({ x: right, y: overlap.y, width: rect.x + rect.width - right, height: overlap.height });
  }
  return pieces;
}

export function snapshot(tree: UITree): NodeSnapshot[] {
  const nodes: NodeSnapshot[] = [];
  for (const node of tree.nodes.values()) {
    const id = node.props.id ?? (node.props.href ? `link:${node.props.href}` : '');
    if (!id || !exposedRects(tree, node).length) continue;
    nodes.push({ ...node.screen, id, text: node.kind === 'text' ? node.content : '', href: node.props.href ?? '' });
  }
  return nodes;
}

export function notifyVisible(tree: UITree, frame: number): void {
  for (const node of tree.nodes.values()) {
    const callback = node.props.onVisible;
    const visible = node.shown && Boolean(visibleRect(node)) && (node.kind !== 'raster' || node.paintedAt === frame);
    if (!visible) node.visibleCallback = undefined;
    if (!visible || !callback) continue;
    if (node.visibleCallback === callback && node.visibleResource === node.paintedResource) continue;
    node.visibleCallback = callback;
    node.visibleResource = node.paintedResource;
    callback();
  }
}
