import { contains } from './query';
import { isDescendant } from './geometry';
import { activate } from './react';
import type { UINode, UITree } from './tree';
import type { CanvasNodePointer, LinkRect } from './types';
import type { CanvasPointer, InteractionEvents } from './interaction';
import type { FocusTarget } from './focus';

export function alive(tree: UITree, node: UINode | null): node is UINode {
  if (!node || !node.shown || node.disabled || !tree.nodes.has(node.nodeId)) return false;
  for (let current: UINode | null = node; current; current = current.parent) {
    if (current.hidden || current.props.disabled || current.style.display === 'none') return false;
    if (current === tree.root) return true;
  }
  return false;
}

export function byId(tree: UITree, id: string): UINode | null {
  for (const node of tree.nodes.values()) if (node.props.id === id && alive(tree, node)) return node;
  return null;
}

export function ancestry(node: UINode | null): UINode[] {
  const nodes: UINode[] = [];
  for (; node; node = node.parent) nodes.push(node);
  return nodes;
}

export function within(node: UINode | null, parent: UINode): boolean {
  return !!node && isDescendant(node, parent);
}

export function changeWithin(previous: UINode[], next: UINode[], kind: 'hover' | 'focus'): void {
  const notify = (node: UINode, value: boolean) => {
    const callback = kind === 'hover' ? node.props.onHoverChange : node.props.onFocusWithinChange;
    if (callback) activate(() => callback(value));
  };
  for (const node of previous) if (!next.includes(node)) notify(node, false);
  for (let index = next.length - 1; index >= 0; index--) {
    const node = next[index]!;
    if (!previous.includes(node)) notify(node, true);
  }
}

export function nodePointer(node: UINode, input: CanvasPointer): CanvasNodePointer {
  return {
    ...input,
    localX: input.x - node.screen.x,
    localY: input.y - node.screen.y,
    modified: !!input.modified,
    button: input.button ?? 0,
  };
}

export function dispatchPointer(node: UINode, input: CanvasPointer): boolean {
  let handled = false;
  const handler = node.props.onPointer;
  if (handler)
    activate(() => {
      handled = handler(nodePointer(node, input)) === true;
    });
  return handled;
}

export function dispatchContextMenu(tree: UITree, node: UINode | null, x: number, y: number): boolean {
  for (; node; node = node.parent) {
    if (!alive(tree, node)) return false;
    const handler = node.props.onContextMenu;
    if (!handler) continue;
    const localX = x - node.screen.x;
    const localY = y - node.screen.y;
    activate(() => handler({ x, y, localX, localY }));
    return true;
  }
  return false;
}

export function draggable(node: UINode | null): UINode | null {
  for (; node; node = node.parent) if (node.props.dragId && !node.disabled) return node;
  return null;
}

export function inlineAt(node: UINode | null, x: number, y: number): LinkRect | undefined {
  if (!node || !contains(node.clip, x, y)) return undefined;
  return node.inlineLinks.find((link) => contains(link, x - node.screen.x, y - node.screen.y));
}

export function linkIdentity(link: LinkRect): string {
  return `${link.runIndex ?? link.start ?? link.href}:${link.href}`;
}

export function isControl(node: UINode): boolean {
  return !!(node.props.onClick || node.props.field || node.props.nativeAction || node.props.href);
}

export function isControlText(node: UINode): boolean {
  for (let current: UINode | null = node; current; current = current.parent) {
    if (current.props.field || current.props.onClick || current.props.nativeAction || current.props.href) return true;
  }
  return false;
}

export function activateTarget(
  tree: UITree,
  events: InteractionEvents,
  target: FocusTarget,
  modified: boolean,
  nativeHandled = false,
): boolean {
  const { node, link } = target;
  if (!alive(tree, node)) return false;
  if (tree.activeModal && !within(node, tree.activeModal)) return false;
  const href = link?.href ?? node.props.href;
  if (href) {
    const internal = link ? link.internal : !node.props.external;
    const native =
      modified ||
      nativeHandled ||
      !internal ||
      (link?.target ?? node.props.target) === '_blank' ||
      (link?.download ?? node.props.download) !== undefined;
    if (!native) {
      if (!link && node.props.onClick) activate(node.props.onClick);
      else events.navigate(href);
    }
    return true;
  }
  if (nativeHandled || node.props.nativeAction || node.props.field) return true;
  const handler = node.props.onClick;
  if (!handler) return false;
  activate(handler);
  return true;
}
