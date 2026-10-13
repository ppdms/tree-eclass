import type { CanvasFocus } from './focus';
import type { InteractionEvents } from './interaction';
import { activateTarget, alive, draggable, isControl } from './interaction-targets';
import type { InteractionScroll } from './interaction-scroll';
import { activate } from './react';
import type { CanvasSelection } from './selection';
import type { UITree } from './tree';
import type { CanvasKey } from './types';

interface KeyboardHost {
  tree: UITree;
  focus: CanvasFocus;
  selection: CanvasSelection;
  scrolling: InteractionScroll;
  events: InteractionEvents;
  cancel(): void;
}

function dispatch(host: KeyboardHost, event: CanvasKey): boolean {
  const start = host.focus.node;
  const activation = event.key === 'Enter' || event.key === ' ';
  for (let node = start; node; node = node.parent) {
    if (!alive(host.tree, node)) return false;
    const input = node.props.onKeyInput;
    if (input) {
      let handled = false;
      activate(() => {
        handled = input(event) === true;
      });
      if (handled) return true;
    }
    if (activation && isControl(node)) return false;
    if ((node.props.field || node.props.nativeAction) && event.key !== 'Tab' && event.key !== 'Escape') return false;
    if (isControl(node) && event.key !== 'Tab' && event.key !== 'Escape') {
      // Course reorder is an explicit drag-owner contract, not ordinary ancestor navigation.
      const reorder = draggable(node.parent)?.props.onKeyInput;
      if (!reorder) return false;
      let handled = false;
      activate(() => {
        handled = reorder(event) === true;
      });
      return handled;
    }
  }
  return false;
}

export function keyboard(host: KeyboardHost, event: CanvasKey): boolean {
  if (event.composing) return false;
  host.focus.keyInput(event);
  if (dispatch(host, event)) return true;
  if (event.key === 'Tab') return host.focus.tab(event.shift);
  const node = host.focus.node;
  if (event.key === 'Escape') {
    host.cancel();
    if (host.selection.hasRange) {
      host.selection.clear();
      return true;
    }
    return false;
  }
  if (node?.props.field) return false;
  if ((event.ctrl || event.meta) && !event.alt) {
    if (event.key.toLocaleLowerCase() === 'a') {
      host.selection.selectAll();
      return true;
    }
    if (event.key.toLocaleLowerCase() === 'c') return host.selection.hasRange;
    return false;
  }
  if (event.alt) return false;
  if (host.focus.menu(event.key)) return true;
  if ((event.key === 'Enter' || event.key === ' ') && host.focus.target) {
    if (event.nativeHandled || node?.props.nativeAction) return true;
    if (event.repeat && event.key === ' ') return true;
    if (activateTarget(host.tree, host.events, host.focus.target, event.shift)) return true;
  }
  return host.scrolling.key(node, event.key, event.shift);
}
