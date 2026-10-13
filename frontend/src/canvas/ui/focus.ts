import type { Scroller } from '../scroll';
import type { FocusDescriptor, InteractionEvents } from './interaction';
import { alive, ancestry, byId, changeWithin, isControl, linkIdentity, within } from './interaction-targets';
import type { UIPainter } from './paint';
import { focusableNodes } from './query';
import type { UINode, UITree } from './tree';
import type { CanvasKey, LinkRect } from './types';

const MODIFIERS = new Set(['Shift', 'Control', 'Alt', 'Meta', 'CapsLock']);

export interface FocusTarget {
  node: UINode;
  link?: LinkRect;
}

interface ScopeEntry {
  node: UINode;
  previous: FocusTarget | null;
}

export class CanvasFocus {
  target: FocusTarget | null = null;
  #scopes: ScopeEntry[] = [];
  #ancestors: UINode[] = [];
  #descriptor = '';
  #leavingMenu: UINode | null = null;
  /** Browser `:focus-visible` modality: programmatic focus shows a ring only after keyboard use. */
  #keyboard = false;

  constructor(
    readonly tree: UITree,
    readonly painter: UIPainter,
    readonly events: InteractionEvents,
    readonly scroller: Scroller,
  ) {}

  get node(): UINode | null {
    return this.target?.node ?? null;
  }

  /** Records keyboard modality like browsers do; shortcuts and bare modifiers do not count. */
  keyInput(event: CanvasKey): void {
    if (!event.meta && !event.ctrl && !event.alt && !MODIFIERS.has(event.key)) this.#keyboard = true;
  }

  descriptor(target = this.target): FocusDescriptor | null {
    if (!target || !alive(this.tree, target.node)) return null;
    const { node, link } = target;
    const action = node.props.nativeAction;
    const id = node.props.id ?? `canvas-node:${node.nodeId}`;
    const href = link?.href ?? node.props.href;
    const descriptor: FocusDescriptor = { id: link ? `${id}:inline:${linkIdentity(link)}` : id };
    if (href) {
      descriptor.href = href;
      descriptor.internal = link ? link.internal : !node.props.external;
      descriptor.target = link?.target ?? node.props.target;
      descriptor.download = link?.download ?? node.props.download;
      descriptor.rel = node.props.rel;
    }
    if (node.props.field) descriptor.nativeKind = 'text';
    else if (action?.kind === 'file' || action?.kind === 'copy') descriptor.nativeKind = action.kind;
    return descriptor;
  }

  set(target: FocusTarget | null, visible: boolean, reveal = true, forceNative = false): void {
    if (target && !alive(this.tree, target.node)) target = null;
    const modal = this.#modal();
    if (target && modal && !within(target.node, modal)) return;
    this.target = target;
    this.painter.focused = target?.node ?? null;
    this.painter.focusedLink = target?.link ?? null;
    this.painter.focusVisible = visible;
    const ancestors = ancestry(target?.node ?? null);
    changeWithin(this.#ancestors, ancestors, 'focus');
    this.#ancestors = ancestors;
    this.#publish(forceNative);
    if (target && reveal) this.#reveal(target.node);
    this.events.invalidate();
  }

  focusNode(id: string, fieldOnly = false): void {
    const node = byId(this.tree, id);
    if (!node || (fieldOnly && !node.props.field)) return;
    const scope = this.#scopes[this.#scopes.length - 1]?.node;
    if (scope?.props.focusScope === 'menu' && !within(node, scope)) this.#leavingMenu = scope;
    this.set({ node }, this.#keyboard, true, true);
  }

  focusPointer(node: UINode | null, link?: LinkRect): void {
    this.#keyboard = false;
    if (node?.props.nativeAction?.kind === 'focus-field') {
      this.focusNode(node.props.nativeAction.fieldId, true);
      return;
    }
    if (link && node) {
      this.set({ node, link }, false, false);
      return;
    }
    for (; node; node = node.parent) {
      if (node.props.focusable !== false && (node.props.focusable || isControl(node) || node.props.dragId)) {
        this.set({ node }, false, false);
        return;
      }
    }
    this.set(null, false, false);
  }

  targets(scope?: UINode | null): FocusTarget[] {
    const targets: FocusTarget[] = [];
    for (const node of focusableNodes(this.tree, scope)) {
      if (!alive(this.tree, node)) continue;
      if (node.props.focusable !== false && (node.props.focusable || isControl(node) || node.props.dragId)) {
        if (node.props.nativeAction?.kind !== 'focus-field') targets.push({ node });
      }
      const seen = new Set<string>();
      for (const link of node.inlineLinks) {
        const identity = linkIdentity(link);
        if (!seen.has(identity)) {
          targets.push({ node, link });
          seen.add(identity);
        }
      }
    }
    return targets;
  }

  tab(reverse: boolean): boolean {
    const scope = this.#traversalScope();
    const targets = this.targets(scope);
    if (!targets.length) {
      if (scope?.props.focusScope === 'dialog') this.set({ node: scope }, true);
      return !!scope;
    }
    const current = this.#index(targets);
    const index =
      current < 0
        ? reverse
          ? targets.length - 1
          : 0
        : (current + (reverse ? -1 : 1) + targets.length) % targets.length;
    this.set(targets[index]!, true);
    return true;
  }

  menu(key: string): boolean {
    let scope = this.node;
    while (scope && scope.props.focusScope !== 'menu') scope = scope.parent;
    if (!scope || !['ArrowDown', 'ArrowUp', 'ArrowLeft', 'ArrowRight', 'Home', 'End'].includes(key)) return false;
    const targets = this.targets(scope);
    if (!targets.length) return true;
    const index = this.#index(targets);
    const reverse = key === 'ArrowUp' || key === 'ArrowLeft';
    const next =
      key === 'Home'
        ? 0
        : key === 'End'
          ? targets.length - 1
          : index < 0
            ? reverse
              ? targets.length - 1
              : 0
            : (index + (reverse ? -1 : 1) + targets.length) % targets.length;
    this.set(targets[next]!, true);
    return true;
  }

  synchronize(): void {
    const roots = this.tree.focusRoots.filter((node) => alive(this.tree, node));
    if (this.#leavingMenu && !roots.includes(this.#leavingMenu)) this.#leavingMenu = null;
    let shared = 0;
    while (shared < roots.length && this.#scopes[shared]?.node === roots[shared]) shared++;
    const removed = this.#scopes.splice(shared);
    if (
      removed.length &&
      (!this.node || !alive(this.tree, this.node) || removed.some((entry) => within(this.node, entry.node)))
    ) {
      this.set(this.#restore(removed[0]!.previous), this.painter.focusVisible, false);
    }
    for (let index = shared; index < roots.length; index++) this.#enter(roots[index]!);
    if (this.target && !alive(this.tree, this.target.node)) this.set(null, this.painter.focusVisible, false);
    if (this.target?.link) {
      const identity = linkIdentity(this.target.link);
      const link = this.target.node.inlineLinks.find((item) => linkIdentity(item) === identity);
      if (link) {
        this.target.link = link;
        this.painter.focusedLink = link;
      } else this.set(null, this.painter.focusVisible, false);
    }
    const ancestors = ancestry(this.node);
    changeWithin(this.#ancestors, ancestors, 'focus');
    this.#ancestors = ancestors;
    const scope = roots[roots.length - 1];
    if (scope?.props.focusScope === 'dialog' && !within(this.node, scope)) {
      this.set(this.targets(scope)[0] ?? { node: scope }, this.#keyboard);
    }
    this.#publish();
  }

  #enter(node: UINode): void {
    this.#scopes.push({ node, previous: this.target });
    if (node.props.focusScope === 'region') return;
    if (!within(this.node, node)) this.set(this.targets(node)[0] ?? { node }, this.#keyboard);
  }

  #restore(target: FocusTarget | null): FocusTarget | null {
    if (!target) return null;
    const id = target.node.props.id;
    const node = alive(this.tree, target.node) ? target.node : id ? byId(this.tree, id) : null;
    if (!node) return null;
    const link = target.link
      ? node.inlineLinks.find((item) => linkIdentity(item) === linkIdentity(target.link!))
      : undefined;
    return target.link && !link ? null : { node, link };
  }

  #traversalScope(): UINode | null {
    for (let index = this.#scopes.length - 1; index >= 0; index--) {
      const node = this.#scopes[index]!.node;
      if (alive(this.tree, node) && (!this.node || within(this.node, node))) return node;
    }
    return null;
  }

  #modal(): UINode | null {
    const modal = this.tree.activeModal;
    if (!modal || !this.#leavingMenu || (!within(modal, this.#leavingMenu) && !within(this.#leavingMenu, modal)))
      return modal;
    for (let index = this.#scopes.length - 1; index >= 0; index--) {
      const node = this.#scopes[index]!.node;
      if (node.props.focusScope === 'dialog') return node;
    }
    return null;
  }

  #index(targets: FocusTarget[]): number {
    return targets.findIndex(
      (target) =>
        target.node === this.node &&
        ((!target.link && !this.target?.link) ||
          (!!target.link && !!this.target?.link && linkIdentity(target.link) === linkIdentity(this.target.link))),
    );
  }

  #publish(force = false): void {
    const descriptor = this.descriptor();
    const signature = JSON.stringify(descriptor);
    if (!force && signature === this.#descriptor) return;
    this.#descriptor = signature;
    this.events.focus(descriptor);
  }

  #reveal(node: UINode): void {
    let x = node.screen.x;
    let y = node.screen.y;
    const now = performance.timeOrigin + performance.now();
    for (let parent = node.parent; parent; parent = parent.parent) {
      const scroll = parent.scrolling;
      if (!scroll) continue;
      const rect = parent.screen;
      const dx = x < rect.x ? x - rect.x : Math.max(0, x + node.screen.width - rect.x - rect.width);
      const dy = y < rect.y ? y - rect.y : Math.max(0, y + node.screen.height - rect.y - rect.height);
      scroll.x.animateTo(scroll.x.offset + dx, now);
      scroll.y.animateTo(scroll.y.offset + dy, now);
      x -= dx;
      y -= dy;
    }
    if (node.fixed || this.tree.activeModal) return;
    const clip = this.tree.root.clip;
    const dy = y < clip.y ? y - clip.y : Math.max(0, y + node.screen.height - clip.y - clip.height);
    if (dy) this.scroller.animateTo(this.scroller.offset + dy, now);
  }
}
