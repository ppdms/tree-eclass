import { TaffyTree, type MeasureFunction } from 'taffy-layout';
import type { Fonts } from './fonts';
import { MeasuredText } from './text';
import { resolveStyle, taffyStyle } from './styles';
import type { HostProps, LinkRect, Rect, UIStyle } from './types';
import { NodeScroll } from './scrolling';

export type HostKind = 'view' | 'text' | 'icon' | 'vector' | 'raster';

let nextNodeInstance = 0;

export class UINode {
  readonly instance = ++nextNodeInstance;
  readonly children: UINode[] = [];
  readonly text = new MeasuredText();
  readonly inlineLinks: LinkRect[] = [];
  parent: UINode | null = null;
  style: UIStyle = {};
  styleKey = '';
  nodeId = 0n;
  value = '';
  path: Path2D | null = null;
  pathValue = '';
  hidden = false;
  shown = false;
  fixed = false;
  disabled = false;
  scrolling: NodeScroll | null = null;
  childrenDirty = true;
  visibleCallback: (() => void) | undefined;
  layoutCallback: ((rect: Rect) => void) | undefined;
  notifiedRect: Rect | null = null;
  paintedAt = 0;
  paintedResource = 0;
  visibleResource = 0;
  editRevision = 0;
  rect: Rect = { x: 0, y: 0, width: 0, height: 0 };
  screen: Rect = { x: 0, y: 0, width: 0, height: 0 };
  clip: Rect = { x: 0, y: 0, width: 0, height: 0 };
  inset = { left: 0, right: 0, top: 0, bottom: 0 };

  constructor(
    readonly root: UITree,
    readonly kind: HostKind | 'raw',
    public props: HostProps,
  ) {}

  get content(): string {
    if (this.hidden) return '';
    if (this.props.runs) return this.props.runs.map((run) => run.text).join('');
    return this.kind === 'raw' ? this.value : this.children.map((child) => child.content).join('');
  }

  update(props: HostProps): void {
    if (this.parent && this.props.fixed !== props.fixed) this.parent.childrenDirty = true;
    this.props = props;
    this.root.dirty = true;
  }
}

export class UITree {
  readonly layout = new TaffyTree();
  readonly root: UINode;
  readonly nodes = new Map<bigint, UINode>();
  readonly fixedRoots: UINode[] = [];
  readonly focusRoots: UINode[] = [];
  readonly byId = new Map<string, UINode>();
  readonly removed = new Set<UINode>();
  activeModal: UINode | null = null;
  dirty = true;
  revision = 0;
  firstCommitAt = 0;
  commitMs = 0;
  layoutMs = 0;
  prepareMs = 0;
  computeMs = 0;
  #commitStart = 0;

  constructor(
    readonly fonts: Fonts,
    readonly invalidate: () => void,
  ) {
    this.layout.disableRounding();
    this.root = this.create('view', { style: { width: '100%' } });
  }

  create(kind: HostKind, props: HostProps): UINode {
    // React may abandon work before commit. Render-phase nodes own no WASM resources.
    return new UINode(this, kind, props);
  }

  raw(value: string): UINode {
    const node = new UINode(this, 'raw', {});
    node.value = value;
    return node;
  }

  insert(parent: UINode, child: UINode, before?: UINode): void {
    if (child.parent) this.detach(child.parent, child);
    const index = before ? parent.children.indexOf(before) : parent.children.length;
    parent.children.splice(index, 0, child);
    child.parent = parent;
    parent.childrenDirty = true;
    this.removed.delete(child);
    this.dirty = true;
  }

  detach(parent: UINode, child: UINode): void {
    const index = parent.children.indexOf(child);
    if (index < 0) throw new Error('React removed a child from the wrong canvas parent');
    parent.children.splice(index, 1);
    child.parent = null;
    parent.childrenDirty = true;
    this.removed.add(child);
    this.dirty = true;
  }

  beginCommit(): void {
    this.#commitStart = performance.now();
  }

  endCommit(): void {
    this.commitMs = performance.now() - this.#commitStart;
    if (!this.firstCommitAt) this.firstCommitAt = performance.timeOrigin + performance.now();
    this.revision++;
    this.dirty = true;
    this.invalidate();
  }

  compute(width: number, height: number): void {
    const start = performance.now();
    this.fixedRoots.length = 0;
    this.focusRoots.length = 0;
    this.byId.clear();
    this.activeModal = null;
    this.#prepare(this.root, {}, width, height, true, false, false);
    this.fixedRoots.sort(
      (a, b) =>
        Number(Boolean(a.props.modal)) - Number(Boolean(b.props.modal)) ||
        (a.style.zIndex ?? 0) - (b.style.zIndex ?? 0),
    );
    this.activeModal = this.fixedRoots.findLast((node) => node.props.modal) ?? null;
    this.focusRoots.sort((a, b) => Number(a.fixed) - Number(b.fixed));
    for (const node of this.removed) this.#dispose(node);
    this.removed.clear();
    const prepared = performance.now();
    this.prepareMs = prepared - start;
    this.layout.computeLayoutWithMeasure(this.root.nodeId, { width, height: 'max-content' }, this.#measure);
    for (const node of this.fixedRoots) {
      this.layout.computeLayoutWithMeasure(node.nodeId, { width, height }, this.#measure);
    }
    this.computeMs = performance.now() - prepared;
    this.#positions(this.root, 0, 0);
    for (const node of this.fixedRoots) this.#positionFixed(node, width, height);
    const now = performance.timeOrigin + performance.now();
    for (const node of this.nodes.values()) node.scrolling?.bounds(node, now);
    this.layoutMs = performance.now() - start;
    this.dirty = false;
  }

  readonly #measure: MeasureFunction = (known, available, id, _context) => {
    const node = this.nodes.get(id);
    if (node?.props.field && !node.props.field.multiline) return node.text.measureField(known, available);
    return node?.text.measure(known, available) ?? { width: 0, height: 0 };
  };

  #prepare(
    node: UINode,
    parent: UIStyle,
    width: number,
    height: number,
    shown: boolean,
    fixed: boolean,
    disabled: boolean,
  ): void {
    if (node.kind === 'raw') return;
    const style = resolveStyle(node.props.style, parent, width);
    node.style = style;
    node.shown = shown && !node.hidden && style.display !== 'none';
    node.fixed = fixed || Boolean(node.props.fixed);
    node.disabled = disabled || Boolean(node.props.disabled);
    if (node.props.scroll && !node.scrolling) node.scrolling = new NodeScroll();
    if (!node.props.scroll) node.scrolling = null;
    if (node.shown && node.props.id) this.byId.set(node.props.id, node);
    if (node.shown && node.props.focusScope) this.focusRoots.push(node);
    if (node.shown && node.props.fixed) this.fixedRoots.push(node);
    if (node.shown && node.props.modal) this.activeModal = node;
    const layoutStyle = node.props.fixed ? fixedStyle(style, width, height) : style;
    const key = JSON.stringify([layoutStyle, node.shown]);
    if (key !== node.styleKey) {
      const value = taffyStyle(layoutStyle, false, !node.shown);
      if (node.nodeId) this.layout.setStyle(node.nodeId, value);
      else {
        node.nodeId = node.kind === 'text' ? this.layout.newLeafWithContext(value, true) : this.layout.newLeaf(value);
        this.nodes.set(node.nodeId, node);
      }
      value.free();
      node.styleKey = key;
    }
    if (node.kind === 'text' && node.shown && node.text.update(this.fonts, node.content, style, node.props.runs)) {
      this.layout.markDirty(node.nodeId);
    }
    if (node.kind === 'view') {
      for (const child of node.children) {
        this.#prepare(child, style, width, height, node.shown, node.fixed, node.disabled);
      }
    }
    if (node.kind === 'view' && node.childrenDirty) {
      this.layout.setChildren(
        node.nodeId,
        node.children.filter((child) => child.kind !== 'raw' && !child.props.fixed).map((c) => c.nodeId),
      );
    }
    node.childrenDirty = false;
  }

  #positions(node: UINode, x: number, y: number): void {
    if (node.kind === 'raw') return;
    const box = this.layout.getLayout(node.nodeId);
    const [position, size, padding, border] = box.get('position', 'size', 'padding', 'border');
    box.free();
    node.rect = { x: x + position.x, y: y + position.y, width: size.width, height: size.height };
    node.inset = {
      left: padding.left + border.left,
      right: padding.right + border.right,
      top: padding.top + border.top,
      bottom: padding.bottom + border.bottom,
    };
    if (node.kind === 'text') {
      const singleLine = node.props.field && !node.props.field.multiline;
      node.text.layout(singleLine ? 100000 : size.width - node.inset.left - node.inset.right);
      this.#links(node);
    }
    if (node.kind === 'view') {
      for (const child of node.children) {
        if (!child.props.fixed) this.#positions(child, node.rect.x, node.rect.y);
      }
    }
  }

  #positionFixed(node: UINode, width: number, height: number): void {
    const box = this.layout.getLayout(node.nodeId);
    const [position, size] = box.get('position', 'size');
    box.free();
    const x = node.style.left ?? (node.style.right === undefined ? 0 : width - node.style.right - size.width);
    const y = node.style.top ?? (node.style.bottom === undefined ? 0 : height - node.style.bottom - size.height);
    this.#positions(node, x - position.x, y - position.y);
  }

  #links(node: UINode): void {
    node.inlineLinks.length = 0;
    const paragraph = node.text.paragraph;
    if (!paragraph || !node.props.runs) return;
    let offset = 0;
    for (const [runIndex, run] of node.props.runs.entries()) {
      const end = offset + run.text.length;
      if (run.href) {
        for (const rect of paragraph.rectsForRange(offset, end)) {
          node.inlineLinks.push({
            x: node.inset.left + rect.x,
            y: node.inset.top + rect.y,
            width: rect.width,
            height: rect.height,
            href: run.href,
            title: run.title,
            runIndex,
            start: offset,
            end,
          });
        }
      }
      offset = end;
    }
  }

  #dispose(node: UINode): void {
    node.shown = false;
    for (const child of node.children) this.#dispose(child);
    node.text.dispose();
    node.path = null;
    if (node.kind !== 'raw' && this.nodes.delete(node.nodeId)) this.layout.remove(node.nodeId);
  }
}

function fixedStyle(style: UIStyle, width: number, height: number): UIStyle {
  return {
    ...style,
    position: 'relative',
    top: undefined,
    bottom: undefined,
    left: undefined,
    right: undefined,
    width: style.width ?? width - (style.left ?? 0) - (style.right ?? 0),
    height:
      style.height ??
      (style.top !== undefined && style.bottom !== undefined ? height - style.top - style.bottom : undefined),
  };
}
