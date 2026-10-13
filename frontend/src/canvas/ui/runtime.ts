import type { ReactNode } from 'react';
import { Renderer } from '../renderer';
import { Scroller } from '../scroll';
import type { Viewport } from '../protocol';
import type { NativeEditInput } from '../app/protocol';
import type { ScrollToOptions } from '../app/browser';
import { getViewport, updateViewport } from '../app/viewport';
import { UIPainter } from './paint';
import { mount } from './react';
import { notifyVisible, snapshot, visibleLinks } from './query';
import { Interaction, type CanvasPointer, type FocusDescriptor } from './interaction';
import type { Fonts } from './fonts';
import { UITree } from './tree';
import type { CanvasKey, LinkRect, NodeSnapshot, Rect } from './types';
import { publishGeometry, updateGeometry, visibleRect } from './geometry';
import { configureImages } from './resources';
import { NativeInputs, type NativeFields } from './nativeInputs';
import { notifyLayoutObservers } from './layoutEvents';

interface RuntimeEvents {
  metrics(values: Record<string, number>, nodes: NodeSnapshot[], renderer: string): void;
  links(items: LinkRect[]): void;
  fields(fields: NativeFields): void;
  cursor(value: 'pointer' | 'default' | 'text', title?: string): void;
  navigate(href: string): void;
  focus(target: FocusDescriptor | null): void;
  selection(text: string): void;
  scroll(offset: number): void;
  error(error: Error): void;
}

function percentile(values: number[]): number {
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * 0.95))] ?? 0;
}

export class UIRuntime {
  readonly tree: UITree;
  readonly scroller = new Scroller();
  readonly input: NativeInputs;
  readonly #painter: UIPainter;
  readonly #interaction: Interaction;
  readonly #frames: number[] = [];
  readonly #gaps: number[] = [];
  #scheduled = false;
  #lastFrame = 0;
  #published = 0;
  #links = '';
  #fields = '';
  #blink: number | undefined;
  #anchor: string | null = null;
  #publishedScroll: number | undefined;
  #restoration: { revision: number; offset: number | null; anchor: string | null } | null = null;
  readonly #render = (time: number): void => {
    try {
      this.#tick(time);
    } catch (error) {
      this.events.error(error instanceof Error ? error : new Error(String(error)));
    }
  };

  constructor(
    readonly renderer: Renderer,
    fonts: Fonts,
    readonly events: RuntimeEvents,
    readonly values: Record<string, number>,
    readonly navigationStart: number,
  ) {
    this.tree = new UITree(fonts, () => this.invalidate());
    this.#painter = new UIPainter(this.tree);
    this.#interaction = new Interaction(this.tree, this.scroller, this.#painter, {
      ...events,
      invalidate: () => this.invalidate(),
    });
    this.input = new NativeInputs(this.tree, this.#painter.fields, () => this.invalidate());
    configureImages(() => this.invalidate());
  }

  mount(element: ReactNode): void {
    mount(this.tree, element, this.events.error);
  }

  resize(viewport: Viewport): void {
    const previous = getViewport();
    updateViewport({
      ...previous,
      ...viewport,
      bottom: previous.bottom === previous.height ? viewport.height : Math.min(previous.bottom, viewport.height),
    });
    this.renderer.resize(viewport);
    this.tree.dirty = true;
    this.invalidate();
  }

  insets(top: number, bottom: number): void {
    updateViewport({ ...getViewport(), top, bottom });
    this.tree.dirty = true;
    this.invalidate();
  }

  wheel(x: number, y: number, dx: number, dy: number): void {
    this.#releaseAnchor();
    this.#geometry();
    this.#interaction.wheel(x, y, dx, dy);
  }

  restoreScroll(offset: number | null, anchor: string | null = null, clearInteraction = true): void {
    if (clearInteraction) {
      this.#interaction.cancel();
      this.#interaction.clearSelection();
      this.#painter.fields.state = null;
    }
    this.#anchor = null;
    this.#restoration = { revision: this.tree.revision, offset, anchor };
    this.invalidate();
  }

  pointer(input: CanvasPointer): void {
    if (input.phase === 'down') this.#releaseAnchor();
    this.#geometry();
    this.#interaction.pointer(input);
  }
  key(event: CanvasKey): void {
    this.#releaseAnchor();
    this.#geometry();
    this.#interaction.key(event);
  }
  contextMenu(x: number, y: number): void {
    this.#geometry();
    this.#interaction.contextMenu(x, y);
  }
  selectAll(): void {
    this.#geometry();
    this.#interaction.selectAll();
  }
  clearSelection(): void {
    this.#interaction.clearSelection();
  }
  focusNode(id: string): void {
    this.#geometry();
    this.#interaction.focusNode(id);
  }
  focusField(id: string): void {
    this.#geometry();
    this.#interaction.focusField(id);
  }
  capturePointer(id: string, pointerId: number): void {
    this.#interaction.capturePointer(id, pointerId);
  }
  releasePointer(pointerId: number): void {
    this.#interaction.releasePointer(pointerId);
  }
  edit(input: NativeEditInput): void {
    this.#geometry();
    this.input.edit(input);
  }

  getNodeRect(id: string): Rect | null {
    const node = this.tree.byId.get(id);
    return node ? { ...node.screen } : null;
  }

  scrollPageTo(offset: number, options?: ScrollToOptions): void {
    this.#releaseAnchor();
    this.#geometry();
    this.#scroll(this.scroller, offset, options);
    this.invalidate();
  }

  scrollAreaTo(id: string, offset: number, options?: ScrollToOptions): void {
    this.#geometry();
    const scroll = this.tree.byId.get(id)?.scrolling;
    if (!scroll) return;
    this.#scroll(scroll.y, offset, options);
    if (options?.offsetX !== undefined) this.#scroll(scroll.x, options.offsetX, options);
    this.invalidate();
  }

  scrollToNode(id: string): void {
    // Disclosure/content updates in the same React turn must commit before the
    // anchor is consumed. A prepared explicit target supersedes route restoration.
    this.#anchor = id;
    this.#restoration = null;
    this.invalidate();
  }

  #scroll(scroller: Scroller, offset: number, options?: ScrollToOptions): void {
    if (options?.behavior === 'smooth' && !getViewport().reducedMotion) {
      scroller.animateTo(offset, performance.timeOrigin + performance.now());
    } else scroller.wheel(offset - scroller.offset);
  }

  #releaseAnchor(): void {
    this.#anchor = null;
    this.#restoration = null;
  }

  #revealAnchor(): void {
    if (!this.#anchor) return;
    const node = this.tree.byId.get(this.#anchor);
    if (!node) return;
    // Keep the requested fragment anchored while asynchronous content above it
    // grows. Explicit scrolling or user input releases it, never a time heuristic.
    let y = node.screen.y;
    for (let parent = node.parent; parent; parent = parent.parent) {
      const scroll = parent.scrolling;
      if (!scroll) continue;
      const before = scroll.y.offset;
      scroll.y.wheel(y - parent.screen.y);
      y -= scroll.y.offset - before;
    }
    if (!node.fixed) this.scroller.wheel(y - getViewport().top - 88);
    updateGeometry(this.tree, this.scroller.offset, getViewport());
  }

  invalidate(): void {
    if (this.#scheduled || !this.tree.firstCommitAt) return;
    this.#scheduled = true;
    requestAnimationFrame(this.#render);
  }

  #tick(time: number): void {
    this.#scheduled = false;
    const start = performance.now();
    const gap = this.#lastFrame ? time - this.#lastFrame : 16;
    this.#lastFrame = time;
    this.#geometry();
    const now = performance.timeOrigin + time;
    let moving = this.scroller.step(now) || this.scroller.dragging;
    for (const node of this.tree.nodes.values()) moving = Boolean(node.scrolling?.step(now)) || moving;
    moving = this.#painter.step(Math.min(0.05, gap / 1000)) || moving;
    updateGeometry(this.tree, this.scroller.offset, getViewport());
    this.#revealAnchor();
    this.#interaction.synchronize();
    const phases = this.renderer.frame(
      (context, dpr) => this.#painter.content(context, dpr),
      (chrome) => this.#painter.chrome(chrome),
    );
    this.values.contentDrawMs = phases.content;
    this.values.chromeDrawMs = phases.chrome;
    this.values.compositeMs = phases.composite;
    if (this.#publishedScroll !== this.scroller.offset) {
      this.#publishedScroll = this.scroller.offset;
      this.events.scroll(this.scroller.offset);
    }
    this.#firstContent();
    this.#recordFrame(start, gap);
    this.#postNative();
    if (time - this.#published > 250 || !moving) this.inspect();
    publishGeometry(this.tree);
    notifyLayoutObservers();
    notifyVisible(this.tree, this.#painter.version);
    if (moving || this.#painter.emerging) this.invalidate();
    this.#scheduleBlink();
  }

  #firstContent(): void {
    if (this.values.firstContentMs) return;
    let ready = false;
    for (const node of this.tree.nodes.values()) {
      if (!node.shown) continue;
      if (node.props.pending && visibleRect(node)) return;
      ready ||= Boolean(node.props.ready);
    }
    if (ready) this.values.firstContentMs = performance.timeOrigin + performance.now() - this.navigationStart;
  }

  #recordFrame(start: number, gap: number): void {
    this.#frames.push(performance.now() - start);
    if (gap < 250) this.#gaps.push(gap);
    if (this.#frames.length > 180) this.#frames.shift();
    if (this.#gaps.length > 180) this.#gaps.shift();
  }

  #geometry(): void {
    // Browser input can arrive before React has committed a tree to lay out.
    if (!this.tree.firstCommitAt) return;
    const viewport = getViewport();
    if (this.tree.dirty) this.#layout(viewport.width, viewport.height);
    updateGeometry(this.tree, this.scroller.offset, viewport);
  }

  #layout(width: number, height: number): void {
    this.tree.compute(width, height);
    const viewport = getViewport();
    this.scroller.setBounds(
      -viewport.top,
      this.tree.root.rect.height - viewport.bottom,
      viewport.bottom - viewport.top,
      performance.timeOrigin + performance.now(),
    );
    const restore = this.#restoration;
    if (restore && this.tree.revision > restore.revision) {
      if (restore.offset !== null) this.scroller.wheel(restore.offset - this.scroller.offset);
      this.#anchor = restore.anchor;
      this.#restoration = null;
    }
    this.values.maxLayoutMs = Math.max(this.values.maxLayoutMs ?? 0, this.tree.layoutMs);
    this.values.maxCommitMs = Math.max(this.values.maxCommitMs ?? 0, this.tree.commitMs);
    if (!this.values.firstCommitMs) this.values.firstCommitMs = this.tree.firstCommitAt - this.navigationStart;
  }

  #postNative(): void {
    const links = visibleLinks(this.tree);
    const linkSignature = JSON.stringify(links);
    if (linkSignature !== this.#links) {
      this.#links = linkSignature;
      this.events.links(links);
    }
    const fields = this.input.snapshot();
    const fieldSignature = JSON.stringify(fields);
    if (fieldSignature !== this.#fields) {
      this.#fields = fieldSignature;
      this.events.fields(fields);
    }
  }

  #scheduleBlink(): void {
    if (!this.#painter.fields.state?.active) {
      clearTimeout(this.#blink);
      this.#blink = undefined;
    } else if (this.#blink === undefined) {
      this.#blink = self.setTimeout(
        () => {
          this.#blink = undefined;
          this.invalidate();
        },
        500 - (performance.now() % 500),
      );
    }
  }

  inspect(): void {
    if (this.tree.dirty) {
      this.invalidate();
      return;
    }
    this.#published = performance.now();
    Object.assign(this.values, {
      scroll: this.scroller.offset,
      contentHeight: this.tree.root.rect.height,
      nodes: this.tree.nodes.size,
      drawn: this.#painter.drawn,
      commits: this.tree.revision,
      commitMs: this.tree.commitMs,
      layoutMs: this.tree.layoutMs,
      prepareMs: this.tree.prepareMs,
      computeMs: this.tree.computeMs,
      frameMs: this.#frames.at(-1) ?? 0,
      frameP95: percentile(this.#frames),
      frameGapP95: percentile(this.#gaps),
    });
    this.events.metrics(this.values, snapshot(this.tree), this.renderer.label);
  }
}
