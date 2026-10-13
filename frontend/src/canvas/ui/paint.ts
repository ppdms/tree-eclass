import { barGlass, overlayGlass, type ChromeOp, type ChromeRecorder, type GlassGlow, type GlassStyle } from '../chrome';
import { palette, type Palette } from '../theme';
import { Emergence, type MorphFrame } from './emerge';
import { Icons } from './icons';
import { FieldPaint } from './fieldPaint';
import { OpacityLayers, subtreeExtent } from './paintLayers';
import type { UINode, UITree } from './tree';
import type { Rect } from './types';
import { color, colors, currentPalette } from './palette';
import { rasterImage } from './resources';
import { getViewport } from '../app/viewport';

type Context = OffscreenCanvasRenderingContext2D;
interface Clip {
  rect: Rect;
  radius: number;
}

function apply(context: Context, clip: Clip): void {
  context.save();
  context.beginPath();
  context.roundRect(clip.rect.x, clip.rect.y, clip.rect.width, clip.rect.height, clip.radius);
  context.clip();
}

/**
 * Paints content into the base Canvas2D surface and the navigation layer into the
 * chrome recorder. Glass exists only in fixed layers; elsewhere it is its solid fallback.
 */
export class UIPainter {
  readonly fields = new FieldPaint();
  readonly #icons = new Icons();
  readonly #layers = new OpacityLayers();
  readonly #hover = new Map<UINode, number>();
  /** Pointer energy per glass surface; entries register when drawn and retire once dark. */
  readonly #glow = new Map<UINode, number>();
  readonly #emerge: Emergence;
  readonly #clips: Clip[] = [];
  readonly selectionRects = new Map<UINode, Rect[]>();
  #palette: Palette;
  #theme = currentPalette();
  #context: Context | null = null;
  #chrome: ChromeRecorder | null = null;
  #layerDepth = 0;
  #dpr = 1;
  hovered: UINode | null = null;
  pressed: UINode | null = null;
  pointer: { x: number; y: number } | null = null;
  focused: UINode | null = null;
  focusedLink: Rect | null = null;
  focusVisible = false;
  drawn = 0;
  version = 0;

  constructor(readonly tree: UITree) {
    this.#emerge = new Emergence(tree);
    this.#palette = palette(this.#theme === 'dark', color(colors.page));
  }

  step(dt: number): boolean {
    let moving = false;
    const animate = this.#animate();
    if (this.hovered && !this.#hover.has(this.hovered)) this.#hover.set(this.hovered, 0);
    for (const [node, value] of this.#hover) {
      if (!node.shown) {
        this.#hover.delete(node);
        continue;
      }
      const target = node === this.hovered ? 1 : 0;
      const next = !animate ? target : target > value ? Math.min(1, value + dt / 0.14) : Math.max(0, value - dt / 0.14);
      if (next === 0) this.#hover.delete(node);
      else this.#hover.set(node, next);
      moving ||= next !== target;
    }
    for (const [node, value] of this.#glow) {
      const target = node.shown ? this.#glowTarget(node) : 0;
      const rise = target > value;
      const step = rise ? dt / 0.12 : -dt / 0.3;
      const next = !animate ? target : rise ? Math.min(target, value + step) : Math.max(target, value + step);
      if (next === 0 && target === 0) this.#glow.delete(node);
      else this.#glow.set(node, next);
      moving ||= next !== target;
    }
    return this.#emerge.step(dt, animate) || moving;
  }

  /** Surfaces registered during this frame's draw still need the next frame. */
  get emerging(): boolean {
    return this.#emerge.active;
  }

  #animate(): boolean {
    return !getViewport().reducedMotion && currentPalette() !== 'eink';
  }

  /** Tracks the pointer for glass highlights; null once it leaves the content. */
  point(input: { x: number; y: number } | null): void {
    if (!input) {
      this.pointer = null;
      return;
    }
    const point = (this.pointer ??= { x: 0, y: 0 });
    point.x = input.x;
    point.y = input.y;
  }

  /** Pointer over a glass surface lights it; pressing one of its controls lights it fully. */
  #glowTarget(node: UINode): number {
    const p = this.pointer;
    const r = node.screen;
    if (!p || p.x < r.x || p.y < r.y || p.x > r.x + r.width || p.y > r.y + r.height) return 0;
    for (let at = this.pressed; at; at = at.parent) if (at === node) return 1;
    return 0.45;
  }

  content(context: Context, dpr: number): void {
    const theme = currentPalette();
    if (theme !== this.#theme) {
      this.#palette = palette(theme === 'dark', color(colors.page));
      this.#theme = theme;
    }
    this.#context = context;
    this.#chrome = null;
    this.#dpr = dpr;
    context.fillStyle = color(colors.page);
    context.fillRect(0, 0, context.canvas.width / dpr, context.canvas.height / dpr);
    this.drawn = 0;
    this.version++;
    this.#draw(this.tree.root, false);
  }

  chrome(recorder: ChromeRecorder): void {
    this.#chrome = recorder;
    this.#context = recorder.context;
    recorder.palette = this.#palette;
    if (currentPalette() !== 'eink') {
      for (const node of this.tree.fixedRoots) {
        if (!node.style.scrollEdge || !this.#visible(node)) continue;
        const barBottom = node.screen.y + node.screen.height;
        recorder.queue({ kind: 'edge', top: barBottom - 8, bottom: barBottom + 10 }, 0);
      }
    }
    for (const node of this.tree.fixedRoots) this.#draw(node, true);
    this.#chrome = null;
  }

  #visible(node: UINode): boolean {
    const r = node.screen;
    const c = node.clip;
    return (
      node.shown &&
      node.kind !== 'raw' &&
      r.width > 0 &&
      r.height > 0 &&
      c.width > 0 &&
      c.height > 0 &&
      r.x + r.width > c.x &&
      r.x < c.x + c.width &&
      r.y + r.height > c.y &&
      r.y < c.y + c.height
    );
  }

  #draw(node: UINode, fixed: boolean, material = barGlass): void {
    if (!this.#visible(node) || node.fixed !== fixed) return;
    if (node.style.glass === 'overlay') material = overlayGlass;
    this.drawn++;
    const clipped = node.props.scroll || node.style.overflow === 'hidden';
    if (clipped) this.#push({ rect: node.screen, radius: node.style.borderRadius ?? 0 });
    const opacity = node.style.opacity ?? 1;
    const layer = opacity < 1 ? this.#layers.begin(this.#context!, subtreeExtent(node), this.#dpr) : null;
    if (layer) {
      this.#context = layer.context;
      this.#layerDepth++;
    }
    const morph = this.#morph(node);
    this.#background(node, material, morph);
    if (morph) this.#push({ rect: morph.rect, radius: morph.radius });
    this.#paintNode(node);
    if (node.kind === 'view') {
      for (const child of node.children) {
        if (!child.props.fixed) this.#draw(child, fixed, material);
      }
    }
    this.#focus(node);
    if (morph) this.#endMorph(morph);
    if (layer) {
      this.#layerDepth--;
      this.#context = this.#layers.end(layer, opacity);
    }
    if (clipped) this.#pop();
  }

  #push(clip: Clip): void {
    this.#clips.push(clip);
    apply(this.#context!, clip);
  }

  #pop(): void {
    this.#clips.pop();
    this.#context!.restore();
  }

  /** Queues a compositor operation above the chrome drawn so far, restoring clips if the segment changed. */
  #queue(op: ChromeOp | null, alpha: number): void {
    const chrome = this.#chrome!;
    if (!chrome.queue(op, this.#clips.length, alpha)) return;
    for (const clip of this.#clips) apply(chrome.context, clip);
  }

  #morph(node: UINode): MorphFrame | null {
    if (!this.#chrome || !node.props.morphFrom || !node.style.glass || currentPalette() === 'eink') return null;
    return this.#emerge.frame(node, node.style.borderRadius ?? 22, this.#animate());
  }

  #endMorph(morph: MorphFrame): void {
    this.#pop();
    if (morph.alpha < 1) this.#queue(null, 1);
  }

  #paintNode(node: UINode): void {
    if (node.kind === 'text') this.#text(node);
    else if (node.kind === 'vector') this.#vector(node);
    else if (node.kind === 'raster') this.#raster(node);
    else if (node.kind === 'icon' && node.props.name) {
      this.#icons.draw(this.#context!, node.props.name, node.screen, color(node.style.color ?? colors.text));
      this.#mark(node.screen, 2);
    }
  }

  #background(node: UINode, material: GlassStyle, morph: MorphFrame | null): void {
    const s = node.style;
    const rect = morph?.rect ?? node.screen;
    const radius = morph?.radius ?? s.borderRadius ?? 0;
    const glass = Boolean(s.glass) && this.#chrome !== null && this.#layerDepth === 0 && currentPalette() !== 'eink';
    const scrim = this.#chrome && node.props.modal && s.backgroundColor ? color(s.backgroundColor) : '';
    if (glass) {
      const glow = this.#glowFor(node);
      const op: ChromeOp = {
        kind: 'glass',
        rect: { ...rect },
        radius: morph?.radius ?? s.borderRadius ?? 22,
        style: material,
        glow,
      };
      this.#queue(op, morph?.alpha ?? 1);
    } else if (scrim.startsWith('#') && !radius) {
      // A modal scrim covers the viewport; the compositor fills it instead of uploading it.
      this.#queue({ kind: 'fill', rect: { ...node.screen }, color: scrim }, 1);
    } else if (s.backgroundColor || s.glass) {
      // Without glass (e-ink or content), the surface and border tokens are the material's solid fallback.
      this.#fill(rect, radius, color(s.backgroundColor ?? colors.surface), 1);
    }
    const hover = this.#hover.get(node) ?? 0;
    if ((hover > 0 || node === this.pressed) && s.hoverBackground && !node.disabled) {
      // Animate within the token's own alpha: fills on glass are translucent by design.
      this.#fill(rect, radius, color(s.hoverBackground), (node === this.pressed ? 1 : hover) * (glass ? 0.25 : 1));
    }
    if (!glass) this.#border(node, rect, radius);
  }

  #glowFor(node: UINode): GlassGlow | null {
    if (!this.#glow.has(node)) this.#glow.set(node, 0);
    const energy = this.#glow.get(node) ?? 0;
    return energy > 0 && this.pointer ? { x: this.pointer.x, y: this.pointer.y, energy } : null;
  }

  #fill(rect: Rect, radius: number, fill: string, alpha: number): void {
    const context = this.#context!;
    context.globalAlpha = alpha;
    context.fillStyle = fill;
    if (radius > 0) {
      context.beginPath();
      context.roundRect(rect.x, rect.y, rect.width, rect.height, radius);
      context.fill();
    } else context.fillRect(rect.x, rect.y, rect.width, rect.height);
    context.globalAlpha = 1;
    this.#mark(rect, 1);
  }

  #border(node: UINode, rect: Rect, radius: number): void {
    const s = node.style;
    const context = this.#context!;
    if (s.borderWidth && s.borderColor) {
      context.strokeStyle = color(s.borderColor);
      context.lineWidth = s.borderWidth;
      context.beginPath();
      context.roundRect(rect.x, rect.y, rect.width, rect.height, radius);
      context.stroke();
      this.#mark(rect, s.borderWidth);
    }
    if (s.borderBottomWidth && s.borderColor) {
      const r = node.screen;
      context.fillStyle = color(s.borderColor);
      context.fillRect(r.x, r.y + r.height - s.borderBottomWidth, r.width, s.borderBottomWidth);
      this.#mark(r, 1);
    }
  }

  #text(node: UINode): void {
    const context = this.#context!;
    const r = node.screen;
    if (node.props.field) {
      this.fields.draw(context, node);
      this.#mark(r, 2);
      return;
    }
    const paragraph = node.text.paragraph;
    if (!paragraph) return;
    const x = r.x + node.inset.left;
    const y = r.y + node.inset.top;
    context.fillStyle = color(colors.selection);
    for (const rect of this.selectionRects.get(node) ?? []) {
      context.fillRect(x + rect.x, y + rect.y, rect.width, rect.height);
    }
    const hover = node === this.hovered ? node.style.hoverColor : undefined;
    paragraph.draw(context, x, y, color(hover ?? node.style.color ?? colors.text), color(colors.link));
    const width = Math.max(r.width, node.inset.left + paragraph.longestLine + node.inset.right);
    const height = Math.max(r.height, node.inset.top + paragraph.height + node.inset.bottom);
    this.#mark({ x: r.x, y: r.y, width, height }, 4);
  }

  #raster(node: UINode): void {
    const resource = node.props.source ? rasterImage(node.props.source) : undefined;
    if (!resource) return;
    const r = node.screen;
    this.#context!.drawImage(resource.image, r.x, r.y, r.width, r.height);
    this.#mark(r, 1);
    node.paintedAt = this.version;
    node.paintedResource = resource.revision;
  }

  #vector(node: UINode): void {
    const path = node.props.path ?? '';
    if (path !== node.pathValue) {
      node.path = path ? new Path2D(path) : null;
      node.pathValue = path;
    }
    if (!node.path) return;
    const context = this.#context!;
    context.save();
    context.translate(node.screen.x, node.screen.y);
    if (node.props.viewBox) {
      context.scale(node.screen.width / node.props.viewBox.width, node.screen.height / node.props.viewBox.height);
    }
    if (node.props.fill) {
      context.fillStyle = color(node.props.fill);
      context.fill(node.path);
    }
    if (node.props.stroke || !node.props.fill) {
      context.lineWidth = 1;
      context.strokeStyle = color(node.props.stroke ?? node.style.color ?? colors.muted);
      context.stroke(node.path);
    }
    context.restore();
    this.#mark(node.screen, 2);
  }

  #focus(node: UINode): void {
    if (!this.focusVisible || this.focused !== node || node.disabled) return;
    const link = this.focusedLink;
    const rect = link ? { ...link, x: node.screen.x + link.x, y: node.screen.y + link.y } : node.screen;
    const context = this.#context!;
    context.strokeStyle = color(colors.accent);
    context.lineWidth = 2;
    context.beginPath();
    context.roundRect(rect.x, rect.y, rect.width, rect.height, link ? 2 : (node.style.borderRadius ?? 2));
    context.stroke();
    this.#mark(rect, 2);
  }

  /** Records painted chrome so only touched pixels are uploaded to the compositor. */
  #mark(rect: Rect, outset: number): void {
    this.#chrome?.mark({
      x: rect.x - outset,
      y: rect.y - outset,
      width: rect.width + 2 * outset,
      height: rect.height + 2 * outset,
    });
  }
}
