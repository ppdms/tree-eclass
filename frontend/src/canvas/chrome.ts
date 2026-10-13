// Records the navigation layer as Canvas2D segments interleaved with glass, fill and
// scroll-edge operations. A segment is captured to a texture the moment an operation
// must sit above it, so one chrome canvas serves every segment of the frame.
import { deviceRect, type DeviceRect } from './gl';
import type { Palette } from './theme';
import type { Rect } from './ui/types';

/** Shape-dependent optics; appearance-dependent tint and light come from the palette. */
export interface GlassStyle {
  bevel: number;
  depth: number;
  frost: number;
  density: number;
}
/** Pointer position and its animated energy (0 = untouched, 1 = pressed). */
export interface GlassGlow {
  x: number;
  y: number;
  energy: number;
}
export type ChromeOp =
  | { kind: 'edge'; top: number; bottom: number }
  | { kind: 'glass'; rect: Rect; radius: number; style: GlassStyle; glow: GlassGlow | null }
  | { kind: 'fill'; rect: Rect; color: string }
  | { kind: 'layer'; slot: number; bounds: DeviceRect; alpha: number };

export const barGlass: GlassStyle = { bevel: 20, depth: 22, frost: 1, density: 0 };
export const overlayGlass: GlassStyle = { bevel: 16, depth: 16, frost: 1, density: 0.25 };

/** Uploads `bounds` of the chrome canvas to a texture slot that lives until the frame is composited. */
export type CaptureSegment = (canvas: OffscreenCanvas, bounds: DeviceRect) => number;

export class ChromeRecorder {
  readonly ops: ChromeOp[] = [];
  readonly context: OffscreenCanvasRenderingContext2D;
  palette: Palette | null = null;
  readonly #canvas: OffscreenCanvas;
  #bounds: Rect | null = null;
  #alpha = 1;
  #dpr = 1;

  constructor(readonly capture: CaptureSegment) {
    this.#canvas = new OffscreenCanvas(1, 1);
    const context = this.#canvas.getContext('2d');
    if (!context) throw new Error('Canvas2D is unavailable in the render worker');
    this.context = context;
  }

  begin(width: number, height: number, dpr: number): void {
    this.ops.length = 0;
    this.#bounds = null;
    this.#alpha = 1;
    this.#dpr = dpr;
    if (this.#canvas.width !== width || this.#canvas.height !== height) {
      this.#canvas.width = width;
      this.#canvas.height = height;
    }
    this.context.setTransform(dpr, 0, 0, dpr, 0, 0);
  }

  /** Extends the current segment by a drawn CSS-pixel area. */
  mark(rect: Rect): void {
    const bounds = this.#bounds;
    if (!bounds) {
      this.#bounds = { ...rect };
      return;
    }
    const right = Math.max(bounds.x + bounds.width, rect.x + rect.width);
    const bottom = Math.max(bounds.y + bounds.height, rect.y + rect.height);
    bounds.x = Math.min(bounds.x, rect.x);
    bounds.y = Math.min(bounds.y, rect.y);
    bounds.width = right - bounds.x;
    bounds.height = bottom - bounds.y;
  }

  /**
   * Places `op` above everything drawn so far; later drawing has opacity `alpha`.
   * Returns true when the canvas was cleared and its `depth` saved clip levels unwound,
   * so the caller must re-apply its clip stack.
   */
  queue(op: ChromeOp | null, depth: number, alpha = 1): boolean {
    const flushed = this.#flush(depth);
    if (op) this.ops.push(op);
    this.#alpha = alpha;
    return flushed;
  }

  end(): void {
    this.#flush(0);
  }

  #flush(depth: number): boolean {
    if (!this.#bounds) return false;
    const bounds = deviceRect(this.#bounds, 2, this.#dpr, this.#canvas);
    this.#bounds = null;
    if (bounds.width > 0 && bounds.height > 0) {
      this.ops.push({ kind: 'layer', slot: this.capture(this.#canvas, bounds), bounds, alpha: this.#alpha });
    }
    const context = this.context;
    for (let level = 0; level < depth; level++) context.restore();
    context.setTransform(1, 0, 0, 1, 0, 0);
    context.clearRect(bounds.x, bounds.y, bounds.width, bounds.height);
    context.setTransform(this.#dpr, 0, 0, this.#dpr, 0, 0);
    return true;
  }
}
