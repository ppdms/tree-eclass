// One presentation surface: Canvas2D content, then the completed offscreen WebGL2
// navigation layer. Separate visible canvases can present different frames even
// when the worker updates them in one task.
import { ChromeRecorder } from './chrome';
import { GlassCompositor } from './glass';
import { surfaceHeight } from './gl';
import type { Viewport } from './protocol';

/** Wall milliseconds for each part of a frame; GL calls can include driver waits. */
export interface FramePhases {
  content: number;
  chrome: number;
  composite: number;
}

export class Renderer {
  readonly chrome: ChromeRecorder;
  readonly #base: OffscreenCanvas;
  readonly #context: OffscreenCanvasRenderingContext2D;
  readonly #compositor: GlassCompositor;
  #viewport: Viewport;

  constructor(base: OffscreenCanvas, viewport: Viewport) {
    const width = Math.round(viewport.width * viewport.dpr);
    const height = Math.round(viewport.height * viewport.dpr);
    base.width = width;
    base.height = height;
    this.#base = base;
    const context = base.getContext('2d', { alpha: false });
    if (!context) throw new Error('Canvas2D is unavailable in the render worker');
    this.#context = context;
    this.#compositor = new GlassCompositor(new OffscreenCanvas(width, surfaceHeight(height, viewport.dpr)));
    this.chrome = new ChromeRecorder((canvas, bounds) => this.#compositor.capture(canvas, bounds));
    this.#viewport = viewport;
    this.#compositor.resize(width, height, viewport.dpr);
  }

  get label(): string {
    return `Canvas2D + ${this.#compositor.label}`;
  }

  get viewport(): Viewport {
    return this.#viewport;
  }

  resize(viewport: Viewport): void {
    this.#viewport = viewport;
    const width = Math.round(viewport.width * viewport.dpr);
    const height = Math.round(viewport.height * viewport.dpr);
    if (this.#base.width !== width) this.#base.width = width;
    if (this.#base.height !== height) this.#base.height = height;
    this.#compositor.resize(width, height, viewport.dpr);
  }

  frame(
    drawContent: (context: OffscreenCanvasRenderingContext2D, dpr: number) => void,
    drawChrome: (chrome: ChromeRecorder) => void,
  ): FramePhases {
    const dpr = this.#viewport.dpr;
    const t0 = performance.now();
    this.#context.setTransform(dpr, 0, 0, dpr, 0, 0);
    drawContent(this.#context, dpr);
    const t1 = performance.now();
    this.#compositor.beginFrame();
    this.chrome.begin(this.#base.width, this.#base.height, dpr);
    drawChrome(this.chrome);
    this.chrome.end();
    const t2 = performance.now();
    this.#compositor.run(this.chrome.ops, this.#base, this.chrome.palette);
    if (this.chrome.ops.length) {
      this.#context.setTransform(1, 0, 0, 1, 0, 0);
      this.#context.drawImage(this.#compositor.canvas, 0, 0);
    }
    return { content: t1 - t0, chrome: t2 - t1, composite: performance.now() - t2 };
  }
}
