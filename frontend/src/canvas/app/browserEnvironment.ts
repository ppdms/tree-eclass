import type { Viewport } from '../protocol';
import type { ToApp } from './protocol';

interface VisibleBand {
  top: number;
  bottom: number;
}

export class BrowserEnvironmentHost {
  readonly #dark = matchMedia('(prefers-color-scheme: dark)');
  readonly #motion = matchMedia('(prefers-reduced-motion: reduce)');
  #lastViewport: Viewport;
  #lastBand: VisibleBand;
  #lastActivity = 0;

  constructor(
    readonly canvas: HTMLCanvasElement,
    readonly send: (message: ToApp) => void,
  ) {
    this.#lastViewport = this.viewport;
    this.#lastBand = this.band;
  }
  get viewport(): Viewport {
    return {
      width: Math.max(1, this.canvas.clientWidth),
      height: Math.max(1, this.canvas.clientHeight),
      dpr: window.devicePixelRatio || 1,
    };
  }
  get band(): VisibleBand {
    const viewport = window.visualViewport;
    const height = this.canvas.clientHeight;
    const top = Math.min(height, Math.max(0, viewport?.offsetTop ?? 0));
    return { top, bottom: Math.max(top, Math.min(height, top + (viewport?.height ?? height))) };
  }
  get systemDark(): boolean {
    return this.#dark.matches;
  }
  get reducedMotion(): boolean {
    return this.#motion.matches;
  }
  get visible(): boolean {
    return document.visibilityState !== 'hidden';
  }
  track(): void {
    new ResizeObserver(() => this.#resize()).observe(this.canvas);
    window.addEventListener('resize', () => this.#resize());
    window.visualViewport?.addEventListener('resize', () => this.#resize());
    window.visualViewport?.addEventListener('scroll', () => this.#resize());
    const media = () => this.send({ type: 'media', systemDark: this.systemDark, reducedMotion: this.reducedMotion });
    this.#dark.addEventListener('change', media);
    this.#motion.addEventListener('change', media);
    document.addEventListener('visibilitychange', () => this.#activity('visibility'));
    window.addEventListener('pagehide', () => this.#activity('pagehide'));
    for (const type of ['pointerdown', 'keydown', 'input', 'wheel']) {
      document.addEventListener(type, () => this.#activity('activity'), { capture: true, passive: true });
    }
    document.addEventListener(
      'pointermove',
      (event) => {
        if (event.buttons) this.#activity('activity');
      },
      { capture: true, passive: true },
    );
    window.addEventListener('tree-runtime-changed', () => this.send({ type: 'runtimeChanged' }));
    document.addEventListener('canvas-inspect', () => this.send({ type: 'inspect' }));
    window.addEventListener('canvas-inspect', (event) => {
      if (event.target === window) this.send({ type: 'inspect' });
    });
  }
  #resize(): void {
    const next = this.viewport;
    const previous = this.#lastViewport;
    if (next.width !== previous.width || next.height !== previous.height || next.dpr !== previous.dpr) {
      this.#lastViewport = next;
      this.send({ type: 'resize', viewport: next });
    }
    const band = this.band;
    if (band.top !== this.#lastBand.top || band.bottom !== this.#lastBand.bottom) {
      this.#lastBand = band;
      this.send({ type: 'inset', ...band });
    }
  }
  #activity(kind: 'activity' | 'visibility' | 'pagehide'): void {
    const at = performance.timeOrigin + performance.now();
    if (kind === 'activity' && at - this.#lastActivity < 250) return;
    if (kind === 'activity') this.#lastActivity = at;
    this.send({ type: 'activity', kind, at, visible: kind === 'pagehide' ? false : this.visible });
  }
}
