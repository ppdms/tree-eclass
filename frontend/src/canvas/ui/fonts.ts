// Browser-shaped typography for the render worker. Faces are CSS font strings for
// Canvas2D; the browser shapes, kerns and falls back to system fonts per glyph.
import type { TextRun } from './richTypes';
import type { UIStyle } from './types';

declare global {
  // CSS Font Loading exposes `fonts` on WorkerGlobalScope; the DOM lib only types document.fonts.
  var fonts: FontFaceSet | undefined;
}

const SANS = 'Inter, sans-serif';
const MONO = '"Roboto Mono", monospace';
const WIDTH_CACHE_LIMIT = 200_000;
// Em-relative post/OS2 decoration metrics of the shipped faces: underline below and
// strikeout above the baseline, and their stroke thickness.
const SANS_DECORATION = { underline: 0.17, strike: 0.328, thickness: 0.068 };
const MONO_DECORATION = { underline: 0.073, strike: 0.25, thickness: 0.049 };

/** One resolved text style: its Canvas2D font and half-leading line metrics. */
export interface Face {
  readonly font: string;
  readonly size: number;
  /** Ascent and descent after distributing the line height's leading equally. */
  readonly ascent: number;
  readonly descent: number;
  readonly link: boolean;
  readonly strike: boolean;
  /** Pixel offsets from the baseline and stroke thickness for underline and strikeout. */
  readonly underlineOffset: number;
  readonly strikeOffset: number;
  readonly thickness: number;
}

/** The worker's FontFaceSet, when the engine exposes font loading to workers. */
export function workerFonts(): FontFaceSet | null {
  if (!('FontFaceSet' in globalThis) || !('FontFace' in globalThis)) return null;
  return globalThis.fonts instanceof FontFaceSet ? globalThis.fonts : null;
}

/** Registers the shipped variable faces with the worker and waits until Canvas2D can use them. */
export async function loadFonts(inter: string, mono: string): Promise<Fonts> {
  const set = workerFonts();
  if (!set) throw new Error('This browser cannot load fonts in the render worker');
  const faces = [
    // Inter's shipped subset fixes optical size; native controls and Canvas2D must agree.
    new FontFace('Inter', `url(${JSON.stringify(inter)})`, { weight: '100 900' }),
    new FontFace('Roboto Mono', `url(${JSON.stringify(mono)})`, { weight: '100 700' }),
  ];
  for (const face of faces) set.add(face);
  await Promise.all(faces.map((face) => face.load()));
  return new Fonts();
}

export class Fonts {
  readonly #context: OffscreenCanvasRenderingContext2D;
  readonly #faces = new Map<string, Face>();
  readonly #widths = new Map<string, number>();
  #font = '';

  constructor() {
    const context = new OffscreenCanvas(1, 1).getContext('2d');
    if (!context) throw new Error('Canvas2D is unavailable in the render worker');
    this.#context = context;
    context.direction = 'ltr';
    context.textAlign = 'left';
    context.textBaseline = 'alphabetic';
    context.fontKerning = 'normal';
  }

  face(style: UIStyle, run?: TextRun): Face {
    const size = style.fontSize ?? 14;
    const weight = run?.bold ? 700 : (style.fontWeight ?? 400);
    const italic = run?.italic ? 'italic ' : '';
    const mono = run?.mono ?? style.mono ?? false;
    const font = `${italic}${weight} ${size}px ${mono ? MONO : SANS}`;
    const link = Boolean(run?.href);
    const strike = Boolean(run?.strike || style.strike);
    const lineHeight = style.lineHeight ?? 1.35;
    const key = `${font}|${lineHeight}|${link ? 1 : 0}${strike ? 1 : 0}`;
    let face = this.#faces.get(key);
    if (!face) {
      this.#use(font);
      const metrics = this.#context.measureText('Hg');
      const ascent = Number.isFinite(metrics.fontBoundingBoxAscent)
        ? metrics.fontBoundingBoxAscent
        : metrics.actualBoundingBoxAscent;
      const descent = Number.isFinite(metrics.fontBoundingBoxDescent)
        ? metrics.fontBoundingBoxDescent
        : metrics.actualBoundingBoxDescent;
      const leading = (size * lineHeight - ascent - descent) / 2;
      const decoration = mono ? MONO_DECORATION : SANS_DECORATION;
      face = {
        font,
        size,
        ascent: ascent + leading,
        descent: descent + leading,
        link,
        strike,
        underlineOffset: decoration.underline * size,
        strikeOffset: -decoration.strike * size,
        thickness: decoration.thickness * size,
      };
      this.#faces.set(key, face);
    }
    return face;
  }

  /** Advance width of `text` shaped as one string in `face`. */
  width(face: Face, text: string): number {
    if (!text) return 0;
    const key = `${face.font}\u0000${text}`;
    let width = this.#widths.get(key);
    if (width === undefined) {
      if (this.#widths.size >= WIDTH_CACHE_LIMIT) this.#widths.clear();
      this.#use(face.font);
      width = this.#context.measureText(text).width;
      this.#widths.set(key, width);
    }
    return width;
  }

  #use(font: string): void {
    if (font === this.#font) return;
    this.#context.font = font;
    this.#font = font;
  }
}
