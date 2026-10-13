// UBA owns visual ordering only. Canvas2D shapes complete same-font directional
// runs; UTF-16 source text is never reordered or normalized for selection/copying.
import bidiFactory, { type Bidi, type BidiLevels, type BidiType } from 'bidi-js';
import type { Face, Fonts } from './fonts';
import { graphemeStops, type Span } from './textBreak';

export interface PaintRun {
  start: number;
  end: number;
  face: Face;
  rtl: boolean;
  x: number;
  width: number;
  text: string;
}
export interface VisualRun extends PaintRun {
  stops: number[] | null;
  positions: number[] | null;
}
interface LogicalRun extends VisualRun {
  level: number;
  rank: number;
}

// Skip Unicode table initialization for the ordinary Greek/Latin UI. All RTL
// ranges in the pinned Unicode 13 data, and directional controls, take the UBA path.
const directional = new RegExp(
  '[\\u0590-\\u08FF\\u200E\\u200F\\u202A-\\u202E\\u2066-\\u2069\\uFB1D-\\uFDFF\\uFE70-\\uFEFF' +
    '\\u{10800}-\\u{10FFF}\\u{1E800}-\\u{1EDFF}]',
  'u',
);
const controls = /[\u061C\u200E\u200F\u202A-\u202E\u2066-\u2069]/gu;
const trailing = {
  B: true,
  S: true,
  WS: true,
  LRI: true,
  RLI: true,
  FSI: true,
  PDI: true,
  BN: true,
  RLE: true,
  LRE: true,
  RLO: true,
  LRO: true,
  PDF: true,
} satisfies Partial<Record<BidiType, true>>;
const representative = {
  L: 'A',
  R: '\u05D0',
  AL: '\u0627',
  EN: '1',
  AN: '\u0661',
  NSM: '\u0300',
  ON: '!',
  BN: '\u200B',
  B: '\n',
  S: '\t',
  WS: ' ',
  ES: '+',
  ET: '$',
  CS: ',',
  LRI: '\u2066',
  RLI: '\u2067',
  FSI: '\u2068',
  PDI: '\u2069',
  LRE: '\u202A',
  RLE: '\u202B',
  LRO: '\u202D',
  RLO: '\u202E',
  PDF: '\u202C',
} satisfies Record<BidiType, string>;
let bidi: Bidi | null = null;

function embedding(text: string): BidiLevels {
  const engine = (bidi ??= bidiFactory());
  const points = [...text];
  // The pinned library indexes UTF-16 code units. Feed one code unit per scalar,
  // with its exact library-provided bidi property, then expand levels to source offsets.
  const classified = points.map((point) =>
    point.length === 1 ? point : representative[engine.getBidiCharTypeName(point)],
  );
  const result = engine.getEmbeddingLevels(classified.join(''), 'ltr');
  const levels = new Uint8Array(text.length);
  let offset = 0;
  for (let index = 0; index < points.length; index++) {
    const length = points[index]!.length;
    levels.fill(result.levels[index]!, offset, offset + length);
    offset += length;
  }
  return { levels, paragraphs: [{ start: 0, end: text.length - 1, level: 0 }] };
}

/** Formatting controls already resolved by the UBA have no visible glyph. */
function drawingText(text: string, rtl: boolean, resolved: boolean): string {
  if (!resolved) return text;
  return `${rtl ? '\u202E' : '\u202D'}${text.replace(controls, '')}\u202C`;
}

export class TextDirection {
  readonly #levels: BidiLevels | null;

  constructor(
    readonly fonts: Fonts,
    readonly text: string,
    readonly spans: readonly Span[],
  ) {
    this.#levels = directional.test(text) ? embedding(text) : null;
  }

  /** Complete shaped runs in visual left-to-right order, with global source offsets. */
  runs(start: number, end: number): VisualRun[] {
    if (start === end) return [];
    if (!this.#levels) return this.#plainRuns(start, end);
    const stops = graphemeStops(this.text, start, end);
    const [levels, ranks] = this.#order(start, end);
    const runs: LogicalRun[] = [];
    let spanIndex = this.spans.findIndex((span) => span.end > start);
    for (let index = 0; index < stops.length - 1; index++) {
      const from = stops[index]!;
      const to = stops[index + 1]!;
      while (this.spans[spanIndex]!.end <= from) spanIndex++;
      const face = this.spans[spanIndex]!.face;
      const level = levels?.[from - start] ?? 0;
      const rank = ranks?.[from - start] ?? from - start;
      const previous = runs.at(-1);
      if (previous && previous.level === level && previous.face.font === face.font) {
        previous.end = to;
        previous.rank = Math.min(previous.rank, rank);
        previous.stops!.push(to);
      } else {
        runs.push({
          start: from,
          end: to,
          face,
          level,
          rank,
          rtl: Boolean(level & 1),
          x: 0,
          width: 0,
          text: '',
          stops: [from, to],
          positions: null,
        });
      }
    }
    runs.sort((left, right) => left.rank - right.rank);
    let x = 0;
    for (const run of runs) {
      run.text = drawingText(this.text.slice(run.start, run.end), run.rtl, this.#levels !== null);
      run.width = this.fonts.width(run.face, run.text);
      run.x = x;
      x += run.width;
    }
    return runs;
  }

  /** Caret coordinates are bounded by the complete shaped run, including kerning. */
  positions(run: VisualRun): number[] {
    if (run.positions) return run.positions;
    const stops = (run.stops ??= graphemeStops(this.text, run.start, run.end));
    const resolved = this.#levels !== null;
    let previous = 0;
    const positions = stops.map((stop, index) => {
      let advance = 0;
      if (index === stops.length - 1) advance = run.width;
      else if (index > 0) {
        const suffix = drawingText(this.text.slice(stop, run.end), run.rtl, resolved);
        advance = Math.max(0, Math.min(run.width, run.width - this.fonts.width(run.face, suffix)));
      }
      advance = Math.max(previous, advance);
      previous = advance;
      return run.x + (run.rtl ? run.width - advance : advance);
    });
    run.positions = positions;
    return positions;
  }

  /** Snap range endpoints outwards to complete grapheme clusters. */
  range(run: VisualRun, start: number, end: number): [number, number] | null {
    if (end <= run.start || start >= run.end) return null;
    const positions = this.positions(run);
    const from = run.stops!.findLastIndex((stop) => stop <= Math.max(start, run.start));
    const to = run.stops!.findIndex((stop) => stop >= Math.min(end, run.end));
    const first = positions[Math.max(0, from)]!;
    const last = positions[to < 0 ? positions.length - 1 : to]!;
    return [Math.min(first, last), Math.max(first, last)];
  }

  /** Preserve the browser's complete bidi/fallback context whenever one font covers the line. */
  paintRuns(start: number, end: number, runs: VisualRun[]): PaintRun[] {
    if (!runs.length || !this.#levels) return runs;
    const face = runs[0]!.face;
    if (runs.some((run) => run.face.font !== face.font)) return runs;
    const text = this.text.slice(start, end);
    return [{ start, end, face, rtl: false, x: 0, width: this.fonts.width(face, text), text }];
  }

  #plainRuns(start: number, end: number): VisualRun[] {
    const runs: VisualRun[] = [];
    for (const span of this.spans) {
      if (span.end <= start) continue;
      if (span.start >= end) break;
      const from = Math.max(start, span.start);
      const to = Math.min(end, span.end);
      const previous = runs.at(-1);
      if (previous && previous.face.font === span.face.font) previous.end = to;
      else
        runs.push({
          start: from,
          end: to,
          face: span.face,
          rtl: false,
          x: 0,
          width: 0,
          text: '',
          stops: null,
          positions: null,
        });
    }
    let x = 0;
    for (const run of runs) {
      run.text = this.text.slice(run.start, run.end);
      run.width = this.fonts.width(run.face, run.text);
      run.x = x;
      x += run.width;
    }
    return runs;
  }

  #order(start: number, end: number): [Uint8Array | null, number[] | null] {
    if (!this.#levels || !bidi) return [null, null];
    const text = this.text.slice(start, end);
    const levels = this.#levels.levels.slice(start, end);
    // L1 trailing reset is line-local; call the library with zero-based line indices.
    // This also avoids its substring reordering helper's absolute-index mutation.
    const points = [...text];
    let at = text.length;
    for (let index = points.length - 1; index >= 0; index--) {
      const point = points[index]!;
      at -= point.length;
      if (!Object.hasOwn(trailing, bidi.getBidiCharTypeName(point))) break;
      levels.fill(0, at, at + point.length);
    }
    const embedding: BidiLevels = { levels, paragraphs: [{ start: 0, end: text.length - 1, level: 0 }] };
    const order = Array.from({ length: text.length }, (_, index) => index);
    for (const [from, to] of bidi.getReorderSegments(text, embedding)) {
      for (let left = from, right = to; left < right; left++, right--) {
        const value = order[left]!;
        order[left] = order[right]!;
        order[right] = value;
      }
    }
    const ranks = Array.from({ length: text.length }, () => 0);
    for (let index = 0; index < order.length; index++) ranks[order[index]!] = index;
    return [levels, ranks];
  }
}
