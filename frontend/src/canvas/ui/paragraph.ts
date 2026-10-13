// Browser-shaped paragraph layout, visual bidi geometry and UTF-16 source ranges.
import type { Face, Fonts } from './fonts';
import {
  fittingEnd,
  hardLines,
  measureRange,
  measureUnits,
  trailingSpace,
  wordSegmenter,
  type HardLine,
  type Span,
} from './textBreak';
import { TextDirection, type PaintRun, type VisualRun } from './textDirection';
import { ParagraphInk } from './textPaint';

export interface TextRect {
  x: number;
  y: number;
  width: number;
  height: number;
}
export interface TextRange {
  start: number;
  end: number;
}
export interface ParagraphOptions {
  align: 'left' | 'center' | 'right';
  pre: boolean;
  maxLines: number | undefined;
}
interface LineRange {
  start: number;
  end: number;
  /** Following source line starts after a hard terminator, or at `end` for wrapping. */
  next: number;
}
interface Ellipsis {
  face: Face;
  x: number;
  width: number;
}
interface Line extends LineRange {
  top: number;
  ascent: number;
  descent: number;
  left: number;
  width: number;
  runs: VisualRun[] | null;
  ink: PaintRun[] | null;
  ellipsis: Ellipsis | null;
}

const ELLIPSIS = '…';
const FIT = 0.01;

export class Paragraph {
  readonly maxIntrinsicWidth: number;
  readonly #hard: HardLine[];
  readonly #direction: TextDirection;
  readonly #ink: ParagraphInk;
  #minIntrinsicWidth: number | undefined;
  #unitsReady = false;
  #lines: Line[] = [];
  #width = -1;
  height = 0;
  longestLine = 0;

  constructor(
    readonly fonts: Fonts,
    readonly text: string,
    readonly spans: readonly Span[],
    readonly options: ParagraphOptions,
  ) {
    this.#hard = hardLines(text);
    this.#direction = new TextDirection(fonts, text, spans);
    this.#ink = new ParagraphInk(this.#direction);
    let max = 0;
    for (const line of this.#hard) max = Math.max(max, this.#measure(line.start, line.end));
    this.maxIntrinsicWidth = max;
  }

  get minIntrinsicWidth(): number {
    if (this.options.pre) return this.maxIntrinsicWidth;
    if (this.#minIntrinsicWidth !== undefined) return this.#minIntrinsicWidth;
    this.#prepareUnits();
    let min = 0;
    for (const line of this.#hard) for (const unit of line.units) min = Math.max(min, unit.content);
    return (this.#minIntrinsicWidth = min);
  }

  /** Height when wrapped at `width`, without preparing drawable lines. */
  heightAt(width: number): number {
    const ranges = this.#break(width).slice(0, this.#maxLines);
    let height = 0;
    for (const range of ranges) {
      const [ascent, descent] = this.#metrics(range);
      height += ascent + descent;
    }
    return height;
  }

  layout(width: number): void {
    width = Math.max(0, width);
    if (width === this.#width) return;
    this.#width = width;
    const ranges = this.#break(width);
    const max = this.#maxLines;
    let top = 0;
    this.longestLine = 0;
    this.#lines = ranges.slice(0, max).map((range, index) => {
      const [ascent, descent] = this.#metrics(range);
      const line: Line = { ...range, top, ascent, descent, left: 0, width: 0, runs: null, ink: null, ellipsis: null };
      top += ascent + descent;
      if (ranges.length > max && index === max - 1) this.#ellipsize(line, width);
      line.width = this.#measure(line.start, line.end) + (line.ellipsis?.width ?? 0);
      this.longestLine = Math.max(this.longestLine, line.width);
      const remaining = Number.isFinite(width) ? width - line.width : 0;
      line.left = this.options.align === 'center' ? remaining / 2 : this.options.align === 'right' ? remaining : 0;
      return line;
    });
    this.height = top;
  }

  draw(context: OffscreenCanvasRenderingContext2D, x: number, y: number, color: string, link: string): void {
    context.direction = 'ltr';
    context.textAlign = 'left';
    context.textBaseline = 'alphabetic';
    context.fontKerning = 'normal';
    for (const line of this.#lines) {
      const baseline = y + line.top + line.ascent;
      const left = x + line.left;
      const runs = this.#runs(line);
      const ink = (line.ink ??= this.#direction.paintRuns(line.start, line.end, runs));
      this.#ink.draw(context, ink, runs, left, baseline, color, link);
      if (!line.ellipsis) continue;
      const mark = line.ellipsis;
      context.save();
      context.beginPath();
      context.rect(left + mark.x, baseline - mark.face.size * 4, mark.width, mark.face.size * 8);
      context.clip();
      context.direction = 'ltr';
      context.font = mark.face.font;
      context.fillStyle = mark.face.link ? link : color;
      context.fillText(ELLIPSIS, left + mark.x, baseline);
      context.restore();
    }
  }

  /** Nearest visual grapheme boundary; RTL source offsets need not increase with x. */
  offsetAt(x: number, y: number): number {
    const line = this.#lines.find((entry) => y < entry.top + entry.ascent + entry.descent) ?? this.#lines.at(-1);
    if (!line) return 0;
    const target = x - line.left;
    if (line.ellipsis && target >= line.ellipsis.x) return line.end;
    let offset = line.start;
    let distance = Infinity;
    for (const run of this.#runs(line)) {
      const positions = this.#direction.positions(run);
      for (let index = 0; index < run.stops!.length; index++) {
        const gap = Math.abs(positions[index]! - target);
        if (gap >= distance) continue;
        distance = gap;
        offset = run.stops![index]!;
      }
    }
    return offset;
  }

  /** A logical selection can cover several disjoint visual rectangles on one bidi line. */
  rectsForRange(start: number, end: number): TextRect[] {
    if (start >= end) return [];
    const rectangles: TextRect[] = [];
    for (const line of this.#lines) {
      const intervals: [number, number][] = [];
      for (const run of this.#runs(line)) {
        const interval = this.#direction.range(run, start, Math.min(end, line.end));
        if (interval && interval[1] > interval[0]) intervals.push(interval);
      }
      if (!line.ellipsis && start <= line.end && end > line.end && line.next > line.end) {
        const edge = this.#runs(line).reduce((right, run) => Math.max(right, run.x + run.width), 0);
        intervals.push([edge, edge + this.fonts.width(this.#faceAt(line.end), ' ')]);
      }
      intervals.sort((left, right) => left[0] - right[0]);
      this.#rectangles(rectangles, intervals, line);
    }
    return rectangles;
  }

  wordBoundary(offset: number): TextRange {
    const segment = wordSegmenter.segment(this.text).containing(Math.max(0, Math.min(offset, this.text.length - 1)));
    return segment ? { start: segment.index, end: segment.index + segment.segment.length } : { start: 0, end: 0 };
  }

  /** Displayed source line, including its hard terminator. */
  lineRange(offset: number): TextRange | null {
    const line = this.#lines.find((entry) => offset < entry.next) ?? this.#lines.at(-1);
    return line ? { start: line.start, end: line.ellipsis ? line.end : line.next } : null;
  }

  get #maxLines(): number {
    const max = this.options.maxLines;
    return max === undefined ? Infinity : Math.max(0, Math.floor(max));
  }

  #measure(start: number, end: number): number {
    return measureRange(this.fonts, this.text, this.spans, start, trailingSpace(this.text, start, end));
  }

  #prepareUnits(): void {
    if (this.#unitsReady) return;
    measureUnits(this.fonts, this.text, this.spans, this.#hard, this.options.pre);
    this.#unitsReady = true;
  }

  #break(width: number): LineRange[] {
    const lines: LineRange[] = [];
    const limit = this.#maxLines + 1;
    const fit = this.options.pre ? Infinity : Math.max(0, width) + FIT;
    for (const hard of this.#hard) {
      // Shape fitting hard lines directly. Break units matter only for min-content or wrapping.
      if (this.#measure(hard.start, hard.end) <= fit) {
        lines.push({ start: hard.start, end: hard.end, next: hard.next });
        if (lines.length >= limit) break;
        continue;
      }
      this.#prepareUnits();
      let start = hard.start;
      for (const unit of hard.units) {
        // Shape the candidate line, not a sum of separately shaped break units:
        // kerning and contextual glyphs survive spaces and rich paint boundaries.
        let measured = this.#measure(start, unit.end);
        if (start < unit.start && measured > fit) {
          lines.push({ start, end: unit.start, next: unit.start });
          if (lines.length >= limit) return lines;
          start = unit.start;
          measured = this.#measure(start, unit.end);
        }
        if (measured <= fit) continue;
        start = this.#split(lines, start, unit.end, width, limit);
        if (lines.length >= limit) return lines;
      }
      lines.push({ start, end: hard.end, next: hard.next });
      if (lines.length >= limit) break;
    }
    return lines;
  }

  #split(lines: LineRange[], start: number, last: number, width: number, limit: number): number {
    for (;;) {
      const end = fittingEnd(this.fonts, this.text, this.spans, [start, last], Math.max(0, width));
      if (end >= last) return start;
      lines.push({ start, end, next: end });
      if (lines.length >= limit) return end;
      start = end;
    }
  }

  #metrics(range: LineRange): [number, number] {
    if (range.start === range.end) {
      const face = this.#faceAt(range.start);
      return [face.ascent, face.descent];
    }
    let ascent = 0;
    let descent = 0;
    for (const span of this.spans) {
      if (span.end <= range.start || span.start === span.end) continue;
      if (span.start >= range.end) break;
      ascent = Math.max(ascent, span.face.ascent);
      descent = Math.max(descent, span.face.descent);
    }
    return [ascent, descent];
  }

  #faceAt(offset: number): Face {
    return (this.spans.find((span) => offset < span.end) ?? this.spans.at(-1)!).face;
  }

  #ellipsize(line: Line, width: number): void {
    const face = this.#faceAt(Math.max(line.start, line.end - 1));
    const mark = this.fonts.width(face, ELLIPSIS);
    let cut = trailingSpace(this.text, line.start, line.end);
    if (this.#measure(line.start, cut) + mark > width + FIT) {
      cut = fittingEnd(this.fonts, this.text, this.spans, [line.start, cut], Math.max(0, width - mark), false);
      cut = trailingSpace(this.text, line.start, cut);
    }
    line.end = cut;
    const x = this.#measure(line.start, cut);
    line.ellipsis = { face, x, width: Math.max(0, Math.min(mark, width - x)) };
  }

  #runs(line: Line): VisualRun[] {
    return (line.runs ??= this.#direction.runs(line.start, line.end));
  }

  #rectangles(rectangles: TextRect[], intervals: [number, number][], line: Line): void {
    let previous: TextRect | null = null;
    for (const [left, right] of intervals) {
      if (previous && line.left + left <= previous.x + previous.width + FIT) {
        previous.width = Math.max(previous.width, line.left + right - previous.x);
      } else {
        previous = { x: line.left + left, y: line.top, width: right - left, height: line.ascent + line.descent };
        rectangles.push(previous);
      }
    }
  }
}
