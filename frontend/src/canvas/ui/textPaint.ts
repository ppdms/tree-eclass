// Paint complete single-font logical lines so the browser retains its own bidi,
// fallback and raster context. Visual runs are only geometry for clipping/decorations.
import type { TextDirection, PaintRun, VisualRun } from './textDirection';

export class ParagraphInk {
  constructor(readonly direction: TextDirection) {}

  draw(
    context: OffscreenCanvasRenderingContext2D,
    ink: readonly PaintRun[],
    runs: readonly VisualRun[],
    x: number,
    baseline: number,
    color: string,
    link: string,
  ): void {
    for (const paint of ink) {
      context.font = paint.face.font;
      // A forced RTL fragment still needs an RTL paragraph base: otherwise UAX L1
      // resets its final whitespace to LTR and moves that whitespace to the wrong edge.
      context.direction = paint.rtl ? 'rtl' : 'ltr';
      this.#glyphs(context, paint, runs, x, baseline, color, link);
      this.#decorations(context, paint, runs, x, baseline, color, link);
    }
  }

  #intervals(runs: readonly VisualRun[], paint: PaintRun, start: number, end: number): [number, number][] {
    const intervals: [number, number][] = [];
    const from = Math.max(start, paint.start);
    const to = Math.min(end, paint.end);
    if (from >= to) return intervals;
    for (const run of runs) {
      const interval = this.direction.range(run, from, to);
      if (interval && interval[1] > interval[0]) intervals.push(interval);
    }
    return intervals;
  }

  #glyphs(
    context: OffscreenCanvasRenderingContext2D,
    paint: PaintRun,
    runs: readonly VisualRun[],
    x: number,
    baseline: number,
    color: string,
    link: string,
  ): void {
    const intervals: [number, number][] = [];
    if (color !== link) {
      for (const span of this.direction.spans) {
        if (span.end <= paint.start) continue;
        if (span.start >= paint.end) break;
        if (span.face.link) intervals.push(...this.#intervals(runs, paint, span.start, span.end));
      }
    }
    const top = baseline - paint.face.size * 4;
    const height = paint.face.size * 8;
    if (!intervals.length) {
      context.fillStyle = color;
      context.fillText(paint.text, x + paint.x, baseline);
      return;
    }
    context.save();
    context.beginPath();
    context.rect(x + paint.x - paint.face.size, top, paint.width + paint.face.size * 2, height);
    for (const [left, right] of intervals) context.rect(x + left, top, right - left, height);
    context.clip('evenodd');
    context.fillStyle = color;
    context.fillText(paint.text, x + paint.x, baseline);
    context.restore();
    for (const [left, right] of intervals) {
      context.save();
      context.beginPath();
      context.rect(x + left, top, right - left, height);
      context.clip();
      context.fillStyle = link;
      context.fillText(paint.text, x + paint.x, baseline);
      context.restore();
    }
  }

  #decorations(
    context: OffscreenCanvasRenderingContext2D,
    paint: PaintRun,
    runs: readonly VisualRun[],
    x: number,
    baseline: number,
    color: string,
    link: string,
  ): void {
    for (const span of this.direction.spans) {
      if (span.end <= paint.start) continue;
      if (span.start >= paint.end) break;
      const face = span.face;
      if (!face.link && !face.strike) continue;
      context.fillStyle = face.link ? link : color;
      const half = face.thickness / 2;
      for (const [left, right] of this.#intervals(runs, paint, span.start, span.end)) {
        const width = right - left;
        if (face.link) context.fillRect(x + left, baseline + face.underlineOffset - half, width, face.thickness);
        if (face.strike) context.fillRect(x + left, baseline + face.strikeOffset - half, width, face.thickness);
      }
    }
  }
}
