import type { AvailableSpace, Size } from 'taffy-layout';
import type { UIStyle } from './types';
import type { TextRun } from './richTypes';
import type { Fonts } from './fonts';
import { Paragraph } from './paragraph';
import { graphemeStops, type Span } from './textBreak';

function spans(fonts: Fonts, value: string, style: UIStyle, runs?: TextRun[]): Span[] {
  if (!runs?.length) return [{ start: 0, end: value.length, face: fonts.face(style) }];
  const result: Span[] = [];
  const boundaries = graphemeStops(value, 0, value.length);
  let offset = 0;
  let start = 0;
  let boundary = 0;
  for (const run of runs) {
    offset += run.text.length;
    while (boundary < boundaries.length - 1 && boundaries[boundary]! < offset) boundary++;
    const end = boundaries[boundary]!;
    if (end > start) result.push({ start, end, face: fonts.face(style, run) });
    start = end;
  }
  return result.length ? result : [{ start: 0, end: value.length, face: fonts.face(style) }];
}

/** A paragraph is rebuilt only when its text/typography changes, never during scrolling or recoloring. */
export class MeasuredText {
  paragraph: Paragraph | null = null;
  #key = '';
  #pre = false;
  readonly #heights = new Map<number, number>();

  update(fonts: Fonts, value: string, s: UIStyle, runs?: TextRun[]): boolean {
    // Colors resolve when drawing; only typography and content affect layout.
    const key = JSON.stringify([
      value,
      s.fontSize,
      s.fontWeight,
      s.lineHeight,
      s.mono,
      s.maxLines,
      s.strike,
      s.textAlign,
      s.whiteSpace,
      runs,
    ]);
    if (key === this.#key) return false;
    this.#key = key;
    this.#pre = s.whiteSpace === 'pre';
    this.#heights.clear();
    this.paragraph = new Paragraph(fonts, value, spans(fonts, value, s, runs), {
      align: s.textAlign ?? 'left',
      pre: this.#pre,
      maxLines: s.maxLines,
    });
    return true;
  }

  measure(known: Size<number | undefined>, available: Size<AvailableSpace>): Size<number> {
    const paragraph = this.paragraph;
    if (!paragraph) return { width: known.width ?? 0, height: known.height ?? 0 };
    const max = Math.ceil(paragraph.maxIntrinsicWidth);
    const space = available.width;
    const constraint =
      space === 'min-content'
        ? known.width !== undefined || this.#pre
          ? max
          : Math.ceil(paragraph.minIntrinsicWidth)
        : space === 'max-content'
          ? max
          : space;
    const width = known.width ?? (this.#pre ? max : Math.min(max, constraint));
    // Taffy already owns this height; final display layout still runs at the actual width.
    if (known.height !== undefined) return { width, height: known.height };
    let height = this.#heights.get(width);
    if (height === undefined) {
      height = paragraph.heightAt(this.#pre ? Math.max(width, max) : width);
      this.#heights.set(width, height);
    }
    return { width, height };
  }

  measureField(known: Size<number | undefined>, available: Size<AvailableSpace>): Size<number> {
    const width = available.width;
    const max = Math.ceil(this.paragraph?.maxIntrinsicWidth ?? 0);
    return {
      width: known.width ?? (width === 'min-content' || width === 'max-content' ? max : width),
      height: known.height ?? this.paragraph?.heightAt(Infinity) ?? 0,
    };
  }

  layout(width: number): void {
    const paragraph = this.paragraph;
    if (!paragraph) return;
    if (this.#pre) width = Math.max(width, Math.ceil(paragraph.maxIntrinsicWidth));
    paragraph.layout(Math.max(0.01, width));
  }

  dispose(): void {
    this.paragraph = null;
    this.#key = '';
    this.#heights.clear();
  }
}
