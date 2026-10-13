// Paragraph preparation: hard lines, line-break opportunities and measured break units.
// Offsets are UTF-16 indices into the paragraph text, like DOM and native field offsets.
import type { Face, Fonts } from './fonts';

/** A styled text range. Spans cover the whole paragraph without gaps. */
export interface Span {
  start: number;
  end: number;
  face: Face;
}
/** Text between two break opportunities. */
export interface Unit {
  start: number;
  end: number;
  /** Whole-unit advance without hanging trailing whitespace. */
  content: number;
}
export interface HardLine {
  start: number;
  /** End of the line's text, before its line terminator. */
  end: number;
  /** Start of the next hard line, after the terminator. */
  next: number;
  units: Unit[];
}

const graphemes = new Intl.Segmenter(undefined, { granularity: 'grapheme' });
// Dictionary wrapping and word selection use the same immutable segmentation configuration.
export const wordSegmenter = new Intl.Segmenter(undefined, { granularity: 'word' });
const space = /[\t \u1680\u2000-\u2006\u2008-\u200A\u205F\u3000]/u;
const letter = /[\p{L}\p{N}]/u;
const ideograph = /[\u2E80-\u2FFF\u3040-\u30FF\u3400-\u4DBF\u4E00-\u9FFF\uAC00-\uD7AF\uF900-\uFAFF\uFF00-\uFFEF]/u;
const hyphen = /[-\u2010\u2013]/u;
const opening = /[\p{Ps}\p{Pi}]/u;
const closing = /[\p{Pe}\p{Pf},.!?:;\u3001\u3002\uFF0C\uFF0E\uFF01\uFF1F]/u;
const dictionary = /[\p{Script=Thai}\p{Script=Lao}\p{Script=Khmer}\p{Script=Myanmar}]/u;
const glue = /[\u00A0\u2007\u202F\u2060\uFEFF]/u;
const pictograph = /[\p{Extended_Pictographic}\p{Regional_Indicator}]/u;

/** UAX #14 subset: after spaces, hyphens between letters, around em dashes and ideographs. */
function breaksBefore(before: string, after: string, previous: string): boolean {
  if (glue.test(before) || glue.test(after)) return false;
  if (space.test(after)) return false;
  if (opening.test(before) || closing.test(after)) return false;
  if (space.test(before) || before === '\u200B') return true;
  if (before === '\u2014' || after === '\u2014') return true;
  if (hyphen.test(before)) return letter.test(previous) && /\p{L}/u.test(after);
  if (before === '/') return /\p{L}/u.test(after) && letter.test(previous);
  return ideograph.test(before) || ideograph.test(after) || pictograph.test(before) || pictograph.test(after);
}

/** Unicode mandatory line terminators; CRLF remains one source terminator. */
export function hardLines(text: string): HardLine[] {
  const lines: HardLine[] = [];
  let start = 0;
  for (let at = 0; at < text.length; at++) {
    const char = text[at];
    if (
      char !== '\n' &&
      char !== '\r' &&
      char !== '\u000B' &&
      char !== '\u000C' &&
      char !== '\u0085' &&
      char !== '\u2028' &&
      char !== '\u2029'
    )
      continue;
    const next = char === '\r' && text[at + 1] === '\n' ? at + 2 : at + 1;
    lines.push({ start, end: at, next, units: [] });
    start = next;
    at = next - 1;
  }
  lines.push({ start, end: text.length, next: text.length, units: [] });
  return lines;
}

/** Measures `[start, end)` across the spans it covers, one shaped string per span. */
export function measureRange(fonts: Fonts, text: string, spans: readonly Span[], start: number, end: number): number {
  let width = 0;
  let face: Face | null = null;
  let from = start;
  let to = start;
  for (const span of spans) {
    if (span.end <= start) continue;
    if (span.start >= end) break;
    if (face && face.font !== span.face.font) {
      width += fonts.width(face, text.slice(from, to));
      from = to;
    }
    face = span.face;
    to = Math.min(end, span.end);
  }
  return width + (face ? fonts.width(face, text.slice(from, to)) : 0);
}

/** End of `[start, end)` without its trailing whitespace, which hangs past a line's edge. */
export function trailingSpace(text: string, start: number, end: number): number {
  let at = end;
  while (at > start && space.test(text[at - 1]!)) at--;
  return at;
}

/** Fills each hard line with measured units between its break opportunities. */
export function measureUnits(fonts: Fonts, text: string, spans: readonly Span[], lines: HardLine[], pre: boolean) {
  for (const line of lines) {
    const segments = [...graphemes.segment(text.slice(line.start, line.end))];
    const dictionaryStops = new Set<number>();
    if (!pre && dictionary.test(text.slice(line.start, line.end))) {
      for (const segment of wordSegmenter.segment(text.slice(line.start, line.end))) {
        if (segment.isWordLike) dictionaryStops.add(line.start + segment.index + segment.segment.length);
      }
    }
    let start = line.start;
    for (let index = 0; index < segments.length; index++) {
      const segment = segments[index]!;
      const at = line.start + segment.index + segment.segment.length;
      const after = segments[index + 1]?.segment ?? '';
      const boundary = breaksBefore(segment.segment, after, segments[index - 1]?.segment ?? '');
      const dictionaryBreak =
        dictionaryStops.has(at) &&
        (dictionary.test(segment.segment) || dictionary.test(after)) &&
        !glue.test(segment.segment) &&
        !glue.test(after) &&
        !opening.test(segment.segment) &&
        !closing.test(after);
      if (at < line.end && (pre || (!boundary && !dictionaryBreak))) continue;
      const visible = trailingSpace(text, start, at);
      const content = measureRange(fonts, text, spans, start, visible);
      line.units.push({ start, end: at, content });
      start = at;
    }
  }
}

/** Grapheme boundaries in `[start, end)`, including both ends. */
export function graphemeStops(text: string, start: number, end: number): number[] {
  const stops = [start];
  for (const { index, segment } of graphemes.segment(text.slice(start, end))) {
    stops.push(start + index + segment.length);
  }
  if (stops.length === 1) stops.push(end);
  return stops;
}

/** Longest fitting grapheme prefix; wrapping keeps one cluster, ellipsis may keep none. */
export function fittingEnd(
  fonts: Fonts,
  text: string,
  spans: readonly Span[],
  range: [number, number],
  width: number,
  keepOne = true,
): number {
  const [start, end] = range;
  const stops = graphemeStops(text, start, end);
  let low = keepOne && start < end ? 1 : 0;
  let high = stops.length - 1;
  while (low < high) {
    const middle = Math.ceil((low + high) / 2);
    const stop = stops[middle]!;
    if (measureRange(fonts, text, spans, start, trailingSpace(text, start, stop)) > width) high = middle - 1;
    else low = middle;
  }
  // Kerning/contextual shaping can make an adjacent prefix slightly wider; never return
  // an overflowing prefix for ellipsis, and never split a cluster to satisfy the width.
  while (
    low > (keepOne ? 1 : 0) &&
    measureRange(fonts, text, spans, start, trailingSpace(text, start, stops[low]!)) > width
  )
    low--;
  return stops[low] ?? end;
}
