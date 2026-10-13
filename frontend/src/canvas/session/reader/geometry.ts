import type { TextContent, TextItem } from 'pdfjs-dist/types/src/display/api';
import type { PageSize } from '../../pdf';
import type { AnnotationRect, Bounds, ReaderSelection } from './types';
import { glyphPositionIndex, positionedGlyphs } from './glyphMapping';
import type { GlyphExtent, GlyphPositionIndex } from './glyphMapping';
import type { PdfGlyphPosition } from './pdfPositions';

export interface TextSpan {
  start: number;
  end: number;
  x: number;
  y: number;
  cos: number;
  sin: number;
  height: number;
  width: number;
  edges: number[];
  glyphs: GlyphExtent[];
  rtl: boolean;
  box: Bounds;
}
export interface PageGeometry {
  text: string;
  spans: TextSpan[];
  size: PageSize;
}

function multiply(a: number[], b: number[]): number[] {
  return [
    a[0]! * b[0]! + a[2]! * b[1]!,
    a[1]! * b[0]! + a[3]! * b[1]!,
    a[0]! * b[2]! + a[2]! * b[3]!,
    a[1]! * b[2]! + a[3]! * b[3]!,
    a[0]! * b[4]! + a[2]! * b[5]! + a[4]!,
    a[1]! * b[4]! + a[3]! * b[5]! + a[5]!,
  ];
}

export function spanRect(span: TextSpan, from: number, to: number): Bounds {
  const left = span.rtl ? span.width - to : from;
  const right = span.rtl ? span.width - from : to;
  const alongX = (right - left) * span.cos;
  const alongY = (right - left) * span.sin;
  const acrossX = -span.height * span.sin;
  const acrossY = span.height * span.cos;
  return {
    x: span.x + left * span.cos + Math.min(0, alongX) + Math.min(0, acrossX),
    y: span.y + left * span.sin + Math.min(0, alongY) + Math.min(0, acrossY),
    width: Math.abs(alongX) + Math.abs(acrossX),
    height: Math.abs(alongY) + Math.abs(acrossY),
  };
}

function makeSpan(item: TextItem, content: TextContent, transform: number[], index: GlyphPositionIndex): TextSpan {
  const tx = multiply(transform, item.transform);
  const style = content.styles[item.fontName]!;
  const angle = Math.atan2(tx[1]!, tx[0]!) + (style.vertical ? Math.PI / 2 : 0);
  const height = Math.hypot(tx[2]!, tx[3]!);
  const ascent = height * (style.ascent ?? (style.descent ? 1 + style.descent : 0.8));
  const width = Math.abs(style.vertical ? item.height : item.width) * Math.hypot(transform[0]!, transform[1]!);
  const span: TextSpan = {
    start: 0,
    end: 0,
    x: tx[4]! + ascent * Math.sin(angle),
    y: tx[5]! - ascent * Math.cos(angle),
    cos: Math.cos(angle),
    sin: Math.sin(angle),
    height,
    width,
    edges: Array<number>(item.str.length + 1).fill(0),
    glyphs: [],
    rtl: item.dir === 'rtl',
    box: { x: 0, y: 0, width: 0, height: 0 },
  };
  span.box = spanRect(span, 0, width);
  span.glyphs = positionedGlyphs(item, span, index, { x: tx[4]!, y: tx[5]! });
  assignCaretEdges(span);
  return span;
}

function assignCaretEdges(span: TextSpan): void {
  for (let at = 0; at < span.glyphs.length; at++) {
    const glyph = span.glyphs[at]!;
    const backward = glyph.backward ?? span.rtl;
    let left = glyph.from;
    let right = glyph.to;
    // Whole-chunk normalization can give one logical interval several source
    // owners. Its carets bound their union, not whichever owner is visited last.
    let next = span.glyphs[at + 1];
    while (next && next.start === glyph.start && next.end === glyph.end && (next.backward ?? span.rtl) === backward) {
      left = Math.min(left, next.from);
      right = Math.max(right, next.to);
      at++;
      next = span.glyphs[at + 1];
    }
    const physicalFrom = backward ? right : left;
    const physicalTo = backward ? left : right;
    const from = span.rtl ? span.width - physicalFrom : physicalFrom;
    const to = span.rtl ? span.width - physicalTo : physicalTo;
    for (let unit = glyph.start; unit < glyph.end; unit++) span.edges[unit] = from;
    span.edges[glyph.end] = to;
  }
}

function needsGap(previous: TextSpan | undefined, current: TextSpan): boolean {
  if (!previous) return false;
  const a = previous.box;
  const b = current.box;
  return (
    Math.abs(a.y - b.y) > Math.max(a.height, b.height) * 0.45 || b.x - (a.x + a.width) > Math.max(1, b.height * 0.12)
  );
}

/** Mirrors production pageTextIndex geometric spaces and JS/UTF-16 offsets. */
export function pageGeometry(
  content: TextContent,
  transform: number[],
  size: PageSize,
  positions: PdfGlyphPosition[],
): PageGeometry {
  const spans: TextSpan[] = [];
  const index = glyphPositionIndex(positions);
  let text = '';
  for (const value of content.items) {
    if (!('str' in value) || !value.str) continue;
    const span = makeSpan(value, content, transform, index);
    if (/\S$/.test(text) && /^\S/.test(value.str) && needsGap(spans.at(-1), span)) text += ' ';
    span.start = text.length;
    text += value.str;
    span.end = text.length;
    spans.push(span);
  }
  return { text, spans, size };
}

function nearestSpan(geometry: PageGeometry, x: number, y: number): TextSpan | null {
  let best: TextSpan | null = null;
  let distance = Infinity;
  for (const span of geometry.spans) {
    const dx = x - span.x;
    const dy = y - span.y;
    const along = dx * span.cos + dy * span.sin;
    const across = -dx * span.sin + dy * span.cos;
    const outsideX = Math.max(0, -along, along - span.width);
    const outsideY = Math.max(0, -across, across - span.height);
    const next = outsideX * outsideX + outsideY * outsideY * 4;
    if (next < distance) {
      best = span;
      distance = next;
    }
  }
  return best;
}

export function hitText(geometry: PageGeometry, x: number, y: number): number | null {
  const span = nearestSpan(geometry, x, y);
  if (!span) return null;
  let along = (x - span.x) * span.cos + (y - span.y) * span.sin;
  if (span.rtl) along = span.width - along;
  const end = span.edges.length - 1;
  let best = 0;
  let distance = Math.abs(span.edges[0]! - along);
  const finalDistance = Math.abs(span.edges[end]! - along);
  if (finalDistance < distance) {
    best = end;
    distance = finalDistance;
  }
  // One PDF glyph can own non-contiguous logical units. At coincident carets, preserve the
  // run boundary rather than truncating a full-run drag at an interior unit of that glyph.
  for (let i = 1; i < end; i++) {
    const next = Math.abs(span.edges[i]! - along);
    if (next < distance) {
      best = i;
      distance = next;
    }
  }
  const offset = span.start + best;
  // Carets never split a UTF-16 surrogate pair.
  return /[\uDC00-\uDFFF]/.test(geometry.text[offset] ?? '') ? offset - 1 : offset;
}

export function caretPoint(geometry: PageGeometry, offset: number): { x: number; y: number } | null {
  const span = geometry.spans.find((item) => offset >= item.start && offset <= item.end);
  if (!span) return null;
  const edge = span.edges[offset - span.start] ?? span.width;
  const along = span.rtl ? span.width - edge : edge;
  return {
    x: span.x + along * span.cos - span.height * span.sin,
    y: span.y + along * span.sin + span.height * span.cos,
  };
}

const wordSegmenter = new Intl.Segmenter(undefined, { granularity: 'word' });

export function wordRange(text: string, at: number): [number, number] {
  const point = Math.max(0, Math.min(text.length - 1, at));
  const word = wordSegmenter.segment(text).containing(point);
  return word ? [word.index, word.index + word.segment.length] : [point, point];
}

export function lineRange(geometry: PageGeometry, at: number): [number, number] {
  const chosen = geometry.spans.find((span) => at >= span.start && at <= span.end);
  if (!chosen) return [at, at];
  let start = chosen.start;
  let end = chosen.end;
  for (const span of geometry.spans) {
    const across = -(span.x - chosen.x) * chosen.sin + (span.y - chosen.y) * chosen.cos;
    const angle = span.cos * chosen.cos + span.sin * chosen.sin;
    if (angle > 0.99 && Math.abs(across) < Math.max(span.height, chosen.height) * 0.45) {
      start = Math.min(start, span.start);
      end = Math.max(end, span.end);
    }
  }
  return [start, end];
}

export function rangeRects(geometry: PageGeometry, start: number, end: number): AnnotationRect[] {
  const rects: AnnotationRect[] = [];
  for (const span of geometry.spans) {
    if (span.end <= start || span.start >= end) continue;
    for (const glyph of span.glyphs) {
      if (span.start + glyph.end <= start || span.start + glyph.start >= end) continue;
      const from = span.rtl ? span.width - glyph.to : glyph.from;
      const to = span.rtl ? span.width - glyph.from : glyph.to;
      const box = spanRect(span, from, to);
      if (box.width <= 0.5 || box.height <= 0.5) continue;
      rects.push({
        x: box.x / geometry.size.width,
        y: box.y / geometry.size.height,
        w: box.width / geometry.size.width,
        h: box.height / geometry.size.height,
      });
    }
  }
  return mergeRects(rects);
}

export function mergeRects(rects: AnnotationRect[]): AnnotationRect[] {
  const merged: AnnotationRect[] = [];
  for (const rect of rects.sort((a, b) => a.y - b.y || a.x - b.x)) {
    const last = merged.at(-1);
    if (last && Math.abs(last.y - rect.y) < Math.max(last.h, rect.h) * 0.35 && rect.x - (last.x + last.w) < 0.012) {
      const right = Math.max(last.x + last.w, rect.x + rect.w);
      last.x = Math.min(last.x, rect.x);
      last.w = right - last.x;
      last.y = Math.min(last.y, rect.y);
      last.h = Math.max(last.h, rect.h);
    } else merged.push({ ...rect });
  }
  return merged;
}

const normalize = (text: string) => text.replace(/\s+/g, ' ').trim();

export function selectionPayload(
  documentId: string,
  page: number,
  geometry: PageGeometry,
  a: number,
  b: number,
  box: Bounds,
): ReaderSelection | null {
  const start = Math.max(0, Math.min(a, b));
  const end = Math.min(geometry.text.length, Math.max(a, b));
  const quote = normalize(geometry.text.slice(start, end));
  const rects = rangeRects(geometry, start, end);
  if (!quote || !rects.length) return null;
  let x = Infinity;
  let y = Infinity;
  let right = -Infinity;
  let bottom = -Infinity;
  for (const rect of rects) {
    x = Math.min(x, box.x + rect.x * box.width);
    y = Math.min(y, box.y + rect.y * box.height);
    right = Math.max(right, box.x + (rect.x + rect.w) * box.width);
    bottom = Math.max(bottom, box.y + (rect.y + rect.h) * box.height);
  }
  return {
    documentId,
    page,
    quote,
    prefix: normalize(geometry.text.slice(Math.max(0, start - 120), start)),
    suffix: normalize(geometry.text.slice(end, end + 120)),
    char_start: start,
    char_end: end,
    rects,
    bounds: { x, y, width: right - x, height: bottom - y },
  };
}
