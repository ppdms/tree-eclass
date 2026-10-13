import type { TextItem } from 'pdfjs-dist/types/src/display/api';
import type { TextSpan } from './geometry';
import type { PdfGlyphPosition } from './pdfPositions';
import { glyphBidi } from './glyphBidi.js';
import { normalizeGlyphUnicode } from './glyphUnicode';
import type { GlyphUnicode } from './glyphUnicode';

export interface GlyphExtent {
  start: number;
  end: number;
  from: number;
  to: number;
  /** Resolved character direction within this cluster, including embedded LTR runs. */
  backward?: boolean;
}
export interface GlyphPositionIndex extends Map<string, Map<string, PdfGlyphPosition[]>> {
  order: Map<PdfGlyphPosition, number>;
}
interface Candidate {
  glyph: PdfGlyphPosition;
  from: number;
  to: number;
  origin: number;
  end: number;
  backward: boolean;
}
interface Cluster {
  from: number;
  to: number;
  backward: boolean;
}
const CELL = 64;
const POSITION_EPSILON = 0.001;

export function glyphPositionIndex(positions: PdfGlyphPosition[]): GlyphPositionIndex {
  const index: GlyphPositionIndex = Object.assign(new Map<string, Map<string, PdfGlyphPosition[]>>(), {
    order: new Map<PdfGlyphPosition, number>(),
  });
  for (let at = 0; at < positions.length; at++) {
    const glyph = positions[at]!;
    index.order.set(glyph, at);
    let cells = index.get(glyph.font);
    if (!cells) index.set(glyph.font, (cells = new Map()));
    const left = Math.floor(Math.min(glyph.x, glyph.endX) / CELL);
    const right = Math.floor(Math.max(glyph.x, glyph.endX) / CELL);
    const top = Math.floor(Math.min(glyph.y, glyph.endY) / CELL);
    const bottom = Math.floor(Math.max(glyph.y, glyph.endY) / CELL);
    for (let x = left; x <= right; x++)
      for (let y = top; y <= bottom; y++) {
        const key = `${x}:${y}`;
        let values = cells.get(key);
        if (!values) cells.set(key, (values = []));
        values.push(glyph);
      }
    const originX = Math.floor((glyph.textX ?? glyph.x) / CELL);
    const originY = Math.floor((glyph.textY ?? glyph.y) / CELL);
    if (originX < left || originX > right || originY < top || originY > bottom) {
      const key = `${originX}:${originY}`;
      let values = cells.get(key);
      if (!values) cells.set(key, (values = []));
      values.push(glyph);
    }
  }
  return index;
}

function nearbyGlyphs(cells: Map<string, PdfGlyphPosition[]>, span: TextSpan): Set<PdfGlyphPosition> {
  const found = new Set<PdfGlyphPosition>();
  const box = span.box;
  const margin = span.height * 1.5;
  for (let x = Math.floor((box.x - margin) / CELL); x <= Math.floor((box.x + box.width + margin) / CELL); x++) {
    for (let y = Math.floor((box.y - margin) / CELL); y <= Math.floor((box.y + box.height + margin) / CELL); y++) {
      for (const glyph of cells.get(`${x}:${y}`) ?? []) found.add(glyph);
    }
  }
  return found;
}

function candidates(
  item: TextItem,
  span: TextSpan,
  index: GlyphPositionIndex,
  baseline: { x: number; y: number },
): Candidate[] {
  const cells = index.get(item.fontName);
  if (!cells) return [];
  const result: Candidate[] = [];
  for (const glyph of nearbyGlyphs(cells, span)) {
    if (glyph.used || !(glyph.unicode ?? glyph.text)) continue;
    const dx = (glyph.textX ?? glyph.x) - baseline.x;
    const dy = (glyph.textY ?? glyph.y) - baseline.y;
    const origin = dx * span.cos + dy * span.sin;
    const across = -dx * span.sin + dy * span.cos;
    // PDF.js flushes a run beyond VERTICAL_SHIFT_RATIO (0.25). Compare its
    // actual baseline, NOT its ascent-adjusted top: close lines never compete.
    if (
      Math.abs(across) > span.height * 0.25 + POSITION_EPSILON ||
      origin < -POSITION_EPSILON ||
      origin > span.width + POSITION_EPSILON
    )
      continue;
    const end =
      ((glyph.textEndX ?? glyph.endX) - baseline.x) * span.cos +
      ((glyph.textEndY ?? glyph.endY) - baseline.y) * span.sin;
    // A following run at this item's end is not part of it. Zero-width marks
    // at that boundary still belong to the preceding cluster.
    if (
      span.width > POSITION_EPSILON &&
      origin >= span.width - POSITION_EPSILON &&
      Math.abs(end - origin) > POSITION_EPSILON &&
      !/\p{Mn}/u.test(glyph.unicode ?? glyph.text)
    )
      continue;
    const from = (glyph.x - span.x) * span.cos + (glyph.y - span.y) * span.sin;
    const to = (glyph.endX - span.x) * span.cos + (glyph.endY - span.y) * span.sin;
    result.push({ glyph, origin, end, from: Math.min(from, to), to: Math.max(from, to), backward: to < from });
  }
  // The evaluator appends ToUnicode clusters in operator order, even for RTL.
  // Horizontal sorting before bidi loses repeated characters and cluster identity.
  return result.sort((a, b) => index.order.get(a.glyph)! - index.order.get(b.glyph)!);
}

function mapRun(item: TextItem, span: TextSpan, values: Candidate[]): GlyphExtent[] | null {
  const clusters: Cluster[] = [];
  const raw: string[] = [];
  const owners: number[][] = [];
  let previous: Candidate | undefined;
  for (const value of values) {
    const unicode = value.glyph.unicode ?? value.glyph.text;
    // Same category classification as PDF.js src/core/unicode.js. Invisible
    // format marks and standalone whitespace do not enter its text chunk.
    const category = unicode.match(/^(\s)|(\p{Mn})|(\p{Cf})$/u);
    if (category?.[3] || category?.[1]) continue;
    if (previous) {
      const gap = value.origin - previous.end;
      const spaceScale = previous.glyph.spaceScale ?? span.height;
      // Exact evaluator SPACE_IN_FLOW factors, using original PDF advances.
      // This is a TJ/character-spacing gap, not a guessed character width.
      if (gap >= spaceScale * 0.102 && gap <= spaceScale * 0.6) {
        const id = clusters.length;
        const a = previous.backward ? previous.from : previous.to;
        const b = value.backward ? value.to : value.from;
        clusters.push({ from: Math.min(a, b), to: Math.max(a, b), backward: a > b });
        raw.push(' ');
        owners.push([id]);
      }
    }
    const id = clusters.length;
    clusters.push({ from: value.from, to: value.to, backward: value.backward });
    raw.push(unicode);
    const source = [id];
    for (let at = 0; at < unicode.length; at++) owners.push(source);
    previous = value;
  }
  const normalized = normalizeGlyphUnicode(raw.join(''), owners);
  return runExtents(item, clusters, normalized);
}

function runExtents(item: TextItem, clusters: Cluster[], normalized: GlyphUnicode): GlyphExtent[] | null {
  const reordered = glyphBidi(normalized.text, -1, item.dir === 'ttb');
  if (reordered.str !== item.str) return null;
  const extents: GlyphExtent[] = [];
  let before: number[] | undefined;
  let direction: boolean | undefined;
  for (let at = 0; at < reordered.order.length; at++) {
    const source = normalized.owners[reordered.order[at]!]!;
    const backward = reordered.backward[at]!;
    if (
      before &&
      direction === backward &&
      before.length === source.length &&
      before.every((owner, i) => owner === source[i])
    ) {
      for (let i = extents.length - source.length; i < extents.length; i++) extents[i]!.end = at + 1;
    } else {
      for (const owner of source) {
        const cluster = clusters[owner]!;
        extents.push({
          start: at,
          end: at + 1,
          from: cluster.from,
          to: cluster.to,
          backward: backward !== cluster.backward,
        });
      }
    }
    before = source;
    direction = backward;
  }
  return extents;
}

/** Carries PDF.js's exact normalization/bidi UTF-16 permutation back to source clusters. */
export function positionedGlyphs(
  item: TextItem,
  span: TextSpan,
  index: GlyphPositionIndex,
  baseline: { x: number; y: number },
): GlyphExtent[] {
  // Standalone synthesized spaces have an exact item extent but no PDF glyph.
  if (/^\s+$/u.test(item.str)) return [{ start: 0, end: item.str.length, from: 0, to: span.width, backward: span.rtl }];
  const values = candidates(item, span, index, baseline);
  let start = 0;
  for (let at = 1; at <= values.length; at++) {
    const previous = values[at - 1]!;
    const next = values[at];
    // A backwards text move beyond NEGATIVE_SPACE_FACTOR starts a new source
    // run (including overprinted copies at identical baselines).
    if (next && next.origin - previous.end >= -(previous.glyph.spaceScale ?? span.height) * 0.2) continue;
    const run = values.slice(start, at);
    if (run[0] && Math.abs(run[0].origin) <= POSITION_EPSILON) {
      const mapped = mapRun(item, span, run);
      if (mapped) {
        for (const value of run) value.glyph.used = true;
        return mapped;
      }
    }
    start = at;
  }
  // Never silently drop source text or attach it to unrelated same-valued glyphs.
  throw new Error(
    `PDF text geometry mismatch for ${item.fontName} at baseline ` +
      `${baseline.x},${baseline.y} (${item.str.length} UTF-16 units, ${values.length} source glyphs).`,
  );
}
