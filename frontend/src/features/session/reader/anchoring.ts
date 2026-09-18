/**
 * Turning a browser text selection into a durable anchor, and back again.
 *
 * A highlight stored only as rectangles is a mystery: it cannot be re-found if
 * the document is re-rendered at another zoom, cannot survive the lecturer
 * replacing the file, and cannot be read by anything that is not a renderer.
 * So the anchor of record is the quote plus the text around it — the W3C
 * TextQuoteSelector idea — and the rectangles are only a cache for painting.
 *
 * When the quote can no longer be found, the honest outcome is to say so. This
 * module never guesses at a nearby match, because a highlight silently moved
 * onto different text is worse than a highlight visibly marked as orphaned.
 */

export interface Anchor {
  quote: string;
  prefix?: string;
  suffix?: string;
  char_start?: number;
  char_end?: number;
  rects?: { x: number; y: number; w: number; h: number }[];
}

export type AnchorConfidence = 'exact' | 'context' | 'quote';

export interface ResolvedAnchor {
  start: number;
  end: number;
  confidence: AnchorConfidence;
}

interface PageNodeEntry {
  node: Node;
  start: number;
  end: number;
}

export interface PageTextIndex {
  text: string;
  nodes: PageNodeEntry[];
}

/** How much surrounding text is kept to disambiguate a repeated quote. */
const CONTEXT_CHARS = 120;

/** Collapse whitespace while preserving contiguous PDF glyphs. */
export const normalize = (text: string): string =>
  String(text || '')
    .replace(/\s+/g, ' ')
    .trim();

function needsTextGap(previous: Node | null, current: Node): boolean {
  if (!previous || !current) return false;
  // SAFETY: text nodes in a PDF.js layer are always children of spans; the
  // cast narrows the generic Node to read its layout box.
  const previousRect = (previous as HTMLElement).parentElement?.getBoundingClientRect?.();
  // SAFETY: text nodes in a PDF.js layer are always children of spans; the
  // cast narrows the generic Node to read its layout box.
  const currentRect = (current as HTMLElement).parentElement?.getBoundingClientRect?.();
  if (!previousRect || !currentRect) return false;
  if (Math.abs(previousRect.top - currentRect.top) > Math.max(previousRect.height, currentRect.height) * 0.45)
    return true;
  return currentRect.left - previousRect.right > Math.max(1, currentRect.height * 0.12);
}

function mergeRects(
  rects: { x: number; y: number; w: number; h: number }[],
): { x: number; y: number; w: number; h: number }[] {
  const sorted = [...rects].sort((a, b) => a.y - b.y || a.x - b.x);
  type Rect = { x: number; y: number; w: number; h: number };
  const merged: Rect[] = [];
  for (const rect of sorted) {
    const previous = merged.at(-1);
    const sameLine = previous && Math.abs(previous.y - rect.y) < Math.max(previous.h, rect.h) * 0.35;
    if (sameLine && rect.x - (previous.x + previous.w) < 0.012) {
      const right = Math.max(previous.x + previous.w, rect.x + rect.w);
      previous.x = Math.min(previous.x, rect.x);
      previous.w = right - previous.x;
      previous.y = Math.min(previous.y, rect.y);
      previous.h = Math.max(previous.h, rect.h);
    } else merged.push({ ...rect });
  }
  return merged;
}

/**
 * Read the flattened text of a rendered page layer, with a map back to nodes.
 *
 * PDF.js paints a page's text as many absolutely positioned spans. Offsets are
 * computed over their concatenation so a selection spanning several spans still
 * produces one contiguous quote.
 */
export function pageTextIndex(layer: HTMLElement): PageTextIndex {
  const walker = document.createTreeWalker(layer, NodeFilter.SHOW_TEXT);
  const nodes: PageNodeEntry[] = [];
  let text = '';
  let previous: Node | null = null;
  let node = walker.nextNode();
  while (node) {
    const value = node.textContent || '';
    if (text && /\S$/.test(text) && /^\S/.test(value) && needsTextGap(previous, node)) text += ' ';
    nodes.push({ node, start: text.length, end: text.length + value.length });
    text += value;
    previous = node;
    node = walker.nextNode();
  }
  return { text, nodes };
}

/** Convert a DOM selection inside a page layer into a storable anchor. */
export function selectionToAnchor(selection: Selection | null, layer: HTMLElement | null): Anchor | null {
  if (!selection || selection.isCollapsed || !layer) return null;
  const range = selection.getRangeAt(0);
  if (!layer.contains(range.commonAncestorContainer)) return null;

  const { text, nodes } = pageTextIndex(layer);
  const offsetOf = (container: Node, offset: number): number | null => {
    const entry = nodes.find((item) => item.node === container);
    return entry ? entry.start + offset : null;
  };
  const start = offsetOf(range.startContainer, range.startOffset);
  const end = offsetOf(range.endContainer, range.endOffset);
  if (start === null || end === null || end <= start) return null;

  const quote = normalize(text.slice(start, end));
  if (!quote) return null;

  // Rectangles are stored relative to the page box, so a highlight repaints
  // correctly at any zoom rather than only at the zoom it was made at.
  const box = layer.getBoundingClientRect();
  const rects = mergeRects(
    Array.from(range.getClientRects())
      .filter((rect) => rect.width > 0.5 && rect.height > 0.5)
      .map((rect) => ({
        x: (rect.left - box.left) / box.width,
        y: (rect.top - box.top) / box.height,
        w: rect.width / box.width,
        h: rect.height / box.height,
      })),
  );

  return {
    quote,
    prefix: normalize(text.slice(Math.max(0, start - CONTEXT_CHARS), start)),
    suffix: normalize(text.slice(end, end + CONTEXT_CHARS)),
    char_start: start,
    char_end: end,
    rects,
  };
}

/**
 * Locate a stored anchor in a freshly rendered page.
 *
 * The remembered offset is tried first and verified, then the quote with its
 * context, then the quote alone but only when it appears exactly once. A quote
 * that appears several times with no matching context is reported unresolved
 * rather than pinned to whichever copy happened to come first.
 */
export function resolveAnchor(
  anchor: Anchor | { quote?: string; prefix?: string; suffix?: string; char_start?: number; char_end?: number },
  layer: HTMLElement | null,
): ResolvedAnchor | null {
  if (!anchor || !layer) return null;
  const quote = normalize(anchor.quote || '');
  if (!quote) return null;

  const { text } = pageTextIndex(layer);
  const haystack = normalize(text);
  // SAFETY: Number.isInteger above established that char_start/char_end are
  // finite numbers; the cast narrows the optional field for arithmetic.
  const remembered = Number.isInteger(anchor.char_start) ? (anchor.char_start as number) : -1;
  // SAFETY: Number.isInteger above established that char_start/char_end are
  // finite numbers; the cast narrows the optional field for arithmetic.
  const rememberedEnd = Number.isInteger(anchor.char_end) ? (anchor.char_end as number) : -1;

  if (remembered >= 0 && rememberedEnd > remembered && normalize(text.slice(remembered, rememberedEnd)) === quote) {
    return { start: remembered, end: rememberedEnd, confidence: 'exact' };
  }

  const contextual = normalize(`${anchor.prefix || ''} ${quote} ${anchor.suffix || ''}`);
  const contextIndex = contextual ? haystack.indexOf(contextual) : -1;
  if (contextIndex >= 0) {
    const offset = contextIndex + normalize(anchor.prefix || '').length;
    return { start: offset, end: offset + quote.length, confidence: 'context' };
  }

  const first = haystack.indexOf(quote);
  if (first >= 0 && haystack.indexOf(quote, first + 1) === -1) {
    return { start: first, end: first + quote.length, confidence: 'quote' };
  }
  return null;
}
