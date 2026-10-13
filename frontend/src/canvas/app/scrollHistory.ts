import type { JsonValue } from '@/lib/display';

export const scrollStorageKey = 'tree-eclass:scroll-positions';

function isScrollRecord(value: JsonValue): value is { [key: string]: JsonValue } {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}
function isScrollOffset(value: JsonValue): value is number {
  return typeof value === 'number' && Number.isFinite(value);
}

export function decodeScrollPositions(raw: string | null | undefined): Record<string, number> {
  const positions: Record<string, number> = Object.create(null);
  if (!raw) return positions;
  try {
    const value: JsonValue = JSON.parse(raw);
    if (!isScrollRecord(value)) return positions;
    for (const [key, offset] of Object.entries(value)) {
      if (isScrollOffset(offset)) positions[key] = offset;
    }
  } catch {
    /* Scroll restoration is a preference, not a prerequisite for opening a page. */
  }
  return positions;
}

export function anchorId(url: string): string | null {
  const hash = new URL(url, 'https://canvas.invalid').hash.slice(1);
  if (!hash) return null;
  try {
    return decodeURIComponent(hash);
  } catch {
    return hash;
  }
}
