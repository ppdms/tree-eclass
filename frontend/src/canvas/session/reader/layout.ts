import type { PageSize } from '../../pdf';

export interface PageLayout {
  page: number;
  top: number;
  left: number;
  width: number;
  height: number;
}
export interface DocumentLayout {
  pages: PageLayout[];
  width: number;
  height: number;
}

export function documentLayout(
  count: number,
  fallback: PageSize,
  sizes: Map<number, PageSize>,
  zoom: number,
  availableWidth: number,
): DocumentLayout {
  let width = Math.max(availableWidth, fallback.width * zoom + 64);
  for (const size of sizes.values()) width = Math.max(width, size.width * zoom + 64);
  const pages: PageLayout[] = [];
  let top = 16;
  for (let page = 1; page <= count; page++) {
    const size = sizes.get(page) ?? fallback;
    const pageWidth = size.width * zoom;
    const height = size.height * zoom;
    pages.push({ page, top, left: (width - pageWidth) / 2, width: pageWidth, height });
    top += height + 16;
  }
  return { pages, width, height: top };
}

export function visiblePages(layout: DocumentLayout, offset: number, height: number): number[] {
  let low = 0;
  let high = layout.pages.length;
  while (low < high) {
    const middle = (low + high) >>> 1;
    const page = layout.pages[middle]!;
    if (page.top + page.height < offset) low = middle + 1;
    else high = middle;
  }
  const result: number[] = [];
  for (let i = low; i < layout.pages.length && layout.pages[i]!.top < offset + height; i++) {
    result.push(layout.pages[i]!.page);
  }
  return result;
}

export function trackedPage(layout: DocumentLayout, visible: number[], offset: number, height: number): number {
  let best = visible[0] ?? 1;
  let visibleHeight = -1;
  for (const number of visible) {
    const page = layout.pages[number - 1]!;
    const overlap = Math.min(page.top + page.height, offset + height) - Math.max(page.top, offset);
    if (overlap > visibleHeight) {
      best = number;
      visibleHeight = overlap;
    }
  }
  return best;
}

export function fitZoom(mode: 'width' | 'page', width: number, height: number, size: PageSize): number {
  const widthScale = Math.max(1, width - 64) / size.width;
  const zoom = mode === 'page' ? Math.min(widthScale, Math.max(1, height - 32) / size.height) : widthScale;
  return Math.max(0.4, Math.min(3, zoom));
}

export function clampPage(page: number, count: number): number {
  return Math.max(1, Math.min(count || 1, Number.isFinite(page) ? Math.round(page) : 1));
}
