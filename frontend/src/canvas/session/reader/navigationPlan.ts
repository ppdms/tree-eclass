import type { DocumentLayout } from './layout';
import { clampPage, visiblePages } from './layout';

export interface PageDestination {
  page: number;
  fraction: number;
}
export interface LayoutReconciliation {
  offset: number;
  scroll: { offset: number; behavior: 'auto' | 'smooth' } | null;
}

export function pageOffset(layout: DocumentLayout, destination: PageDestination, viewportHeight = 0): number {
  const bounds = layout.pages[clampPage(destination.page, layout.pages.length) - 1];
  const offset = Math.max(0, (bounds?.top ?? 16) - 16 + (bounds?.height ?? 0) * destination.fraction);
  return Math.min(offset, Math.max(0, layout.height - viewportHeight));
}

/** Size discoveries preserve a jump's destination, not an intermediate spring sample. */
export function reconcileLayout(
  previous: DocumentLayout,
  next: DocumentLayout,
  offset: number,
  destination: PageDestination | null,
  viewportHeight = 0,
): LayoutReconciliation {
  if (destination) {
    const before = pageOffset(previous, destination, viewportHeight);
    const after = pageOffset(next, destination, viewportHeight);
    return { offset, scroll: before === after ? null : { offset: after, behavior: 'smooth' } };
  }
  const number = visiblePages(previous, offset, 1)[0] ?? previous.pages.at(-1)?.page ?? 1;
  const before = previous.pages[number - 1];
  const after = next.pages[number - 1];
  const adjusted = before && after ? after.top + ((offset - before.top) / before.height) * after.height : offset;
  return { offset: adjusted, scroll: adjusted === offset ? null : { offset: adjusted, behavior: 'auto' } };
}

/** An offscreen, unpainted target cannot exclude the page the user is now viewing. */
export function neededPages(
  visible: number[],
  target: number,
  current: number,
  count: number,
  targetReady: boolean,
): number[] {
  const pages = new Set<number>();
  if (!targetReady) pages.add(target);
  if (visible.includes(current)) pages.add(current);
  for (const page of visible) pages.add(page);
  if (targetReady && visible.length) {
    if (visible[0]! > 1) pages.add(visible[0]! - 1);
    if (visible.at(-1)! < count) pages.add(visible.at(-1)! + 1);
  }
  return [...pages];
}
