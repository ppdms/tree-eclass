import { useEffect, useRef, type RefObject } from 'react';

function visibleHeight(node: Element, containerRect: DOMRect): number {
  const rect = node.getBoundingClientRect();
  return Math.max(0, Math.min(rect.bottom, containerRect.bottom) - Math.max(rect.top, containerRect.top));
}

function pageWithMostVisibleArea(container: Element): number {
  const containerRect = container.getBoundingClientRect();
  let winner = 1;
  let winnerHeight = 0;
  container.querySelectorAll<HTMLElement>('[data-page]').forEach((node) => {
    const height = visibleHeight(node, containerRect);
    if (height > winnerHeight) {
      winnerHeight = height;
      winner = Number(node.dataset.page) || winner;
    }
  });
  return winner;
}

/** Track the page with the greatest visible height, including after lazy pages resize. */
export function usePageTracking(
  containerRef: RefObject<HTMLElement | null>,
  pageCount: number,
  onPageChange?: (page: number) => void,
): void {
  const frame = useRef<number | null>(null);
  const onPageChangeRef = useRef(onPageChange);
  const lastPage = useRef<number | null>(null);
  onPageChangeRef.current = onPageChange;

  useEffect(() => {
    const container = containerRef.current;
    if (!container || !pageCount) return undefined;

    const report = (): void => {
      frame.current = null;
      const page = pageWithMostVisibleArea(container);
      if (page !== lastPage.current) {
        lastPage.current = page;
        onPageChangeRef.current?.(page);
      }
    };
    const schedule = (): void => {
      if (frame.current === null) frame.current = requestAnimationFrame(report);
    };
    const resize = new ResizeObserver(schedule);
    resize.observe(container);
    container.querySelectorAll('[data-page]').forEach((node) => resize.observe(node));
    container.addEventListener('scroll', schedule, { passive: true });
    schedule();

    return () => {
      if (frame.current !== null) cancelAnimationFrame(frame.current);
      resize.disconnect();
      container.removeEventListener('scroll', schedule);
      lastPage.current = null;
    };
  }, [containerRef, pageCount]);
}
