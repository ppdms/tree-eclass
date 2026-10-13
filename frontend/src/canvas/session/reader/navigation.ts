import { useCallback, useEffect, useRef, useState } from 'react';
import type { Dispatch, MutableRefObject, SetStateAction } from 'react';
import { scrollAreaTo } from '../../app/browser';
import { clampPage, trackedPage, visiblePages } from './layout';
import type { DocumentLayout } from './layout';
import { pageOffset, reconcileLayout, type PageDestination } from './navigationPlan';
import type { ReaderSource } from './source';

export interface ReaderNavigation {
  offset: number;
  current: number;
  target: number;
  visible: number[];
  onUserScroll(): void;
  onScroll(offset: number): void;
  goToPage(page: number, fraction?: number): void;
  scrollBy(delta: number): void;
  requestCurrentPage(): void;
}
interface NavigationContext {
  id: string;
  source: ReaderSource | null;
  layout: DocumentLayout;
  requested: MutableRefObject<number>;
  height: number;
  viewportTop: number;
  scrollHeight: number;
}
interface NavigationPosition {
  target: number;
  setTarget: Dispatch<SetStateAction<number>>;
  previous: MutableRefObject<DocumentLayout | null>;
  destination: MutableRefObject<PageDestination | null>;
  currentOffset: number;
  offsetRef: MutableRefObject<number>;
  setOffset: Dispatch<SetStateAction<number | null>>;
}

function useNavigationPosition({ id, source, layout, requested, scrollHeight }: NavigationContext): NavigationPosition {
  const [offset, setOffset] = useState<number | null>(null);
  const [target, setTarget] = useState(requested.current);
  const previous = useRef<DocumentLayout | null>(null);
  const destination = useRef<PageDestination | null>(null);
  const currentOffset = offset ?? pageOffset(layout, { page: target, fraction: 0 }, scrollHeight);
  const offsetRef = useRef(currentOffset);
  offsetRef.current = currentOffset;
  useEffect(() => {
    if (!source) {
      previous.current = null;
      destination.current = null;
      setOffset(null);
      return;
    }
    requested.current = clampPage(requested.current, source.pageCount);
    const initial = pageOffset(layout, { page: requested.current, fraction: 0 }, scrollHeight);
    const next = previous.current
      ? reconcileLayout(previous.current, layout, offsetRef.current, destination.current, scrollHeight)
      : { offset: initial, scroll: { offset: initial, behavior: 'auto' as const } };
    setTarget(requested.current);
    setOffset(next.offset);
    if (next.scroll) scrollAreaTo(id, next.scroll.offset, { behavior: next.scroll.behavior });
    previous.current = layout;
  }, [id, source, layout, requested, scrollHeight]);
  return { target, setTarget, previous, destination, currentOffset, offsetRef, setOffset };
}

function usePageControls(
  { id, source, layout, requested, scrollHeight }: NavigationContext,
  { destination, setTarget, offsetRef }: NavigationPosition,
  current: number,
) {
  const goToPage = useCallback(
    (page: number, fraction = 0) => {
      const number = clampPage(page, source?.pageCount ?? Number.MAX_SAFE_INTEGER);
      requested.current = number;
      setTarget(number);
      destination.current = { page: number, fraction };
      scrollAreaTo(id, pageOffset(layout, destination.current, scrollHeight), { behavior: 'smooth' });
    },
    [id, source, layout, requested, scrollHeight, destination, setTarget],
  );
  const onUserScroll = useCallback(() => {
    destination.current = null;
    requested.current = current;
    setTarget(current);
  }, [current, requested, destination, setTarget]);
  return {
    goToPage,
    onUserScroll,
    scrollBy: (delta: number) => {
      destination.current = null;
      scrollAreaTo(id, offsetRef.current + delta);
    },
    requestCurrentPage: () => {
      if (destination.current) return;
      requested.current = current;
      setTarget(current);
    },
  };
}

function useNavigationScroll(
  { source, layout, viewportTop, height, requested, scrollHeight }: NavigationContext,
  { offsetRef, setOffset, destination, previous, setTarget }: NavigationPosition,
) {
  return useCallback(
    (next: number) => {
      // A painted new-layout frame can still carry the old scroll offset until
      // passive reconciliation anchors it. Do not replace the prior-layout anchor.
      if (!source || previous.current !== layout) return;
      offsetRef.current = next;
      setOffset(next);
      if (destination.current && Math.abs(next - pageOffset(layout, destination.current, scrollHeight)) <= 0.5) {
        destination.current = null;
      }
      if (destination.current) return;
      const visible = visiblePages(layout, next + viewportTop, height);
      if (!visible.length) return;
      const page = trackedPage(layout, visible, next + viewportTop, height);
      requested.current = page;
      setTarget(page);
    },
    [
      source,
      layout,
      viewportTop,
      height,
      requested,
      scrollHeight,
      offsetRef,
      setOffset,
      destination,
      previous,
      setTarget,
    ],
  );
}

export function useReaderNavigation(
  id: string,
  source: ReaderSource | null,
  layout: DocumentLayout,
  requested: MutableRefObject<number>,
  height: number,
  viewportTop: number,
  onPageChange: (page: number) => void,
  scrollHeight = height,
): ReaderNavigation {
  const context = { id, source, layout, requested, height, viewportTop, scrollHeight };
  const position = useNavigationPosition(context);
  const { currentOffset, previous, destination, target } = position;
  const visibleOffset =
    previous.current && previous.current !== layout
      ? reconcileLayout(previous.current, layout, currentOffset, destination.current, scrollHeight).offset
      : currentOffset;
  const visible = height > 0 ? visiblePages(layout, visibleOffset + viewportTop, height) : [];
  const current = trackedPage(layout, visible, visibleOffset + viewportTop, height);
  const reported = useRef<number | null>(null);
  useEffect(() => {
    if (source && visible.length && reported.current !== current) {
      reported.current = current;
      onPageChange(current);
    }
  }, [source, current, visible.length, onPageChange]);
  const controls = usePageControls(context, position, current);
  const onScroll = useNavigationScroll(context, position);
  return {
    offset: currentOffset,
    current,
    target: clampPage(target, source?.pageCount ?? target),
    visible,
    onScroll,
    ...controls,
  };
}
