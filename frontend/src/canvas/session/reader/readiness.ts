import { useCallback, useEffect, useState } from 'react';
import type { ReaderResources } from './resources';
import type { ReaderNavigation } from './navigation';
import { rasterWidth } from './cache';
import type { PageCache } from './cache';
import { neededPages } from './navigationPlan';

export interface ReaderReadiness {
  ready: boolean;
  firstReady: boolean;
  error: string | null;
  pages: number[];
  onDrawn(page: number, image: string): void;
}

function useDrawnPages(cache: PageCache | null, zoom: number, dpr: number, onFirstDrawn: (identity: number) => void) {
  const [drawnPages, setDrawnPages] = useState(new Map<number, string>());
  const [firstDrawn, setFirstDrawn] = useState(0);
  const onDrawn = useCallback(
    (page: number, image: string) => {
      const entry = cache?.pages.get(page);
      if (entry?.image !== image || !entry.geometry || !entry.size) return;
      if (entry.width !== rasterWidth(entry.size, zoom, dpr)) return;
      setDrawnPages((previous) => {
        if (previous.get(page) === image) return previous;
        const next = new Map<number, string>();
        for (const [number, cached] of cache!.pages) {
          if (cached.image && previous.get(number) === cached.image) next.set(number, cached.image);
        }
        next.set(page, image);
        return next;
      });
      setFirstDrawn(cache!.identity);
      onFirstDrawn(cache!.identity);
    },
    [cache, zoom, dpr, onFirstDrawn],
  );
  return { drawnPages, firstDrawn, onDrawn };
}

export function useReaderReadiness(
  resources: ReaderResources,
  navigation: ReaderNavigation,
  zoom: number,
  dpr: number,
  onFirstDrawn: (identity: number) => void,
): ReaderReadiness {
  const { cache, source, revision } = resources;
  const { drawnPages, firstDrawn, onDrawn } = useDrawnPages(cache, zoom, dpr, onFirstDrawn);
  const target = cache?.pages.get(navigation.target);
  const targetReady =
    !!target?.image &&
    drawnPages.get(navigation.target) === target.image &&
    !!target.size &&
    target.width === rasterWidth(target.size, zoom, dpr);
  const current = cache?.pages.get(navigation.current);
  const currentReady =
    !!current?.image &&
    !!current.geometry &&
    drawnPages.get(navigation.current) === current.image &&
    !!current.size &&
    current.width === rasterWidth(current.size, zoom, dpr);
  const ready = !!source && navigation.visible.length > 0 && currentReady;
  const pages = source
    ? neededPages(navigation.visible, navigation.target, navigation.current, source.pageCount, targetReady)
    : [];
  const signature = pages.join(',');
  useEffect(() => {
    if (cache) cache.update(signature ? signature.split(',').map(Number) : [], zoom, dpr);
  }, [cache, signature, zoom, dpr, revision]);
  const error =
    resources.error ??
    current?.error ??
    (navigation.visible.includes(navigation.target) ? target?.error : null) ??
    null;
  return { ready, firstReady: !!cache && firstDrawn === cache.identity, error, pages, onDrawn };
}
