import type * as React from 'react';
import * as stylex from '@stylexjs/stylex';
import { useCallback, useEffect, useRef, useState } from 'react';
import { Button } from '@/components/ui/button';
import { fetchJson } from '@/lib/api';
import type { ActivityGroup, InboxPayload } from '@/lib/types';
import { activityPartsStyles } from './activityPartsStyles';
import { ActivityDayList } from './activityParts';

const styles = activityPartsStyles;

function useFeedSentinel(
  sentinelRef: React.RefObject<HTMLDivElement | null>,
  hasMore: boolean,
  loadOlder: () => Promise<void>,
) {
  useEffect(() => {
    const node = sentinelRef.current;
    if (!node || !hasMore) return;
    const observer = new IntersectionObserver(
      ([entry]) => {
        if (entry?.isIntersecting) void loadOlder();
      },
      { rootMargin: '30% 0%' },
    );
    observer.observe(node);
    return () => observer.disconnect();
  }, [hasMore, loadOlder]);
}

function useActivityFeedLoader(initialOffset: number, initialHasMore: boolean) {
  const [olderGroups, setOlderGroups] = useState<ActivityGroup[]>([]);
  const [offset, setOffset] = useState(initialOffset);
  const [hasMore, setHasMore] = useState(initialHasMore);
  const [loading, setLoading] = useState(false);
  const sentinelRef = useRef<HTMLDivElement>(null);
  const loadingRef = useRef(false);
  const request = useRef<AbortController | null>(null);
  const [error, setError] = useState(false);
  useEffect(() => () => request.current?.abort(), []);

  const loadOlder = useCallback(async () => {
    if (loadingRef.current || !hasMore) return;
    loadingRef.current = true;
    setLoading(true);
    setError(false);
    const controller = new AbortController();
    request.current = controller;
    try {
      const payload = await fetchJson<InboxPayload>(`api/v1/inbox?include_timeline=false&limit=30&offset=${offset}`, {
        signal: controller.signal,
      });
      if (controller.signal.aborted) return;
      const incoming = payload.groups || [];
      if (incoming.length > 0) {
        setOlderGroups((current) => [...current, ...incoming]);
      }
      setOffset(payload.next_offset ?? offset + 30);
      setHasMore(Boolean(payload.has_more) && incoming.length > 0);
    } catch {
      if (!controller.signal.aborted) setError(true);
    } finally {
      loadingRef.current = false;
      if (!controller.signal.aborted) setLoading(false);
    }
  }, [hasMore, offset]);

  useFeedSentinel(sentinelRef, hasMore && !error, loadOlder);

  return { olderGroups, hasMore, loading, sentinelRef, error, loadOlder };
}

export function ActivityFeedLoader({
  initialOffset = 30,
  hasMore: initialHasMore = false,
}: {
  initialOffset?: number;
  hasMore?: boolean;
}) {
  const { olderGroups, hasMore, loading, sentinelRef, error, loadOlder } = useActivityFeedLoader(
    initialOffset,
    initialHasMore,
  );

  if (!initialHasMore && olderGroups.length === 0) return null;

  return (
    <>
      {olderGroups.length > 0 && <ActivityDayList groups={olderGroups} />}
      {error && (
        <p role="alert">
          Older updates could not load. <Button onClick={() => void loadOlder()}>Try again</Button>
        </p>
      )}
      {hasMore && (
        <div
          ref={sentinelRef}
          {...stylex.props(styles.activityLoadSentinel)}
          role={loading ? 'status' : undefined}
          aria-live="polite"
        >
          {loading ? 'Fetching older updates…' : null}
        </div>
      )}
    </>
  );
}
