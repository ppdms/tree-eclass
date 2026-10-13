import { createContext, useContext, useSyncExternalStore } from 'react';
import type { ActivityGroup, InboxPayload } from '@/lib/types';
import { fetchJson } from '@/lib/api';

type ActivityId = string | number;
type Listener = () => void;

export class ActivityStore {
  private ids: Set<ActivityId>;
  private listeners = new Set<Listener>();

  constructor(
    ids: ActivityId[],
    private changed: (ids: ActivityId[]) => void,
  ) {
    this.ids = new Set(ids);
  }

  getSnapshot = (): Set<ActivityId> => this.ids;

  subscribe = (listener: Listener): (() => void) => {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  };

  replace(ids: ActivityId[]): void {
    const next = new Set(ids);
    if (next.size === this.ids.size && [...next].every((id) => this.ids.has(id))) return;
    this.ids = next;
    this.listeners.forEach((listener) => listener());
  }

  toggle(id: ActivityId): void {
    const next = new Set(this.ids);
    if (next.has(id)) next.delete(id);
    else next.add(id);
    this.ids = next;
    this.listeners.forEach((listener) => listener());
    this.changed([...next]);
  }
}

export function useReadIds(store: ActivityStore): Set<ActivityId> {
  return useSyncExternalStore(store.subscribe, store.getSnapshot, store.getSnapshot);
}

export const ActivityStoreContext = createContext<ActivityStore | null>(null);

export function useActivityStore(): ActivityStore {
  const store = useContext(ActivityStoreContext);
  if (!store) throw new Error('Activity read state is outside the application provider');
  return store;
}

export interface FeedSnapshot {
  groups: ActivityGroup[];
  offset: number;
  hasMore: boolean;
  loaded: boolean;
  loading: boolean;
  error: 'first' | 'older' | null;
}

export class ActivityFeed {
  private state: FeedSnapshot = {
    groups: [],
    offset: 0,
    hasMore: false,
    loaded: false,
    loading: true,
    error: null,
  };
  private listeners = new Set<Listener>();
  private request: AbortController | null = null;
  private reportedData = false;

  constructor(private onData: () => void) {}

  getSnapshot = (): FeedSnapshot => this.state;

  subscribe = (listener: Listener): (() => void) => {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  };

  private publish(patch: Partial<FeedSnapshot>): void {
    this.state = { ...this.state, ...patch };
    this.listeners.forEach((listener) => listener());
  }

  cancel = (): void => {
    this.request?.abort();
    this.request = null;
    if (this.state.loading) this.publish({ loading: false });
  };

  loadFirst = async (signal?: AbortSignal): Promise<InboxPayload | null> => {
    this.cancel();
    return this.load(0, 10, false, signal);
  };

  loadOlder = async (): Promise<InboxPayload | null> => {
    if (!this.state.loaded || !this.state.hasMore || this.state.loading) return null;
    return this.load(this.state.offset, 30, true);
  };

  retry = async (): Promise<InboxPayload | null> => {
    if (this.state.error === 'first') return this.loadFirst();
    if (this.state.error === 'older') return this.loadOlder();
    return null;
  };

  private async load(
    offset: number,
    limit: number,
    append: boolean,
    signal?: AbortSignal,
  ): Promise<InboxPayload | null> {
    const controller = new AbortController();
    const abort = () => controller.abort();
    signal?.addEventListener('abort', abort, { once: true });
    if (signal?.aborted) controller.abort();
    this.request = controller;
    this.publish({ loading: true, error: null });
    try {
      const path = `/api/v1/inbox?include_timeline=false&limit=${limit}&offset=${offset}`;
      const payload = await fetchJson<InboxPayload>(path, { signal: controller.signal });
      if (controller.signal.aborted || this.request !== controller) return null;
      if (!this.reportedData) {
        this.reportedData = true;
        this.onData();
      }
      const incoming = payload.groups || [];
      this.publish({
        groups: append ? [...this.state.groups, ...incoming] : incoming,
        offset: payload.next_offset ?? offset + limit,
        hasMore: Boolean(payload.has_more) && incoming.length > 0,
        loaded: true,
      });
      return payload;
    } catch {
      if (!controller.signal.aborted && this.request === controller) {
        this.publish({ error: append ? 'older' : 'first' });
      }
      return null;
    } finally {
      signal?.removeEventListener('abort', abort);
      if (this.request === controller) {
        this.request = null;
        this.publish({ loading: false });
      }
    }
  }
}

export function useActivityFeed(feed: ActivityFeed): FeedSnapshot {
  return useSyncExternalStore(feed.subscribe, feed.getSnapshot, feed.getSnapshot);
}
