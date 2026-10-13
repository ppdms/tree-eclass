import { useCallback, useEffect, useReducer, useState } from 'react';
import { requestJson } from '../app/api';
import type { Conversation } from './model';

export function useConversationHistory(enabled: boolean, refreshed: number) {
  const [attempt, invalidate] = useReducer((value: number) => value + 1, 0);
  const key = `${refreshed}:${attempt}`;
  const [cached, setCached] = useState<{ key: string; items: Conversation[] } | null>(null);
  const [failed, setFailed] = useState<string | null>(null);
  const items = cached?.key === key ? cached.items : null;
  useEffect(() => {
    if (!enabled || items !== null) return;
    const controller = new AbortController();
    setFailed(null);
    requestJson<{ conversations?: Conversation[] }>('/api/ask/conversations', { signal: controller.signal })
      .then((payload) => {
        if (!controller.signal.aborted) setCached({ key, items: payload.conversations || [] });
      })
      .catch(() => {
        if (!controller.signal.aborted) setFailed(key);
      });
    return () => controller.abort();
  }, [enabled, items, key]);
  const retry = useCallback(() => invalidate(), []);
  return { items: items || [], failed: failed === key, pending: enabled && items === null && failed !== key, retry };
}
