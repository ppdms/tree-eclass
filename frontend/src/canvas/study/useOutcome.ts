import { useRef, useState } from 'react';
import type { StudyAction } from '@/lib/types';
import { requestJson } from '../app/api';
import { requestKey } from '../session/api';
import { outcomeBody, type OutcomeResponse, type OutcomeType, type Reflection } from './outcomeModel';

export function useOutcome(action: StudyAction, onRecorded: (result: OutcomeResponse) => void) {
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [fields, setFields] = useState<Reflection>({
    minutes: String(action.minutes || action.remaining_minutes || 15),
    confidence: '',
    note: '',
  });
  const keys = useRef(new Map<OutcomeType, string>());
  const pending = useRef(false);
  const record = async (type: OutcomeType) => {
    if (pending.current) return;
    setError(null);
    const result = outcomeBody(action, type, fields);
    if ('error' in result) {
      setError(result.error);
      return;
    }
    pending.current = true;
    setBusy(true);
    const key = keys.current.get(type) || requestKey('study');
    keys.current.set(type, key);
    try {
      const saved = await requestJson<OutcomeResponse>('/api/v1/study/actions/event', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ ...result.body, idempotency_key: key }),
      });
      keys.current.delete(type);
      onRecorded(saved);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Could not record progress');
    } finally {
      pending.current = false;
      setBusy(false);
    }
  };
  return { fields, setFields, busy, error, record };
}
