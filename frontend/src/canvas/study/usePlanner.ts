import { useRef, useState } from 'react';
import type { JsonValue } from '@/lib/display';
import { request } from '../app/api';
import { plannerErrorMessages, plannerPayload, validatePlanner, type PlannerDraft } from './plannerModel';

export interface PlannerState {
  state: string | null;
  errors: string[];
  saving: boolean;
  save: (draft: PlannerDraft) => Promise<void>;
}

async function submitPlanner(draft: PlannerDraft): Promise<string[]> {
  try {
    const response = await request('/api/v1/study/planner', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
      body: JSON.stringify(plannerPayload(draft)),
    });
    const body: JsonValue = await response.json().catch(() => ({}));
    return response.ok ? [] : plannerErrorMessages(body, response.status);
  } catch (error) {
    return [error instanceof Error ? error.message : 'Could not save study plan. Check your connection and try again.'];
  }
}

export function usePlanner(onSaved: () => void, onClose: () => void): PlannerState {
  const [state, setState] = useState<string | null>(null);
  const [errors, setErrors] = useState<string[]>([]);
  const pending = useRef(false);
  const save = async (draft: PlannerDraft) => {
    if (pending.current) return;
    const issues = validatePlanner(draft);
    if (issues.length) {
      setErrors(issues);
      setState(null);
      return;
    }
    pending.current = true;
    setState('Saving study plan…');
    setErrors([]);
    const messages = await submitPlanner(draft);
    pending.current = false;
    if (messages.length) {
      setErrors(messages);
      setState(null);
      return;
    }
    setState('Study plan saved.');
    onSaved();
    onClose();
  };
  return { state, errors, saving: state === 'Saving study plan…', save };
}
