import { useEffect, useRef, useState } from 'react';
import type { Dispatch, SetStateAction } from 'react';
import { mergePracticeUnit } from './model';
import type { CoursePracticeQuestion, CoursePracticeView, RecordedPracticeAttempt } from './model';

export function usePracticePublication(
  view: CoursePracticeView | null,
  setView: Dispatch<SetStateAction<CoursePracticeView | null>>,
  generation: number,
  fresh: boolean,
  revision: string | undefined,
) {
  const [status, setStatus] = useState('');
  const [refreshError, setRefreshError] = useState<string | null>(null);
  const [refreshStale, setRefreshStale] = useState<number | null>(null);
  const latestGeneration = useRef(generation);
  latestGeneration.current = generation;
  const acknowledgedGeneration = useRef(0);
  useEffect(() => {
    if (!fresh || generation <= acknowledgedGeneration.current) return;
    acknowledgedGeneration.current = generation;
    if (refreshStale !== null && generation > refreshStale) setRefreshStale(null);
    setRefreshError(null);
  }, [generation, fresh, refreshStale]);
  const onCommitted = (saved: RecordedPracticeAttempt, question: CoursePracticeQuestion) => {
    const changed = Boolean(saved.practice && view?.blueprint_revision_id !== saved.practice.blueprint_revision_id);
    if (changed) {
      setRefreshStale(latestGeneration.current);
      if (!saved.practice?.blueprint_revision_id) setView(saved.practice);
    } else if (saved.practice) {
      const refreshed = saved.practice;
      setView((current) =>
        current ? mergePracticeUnit(current, refreshed, saved.attempt.unit_key || question.unit_key) : refreshed,
      );
    }
    setStatus(`Recorded: ${saved.attempt.outcome || 'attempt'}`);
    setRefreshError(saved.practice_error || (saved.practice ? null : 'Attempt saved. Practice refresh is pending.'));
  };
  const stale = refreshStale !== null || Boolean(revision && view && revision !== view.blueprint_revision_id);
  return { status, refreshError, stale, onStarted: () => setStatus(''), onCommitted };
}
