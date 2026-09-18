import * as React from 'react';
import { fetchJson } from '@/lib/api';
import type { FileInsight, FileInsightAi } from '@/lib/types';

type Guide = { ai: FileInsightAi | null; source_hash: string };

export function useFileGuide(insight: FileInsight, open: boolean) {
  const key = [
    insight.course_id,
    insight.id,
    insight.source_hash,
    insight.enrichment_model,
    insight.enrichment_analysis_version,
    insight.enrichment_generated_at,
  ].join(':');
  const [cached, setCached] = React.useState<{ key: string; guide: Guide } | null>(null);
  const [error, setError] = React.useState(false);
  const [attempt, retry] = React.useReducer((n: number) => n + 1, 0);
  const guide = cached?.key === key ? cached.guide : null;
  React.useEffect(() => {
    if (!open || guide || insight.ai?.summary) return;
    const controller = new AbortController();
    setError(false);
    const base = `/api/v1/courses/${encodeURIComponent(String(insight.course_id))}`;
    fetchJson<Guide>(`${base}/files/${encodeURIComponent(insight.id || '')}/guide`, { signal: controller.signal })
      .then((value) => {
        if (!controller.signal.aborted) {
          const current = !insight.source_hash || value.source_hash === insight.source_hash;
          setCached({ key, guide: current ? value : { ...value, ai: null } });
        }
      })
      .catch(() => {
        if (!controller.signal.aborted) setError(true);
      });
    return () => controller.abort();
  }, [open, key, guide, insight.ai, insight.course_id, insight.id, insight.source_hash, attempt]);
  return {
    ai: insight.ai?.summary ? insight.ai : guide?.ai,
    loaded: Boolean(guide || insight.ai?.summary),
    error,
    retry,
  };
}
