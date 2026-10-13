import * as React from 'react';
import type { FileInsight, FileInsightAi } from '@/lib/types';
import { requestJson } from '../app/api';
import { guideIdentity } from './helpers';

interface Guide {
  document_id?: string;
  ai: FileInsightAi | null;
  source_hash: string;
  status?: string;
}
interface CachedGuide {
  key: string;
  guide: Guide;
}

export function useFileGuide(insight: FileInsight, open: boolean) {
  const key = guideIdentity(insight);
  const [cached, setCached] = React.useState<CachedGuide | null>(null);
  const [failed, setFailed] = React.useState<string | null>(null);
  const [attempt, retry] = React.useReducer((value: number) => value + 1, 0);
  const guide = cached?.key === key ? cached.guide : null;
  const inline = insight.ai?.summary ? insight.ai : null;
  React.useEffect(() => {
    if (!open || guide || inline) return;
    const controller = new AbortController();
    setFailed(null);
    const base = `/api/v1/courses/${encodeURIComponent(String(insight.course_id))}`;
    const url = `${base}/files/${encodeURIComponent(insight.id || '')}/guide`;
    void requestJson<Guide>(url, { signal: controller.signal }).then(
      (value) => {
        if (controller.signal.aborted) return;
        const currentSource = !insight.source_hash || value.source_hash === insight.source_hash;
        const currentDocument = !value.document_id || value.document_id === insight.id;
        setCached({ key, guide: currentSource && currentDocument ? value : { ...value, ai: null } });
      },
      () => {
        if (!controller.signal.aborted) setFailed(key);
      },
    );
    return () => controller.abort();
  }, [open, key, guide, inline, insight.course_id, insight.id, insight.source_hash, attempt]);
  return { ai: inline || guide?.ai, loaded: Boolean(inline || guide), error: failed === key, retry };
}
