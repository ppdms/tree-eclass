import { useEffect, useState } from 'react';
import { Notice } from '../ui/controls';
import { Text, View } from '../ui/primitives';
import { api, message } from './api';
import { styles } from './styles';
import type { PageInsight, Source } from './types';

const sections = [
  ['key_points', 'Key points'],
  ['definitions', 'Definitions'],
  ['formulas', 'Formulas'],
  ['examples', 'Examples'],
  ['assessment_clues', 'Exam clues'],
  ['visuals', 'Visuals'],
  ['references', 'References'],
] as const;
export function InsightPanel({ course, source, page }: { course: string; source: Source | null; page: number }) {
  const identity = JSON.stringify([course, source?.document_id, source?.source_hash, page]);
  const [state, setState] = useState<{ identity: string; data: PageInsight | null; error: string | null }>({
    identity: '',
    data: null,
    error: null,
  });
  useEffect(() => {
    if (!source) return;
    const controller = new AbortController();
    void api
      .pages(course, String(source.document_id), page, controller.signal)
      .then((result) => {
        if (result.source_hash !== source.source_hash) {
          throw new Error('This aid belongs to different source bytes. Reopen the document before using it.');
        }
        if (!controller.signal.aborted)
          setState({ identity, data: result.pages.find((item) => item.page_number === page) ?? null, error: null });
      })
      .catch((error) => {
        if (!controller.signal.aborted) setState({ identity, data: null, error: message(error) });
      });
    return () => controller.abort();
  }, [identity]);
  if (!source) return <Text style={styles.muted}>Choose a document to read its page insight.</Text>;
  if (state.identity !== identity) return <Text style={styles.muted}>Reading page {page}…</Text>;
  if (state.error) return <Notice tone="danger">{state.error}</Notice>;
  if (!state.data) return <Text style={styles.muted}>Page {page} has not been analysed yet.</Text>;
  if (state.data.status !== 'ready')
    return (
      <Text style={styles.muted}>
        Analysis for page {page} is {state.data.status}.
      </Text>
    );
  return <InsightBody page={state.data} />;
}
function InsightBody({ page }: { page: PageInsight }) {
  const insight: NonNullable<PageInsight['insight']> = page.stale ? {} : (page.insight ?? {});
  return (
    <View style={styles.column}>
      {page.stale && <Notice tone="warning">This analysis was generated from an earlier version of the file.</Notice>}
      {insight.summary && <Text style={styles.body}>{String(insight.summary)}</Text>}
      {sections.map(([field, title]) => {
        const items = insight[field];
        if (!items?.length) return null;
        return (
          <View key={field} style={styles.column}>
            <Text style={styles.title}>{title}</Text>
            {(Array.isArray(items) ? items : [items]).map((item, index) => (
              <Text key={`${field}:${index}:${String(item)}`} style={styles.body}>
                • {String(item)}
              </Text>
            ))}
          </View>
        );
      })}
      <Text style={styles.muted}>
        AI-derived reading aid from {page.model || 'the page analysis'}. Check the source page before trusting a claim.
      </Text>
    </View>
  );
}
