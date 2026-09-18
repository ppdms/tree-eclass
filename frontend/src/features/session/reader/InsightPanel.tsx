import * as stylex from '@stylexjs/stylex';
import { useEffect, useState } from 'react';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { Separator } from '@/components/ui/separator';
import { Skeleton } from '@/components/ui/skeleton';
import { colors, typography } from '@/styles/tokens.stylex';
import type { PageInsight } from '@/features/session/reader/types';
import { errorLikeSchema, errorMessage } from '@/lib/errors';
import { api } from './api';

const SECTIONS: [keyof NonNullable<PageInsight['insight']>, string][] = [
  ['key_points', 'Key points'],
  ['definitions', 'Definitions'],
  ['formulas', 'Formulas'],
  ['examples', 'Examples'],
  ['assessment_clues', 'Exam clues'],
  ['visuals', 'Visuals'],
  ['references', 'References'],
];

const styles = stylex.create({
  section: {
    marginBlockEnd: '0.75rem',
  },
  sectionTitle: {
    color: colors.textSecondary,
    fontSize: '0.7rem',
    fontWeight: 600,
    letterSpacing: '0.08em',
    marginBlockEnd: 4,
    textTransform: 'uppercase',
  },
  sectionAccent: {
    color: colors.primary,
  },
  list: {
    margin: 0,
    padding: 0,
    gap: '0.25rem',
    listStyle: 'none',
    display: 'flex',
    flexDirection: 'column',
    fontSize: '0.8rem',
    lineHeight: typography.leadingRelaxed,
  },
  listItem: {
    borderInlineStartColor: colors.border,
    borderInlineStartStyle: 'solid',
    borderInlineStartWidth: '2px',
    paddingInlineStart: '0.5rem',
  },
  container: {
    padding: '0.75rem',
  },
  skeletonContainer: {
    padding: '0.75rem',
    gap: '0.5rem',
    display: 'flex',
    flexDirection: 'column',
  },
  skeletonH: {
    height: '0.75rem',
  },
  skeletonW34: {
    width: '75%',
  },
  skeletonWFull: {
    width: '100%',
  },
  skeletonW56: {
    width: '83%',
  },
  loadingText: {
    color: colors.textSecondary,
    fontSize: '0.75rem',
    paddingTop: '0.25rem',
  },
  alertNotice: {
    paddingBlock: '0.5rem',
    paddingInline: '0.75rem',
    fontSize: '0.7rem',
    marginBottom: '0.5rem',
  },
  summaryText: {
    fontSize: '0.82rem',
    lineHeight: typography.leadingRelaxed,
    marginBottom: '0.75rem',
  },
  separator: {
    marginBlock: '1rem',
  },
  disclaimer: {
    color: colors.textSecondary,
    fontSize: '0.68rem',
    paddingTop: '0.5rem',
  },
  unanalysed: {
    padding: '0.75rem',
    color: colors.textSecondary,
    fontSize: '0.75rem',
  },
  alertError: {
    paddingBlock: '0.5rem',
    paddingInline: '0.75rem',
    fontSize: '0.75rem',
  },
});

function List({ title, items, accent }: { title: string; items: string[] | string | undefined; accent: boolean }) {
  if (!items?.length) return null;
  return (
    <section {...stylex.props(styles.section)}>
      <h3 {...stylex.props(styles.sectionTitle, accent && styles.sectionAccent)}>{title}</h3>
      <ul {...stylex.props(styles.list)}>
        {(Array.isArray(items) ? items : [items]).map((item, index) => (
          <li key={index} {...stylex.props(styles.listItem)}>
            {String(item)}
          </li>
        ))}
      </ul>
    </section>
  );
}

function LoadingState({ pageNumber }: { pageNumber: number }) {
  return (
    <div {...stylex.props(styles.skeletonContainer)} role="status" aria-live="polite">
      <Skeleton {...stylex.props(styles.skeletonH, styles.skeletonW34)} />
      <Skeleton {...stylex.props(styles.skeletonH, styles.skeletonWFull)} />
      <Skeleton {...stylex.props(styles.skeletonH, styles.skeletonW56)} />
      <p {...stylex.props(styles.loadingText)}>Reading page {pageNumber}…</p>
    </div>
  );
}

function ErrorState({ message }: { message: string }) {
  return (
    <div {...stylex.props(styles.container)}>
      <Alert variant="destructive" {...stylex.props(styles.alertError)}>
        <AlertDescription>{message}</AlertDescription>
      </Alert>
    </div>
  );
}

function UnanalysedState({ pageNumber, status }: { pageNumber: number; status?: string }) {
  return (
    <p {...stylex.props(styles.unanalysed)}>
      {status ? `Analysis for page ${pageNumber} is ${status}.` : `Page ${pageNumber} has not been analysed yet.`}
    </p>
  );
}

function InsightBody({ page }: { page: PageInsight }) {
  const insight = page.insight || {};

  return (
    <div {...stylex.props(styles.container)}>
      {page.stale ? (
        <Alert role="note" aria-label="Analysis version notice" {...stylex.props(styles.alertNotice)}>
          <AlertDescription>This analysis was generated from an earlier version of the file.</AlertDescription>
        </Alert>
      ) : null}

      {insight.summary ? <p {...stylex.props(styles.summaryText)}>{insight.summary}</p> : null}

      {SECTIONS.map(([field, title]) => (
        <List key={field} title={title} items={insight[field]} accent={field === 'assessment_clues'} />
      ))}

      <Separator {...stylex.props(styles.separator)} />
      <p {...stylex.props(styles.disclaimer)}>
        AI-derived reading aid from {page.model || 'the page analysis'}. The page itself is beside it — check it before
        trusting a claim.
      </p>
    </div>
  );
}

interface InsightState {
  loading: boolean;
  page: PageInsight | null;
  error: string | null;
}

export interface InsightPanelProps {
  courseId: string | number;
  documentId: string | number | null | undefined;
  pageNumber: number;
}

export default function InsightPanel({ courseId, documentId, pageNumber }: InsightPanelProps) {
  const [state, setState] = useState<InsightState>({ loading: true, page: null, error: null });

  useEffect(() => {
    if (!documentId || !pageNumber) return undefined;
    const controller = new AbortController();
    setState((current) => ({ ...current, loading: true }));

    api
      .pages(courseId, documentId, pageNumber, pageNumber, controller.signal)
      .then((result) => {
        if (controller.signal.aborted) return;
        const page = (result.pages || []).find((item) => item.page_number === pageNumber);
        setState({ loading: false, page: page || null, error: null });
      })
      .catch((error) => {
        if (!controller.signal.aborted) {
          const parsed = errorLikeSchema.safeParse(error);
          const errorLike = parsed.success ? parsed.data : { message: String(error) };
          setState({
            loading: false,
            page: null,
            error: errorMessage(errorLike, 'Could not load the page insight.'),
          });
        }
      });

    return () => {
      controller.abort();
    };
  }, [courseId, documentId, pageNumber]);

  if (state.loading) return <LoadingState pageNumber={pageNumber} />;
  if (state.error) return <ErrorState message={state.error} />;
  if (!state.page) return <UnanalysedState pageNumber={pageNumber} />;
  if (state.page.status !== 'ready') {
    return <UnanalysedState pageNumber={pageNumber} status={state.page.status} />;
  }
  return <InsightBody page={state.page} />;
}
