import * as stylex from '@stylexjs/stylex';
import { Icon } from '@/components/Icon';
import * as React from 'react';
import { buttonStyles } from '@/components/ui/styles';
import { fetchJson } from '@/lib/api';
import { errorLikeSchema, errorMessage } from '@/lib/errors';
import { isServer } from '@/lib/display';
import type { StudyAction, StudyInboxItem, StudyPayload } from '@/lib/types';
import { commonStyles } from '@/styles/common';
import { media } from '@/styles/constants.stylex';
import { colors, spacing, typography } from '@/styles/tokens.stylex';
import { PrimaryAction } from './primary';
import { Alternatives } from './alternatives';
import { Planner } from './planner';
import { ExamHorizon } from './calendar';
import { RunCheckButton } from './support';

const styles = stylex.create({
  studyDecisionPage: {
    marginInline: 'auto',
    maxWidth: '73.75rem',
    minWidth: 0,
    width: '100%',
  },
  studyEmptyCard: {
    padding: '2rem',
    borderColor: colors.border,
    borderRadius: '1rem',
    borderStyle: 'solid',
    borderWidth: 1,
    placeItems: 'center',
    backgroundColor: colors.surface,
    display: 'grid',
    textAlign: 'center',
    minHeight: '20rem',
  },
  studyEmptyActions: {
    gap: spacing.sm,
    display: 'flex',
    marginTop: spacing.md,
  },
  studyLoading: {
    paddingBlock: '3rem',
    color: colors.textSecondary,
    fontSize: typography.sizeSm,
    textAlign: 'center',
  },
  studyDashboardGrid: {
    gap: '1rem',
    alignItems: 'start',
    display: 'grid',
    gridTemplateColumns: {
      default: 'minmax(0, 1fr) 19rem',
      [media.tablet]: '1fr',
    },
  },
  studyDashboardMain: {
    gap: '1rem',
    display: 'grid',
    minWidth: 0,
  },
  emptyIcon: {
    color: colors.textSecondary,
    fontSize: '1.5rem',
  },
});

function NextActionOrEmpty({
  primary,
  fallback,
  onAddDates,
}: {
  primary: StudyAction | undefined;
  fallback: boolean;
  onAddDates: () => void;
}) {
  if (primary) return <PrimaryAction action={primary} fallback={fallback} />;
  return (
    <div {...stylex.props(styles.studyEmptyCard)}>
      <span {...stylex.props(styles.emptyIcon)}>
        <Icon name="hourglass-split" aria-hidden="true" />
      </span>
      <h2>No next action is ready</h2>
      <p>Add exam dates or refresh courses to build a plan.</p>
      <div {...stylex.props(styles.studyEmptyActions)}>
        <RunCheckButton />
        <button type="button" {...stylex.props(buttonStyles.base, buttonStyles.secondary)} onClick={onAddDates}>
          Add exam dates
        </button>
      </div>
    </div>
  );
}

function useStudyPlan(courseId: string | null, initialData: StudyPayload | null) {
  const [data, setData] = React.useState<StudyPayload | null>(initialData);
  const [error, setError] = React.useState<string | null>(null);
  const loadStudy = React.useCallback(() => {
    setError(null);
    const query = courseId ? `?course_id=${encodeURIComponent(courseId)}` : '';
    return fetchJson<StudyPayload>(`api/v1/study${query}`)
      .then(setData)
      .catch((err) => {
        const parsed = errorLikeSchema.safeParse(err);
        const errorLike = parsed.success ? parsed.data : { message: String(err) };
        setError(errorMessage(errorLike, 'Network connection issue'));
      });
  }, [courseId]);
  React.useEffect(() => {
    // The route loader already loaded the study plan into initialData; only fetch when
    // the route rendered without it (plain client navigation).
    if (initialData) return;
    setData(null);
    loadStudy();
  }, [initialData, loadStudy]);
  return { data, error, loadStudy };
}

/**
 * Route loader contract: fetch the same study payload the client fetches. Pass the raw
 * query string (e.g. 'course_id=5'); the course-scoped plan is derived from
 * it, matching what the page reads from the URL client-side.
 */
export interface StudyPageProps {
  initialData?: StudyPayload | null;
}

function StudyPageError({ error, onRetry }: { error: string; onRetry: () => void }) {
  return (
    <div {...stylex.props(styles.studyDecisionPage)}>
      <div {...stylex.props(styles.studyEmptyCard)} role="alert">
        <h2>Could not load this study plan</h2>
        <p>{error}</p>
        <button type="button" {...stylex.props(buttonStyles.base, buttonStyles.primary)} onClick={onRetry}>
          Try again
        </button>
        <a {...stylex.props(buttonStyles.base, buttonStyles.secondary)} href="/study">
          Open all courses
        </a>
      </div>
    </div>
  );
}

export default function StudyPage({ initialData = null }: StudyPageProps) {
  const [plannerOpen, setPlannerOpen] = React.useState(false);
  const courseId = isServer() ? null : new URLSearchParams(window.location.search).get('course_id');
  const { data, error, loadStudy } = useStudyPlan(courseId, initialData);
  const reloadStudy = React.useCallback(() => {
    void loadStudy();
  }, [loadStudy]);
  if (error) return <StudyPageError error={error} onRetry={loadStudy} />;
  if (!data)
    return (
      <div {...stylex.props(styles.studyDecisionPage)}>
        <p {...stylex.props(styles.studyLoading)} role="status">
          Opening study planner…
        </p>
      </div>
    );
  const adaptive = data.adaptive_plan?.next_session;
  const focus = data.study_intelligence?.focus_queue?.[0];
  const inbox: StudyInboxItem[] = data.inbox || [];
  const primary = adaptive || focus || inbox[0];
  const fallback = !adaptive;
  const alternatives = adaptive
    ? (data.adaptive_plan?.today_queue || []).filter((item) => item.action_id !== adaptive.action_id)
    : (data.study_intelligence?.focus_queue || inbox).slice(1);
  return (
    <div {...stylex.props(styles.studyDecisionPage)}>
      <h1 {...stylex.props(commonStyles.srOnly)}>Study</h1>
      {data.study_projection_status === 'pending' && (
        <p role="status">The study plan is being prepared. Available course updates are shown below.</p>
      )}
      <div {...stylex.props(styles.studyDashboardGrid)}>
        <div {...stylex.props(styles.studyDashboardMain)}>
          <NextActionOrEmpty primary={primary} fallback={fallback} onAddDates={() => setPlannerOpen(true)} />
          <Alternatives actions={alternatives} fallback={fallback} />
        </div>
        <ExamHorizon rows={data.planner_rows || []} onEdit={() => setPlannerOpen(true)} />
      </div>
      <Planner data={data} onSaved={reloadStudy} open={plannerOpen} onOpenChange={setPlannerOpen} />
    </div>
  );
}
