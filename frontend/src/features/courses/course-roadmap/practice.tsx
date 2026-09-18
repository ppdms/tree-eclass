import * as stylex from '@stylexjs/stylex';
import { Icon } from '@/components/Icon';
import * as React from 'react';
import { fetchJson } from '@/lib/api';
import { errorLikeSchema, errorMessage } from '@/lib/errors';
import type { CourseSummary } from '@/lib/types';
import type { PracticeView } from '@/features/session/reader/types';
import { colors, effects, layout, spacing, typography } from '@/styles/tokens.stylex';
import { PracticeQuestionCard, type PracticeOutcome, type QueueQuestion } from './practiceCard';

const styles = stylex.create({
  courseRoadmap: {
    gap: '.85rem',
    display: 'grid',
    marginBottom: '1.25rem',
  },
  coursePractice: {
    marginBlock: 0,
    marginInline: 0,
    borderTopColor: colors.border,
    borderTopStyle: 'solid',
    borderTopWidth: 1,
  },
  roadmapHead: {
    padding: '1.25rem',
    borderColor: colors.border,
    borderRadius: layout.radiusLarge,
    borderStyle: 'solid',
    borderWidth: 1,
    gap: '1.25rem',
    alignItems: 'flex-start',
    backgroundColor: colors.surface,
    boxShadow: effects.shadow,
    display: 'flex',
    justifyContent: 'space-between',
  },
  roadmapHeadTitle: {
    margin: 0,
  },
  roadmapHeadSubtitle: {
    margin: 0,
    color: colors.textSecondary,
    fontSize: '0.8125rem',
    marginTop: '.375rem',
  },
  roadmapHeadActions: {
    gap: '.625rem',
    alignItems: 'center',
    display: 'flex',
    flexWrap: 'wrap',
    justifyContent: 'flex-end',
  },
  chip: {
    borderColor: colors.border,
    borderRadius: layout.radiusPill,
    borderStyle: 'solid',
    borderWidth: 1,
    paddingBlock: '.25rem',
    paddingInline: '.55rem',
    backgroundColor: colors.surfaceRaised,
    fontSize: typography.sizeXs,
    fontWeight: 550,
  },
  practiceBody: {
    padding: '1.25rem',
    borderColor: colors.border,
    borderRadius: layout.radiusLarge,
    borderStyle: 'solid',
    borderWidth: 1,
    gap: spacing.md,
    backgroundColor: colors.surface,
    boxShadow: effects.shadow,
    display: 'flex',
    flexDirection: 'column',
  },
  studyEventStatus: {
    marginInline: 0,
    color: colors.danger,
    fontSize: '.75rem',
    marginBlockEnd: 0,
    marginBlockStart: '.375rem',
    minHeight: '1em',
  },
  roadmapMeta: {
    color: colors.textSecondary,
    fontSize: '0.8125rem',
  },
  roadmapDetails: {
    padding: '1.25rem',
    color: colors.textSecondary,
    fontSize: '0.8125rem',
  },
});

function usePracticeView(course: CourseSummary) {
  const [view, setView] = React.useState<PracticeView | null>(null);
  const [error, setError] = React.useState<string | null>(null);
  React.useEffect(() => {
    const controller = new AbortController();
    fetchJson<PracticeView>(`api/study/practice?course_id=${course.id}`, { signal: controller.signal })
      .then((value) => {
        if (!controller.signal.aborted) setView(value);
      })
      .catch((err) => {
        if (controller.signal.aborted) return;
        const parsed = errorLikeSchema.safeParse(err);
        const errorLike = parsed.success ? parsed.data : { message: String(err) };
        setError(errorMessage(errorLike, 'The practice queue could not be loaded.'));
      });
    return () => controller.abort();
  }, [course.id]);
  return { view, setView, error };
}

interface PracticeAttemptBody {
  course_id: string | number;
  question_id: string;
  outcome: PracticeOutcome;
  idempotency_key: string;
  seconds?: number;
  note?: string;
}

async function submitAttempt(
  course: CourseSummary,
  question: QueueQuestion | undefined,
  revealed: boolean,
  note: string,
  outcome: PracticeOutcome,
): Promise<{ practice?: PracticeView }> {
  const payload: PracticeAttemptBody = {
    course_id: course.id,
    question_id: String(question?.question_id || question?.id || ''),
    outcome,
    idempotency_key: `practice:${course.id}:${question?.question_id || question?.id || ''}:${Date.now()}`,
  };
  if (revealed) payload.seconds = 5;
  if (note.trim()) payload.note = note.trim();
  return fetchJson(`api/study/practice/attempt`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
}

function usePracticeSession(view: PracticeView | null, setView: (view: PracticeView) => void, course: CourseSummary) {
  const [index, setIndex] = React.useState(0);
  const [revealed, setRevealed] = React.useState(false);
  const [note, setNote] = React.useState('');
  const [busy, setBusy] = React.useState(false);
  const [status, setStatus] = React.useState('');
  const queue = React.useMemo<QueueQuestion[]>(
    () =>
      (view?.units || []).flatMap((unit) =>
        (unit.questions || []).map((question) => ({ ...question, unit_title: unit.title })),
      ),
    [view],
  );
  const question = queue[index];
  const record = async (outcome: PracticeOutcome) => {
    setBusy(true);
    setStatus('');
    try {
      const result = await submitAttempt(course, question, revealed, note, outcome);
      if (result.practice) setView(result.practice);
      setIndex(0);
      setRevealed(false);
      setNote('');
      setStatus(`Recorded: ${outcome}`);
    } catch (recordError) {
      setStatus(recordError instanceof Error ? recordError.message : 'That attempt could not be recorded.');
    } finally {
      setBusy(false);
    }
  };
  const next = () => {
    setIndex((index + 1) % queue.length);
    setRevealed(false);
    setNote('');
  };
  return { index, revealed, note, busy, status, queue, record, setRevealed, setNote, next };
}

export function PracticeHeader({
  practice,
  totals,
}: {
  practice: { enabled?: boolean; pending_units?: number };
  totals: NonNullable<PracticeView['totals']>;
}) {
  return (
    <header {...stylex.props(styles.roadmapHead)}>
      <div>
        <h2 id="course-practice-title" {...stylex.props(styles.roadmapHeadTitle)}>
          <Icon name="question-circle" aria-hidden="true" /> Active recall
        </h2>
        <p {...stylex.props(styles.roadmapHeadSubtitle)}>
          Answer from memory first, then reveal the expected answer and grade yourself honestly. Every attempt is kept.
        </p>
      </div>
      <PracticeHeaderBadges practice={practice} totals={totals} />
    </header>
  );
}

function PracticeHeaderBadges({
  practice,
  totals,
}: {
  practice: { pending_units?: number };
  totals: NonNullable<PracticeView['totals']>;
}) {
  return (
    <div {...stylex.props(styles.roadmapHeadActions)}>
      <PracticeCounter totals={totals} />
      <PracticePendingBadge pending={practice.pending_units} />
    </div>
  );
}

function PracticeCounter({ totals }: { totals: NonNullable<PracticeView['totals']> }) {
  return (
    <span {...stylex.props(styles.chip)} data-practice-counter>
      {totals.questions ? `${totals.due} due of ${totals.questions}` : 'no questions yet'}
    </span>
  );
}

function PracticePendingBadge({ pending }: { pending?: number }) {
  return pending ? (
    <span {...stylex.props(styles.chip)} title="Questions for these units are still being written.">
      {pending} unit{pending === 1 ? '' : 's'} preparing
    </span>
  ) : null;
}

interface PracticeBodyProps {
  view: PracticeView | null;
  error: string | null;
  notReady: string;
  question: QueueQuestion | undefined;
  index: number;
  queueLength: number;
  totals: NonNullable<PracticeView['totals']>;
  revealed: boolean;
  busy: boolean;
  note: string;
  status: string;
  onReveal: () => void;
  onGrade: (outcome: PracticeOutcome) => void;
  onNote: (value: string) => void;
  onNext: () => void;
}

function PracticeContent(props: PracticeBodyProps) {
  const { error, view, question, notReady } = props;
  if (error)
    return (
      <p {...stylex.props(styles.studyEventStatus)} data-state="error" role="alert">
        {error}
      </p>
    );
  if (!view)
    return (
      <p {...stylex.props(styles.roadmapMeta)} data-practice-placeholder>
        Opening the practice queue…
      </p>
    );
  if (!question) return <p {...stylex.props(styles.roadmapMeta)}>{notReady}</p>;
  return <PracticeQuestionCard {...props} question={question} />;
}

export function PracticeBody(props: PracticeBodyProps) {
  return (
    <div {...stylex.props(styles.practiceBody)} data-practice-body>
      <PracticeContent {...props} />
    </div>
  );
}

export function PracticePanel({
  course,
  practice,
}: {
  course: CourseSummary;
  practice: { enabled?: boolean; pending_units?: number };
}) {
  const { view, setView, error } = usePracticeView(course);
  const session = usePracticeSession(view, setView, course);
  const question = session.queue[session.index];
  const totals = view?.totals || {};
  const notReady = session.queue.length
    ? 'Questions for this course are still being written. They appear here automatically.'
    : 'No practice questions are available for the current roadmap revision yet.';
  return (
    <section
      {...stylex.props(styles.courseRoadmap, styles.coursePractice)}
      aria-labelledby="course-practice-title"
      data-practice-panel
      data-course-id={course.id}
    >
      <PracticeHeader practice={practice} totals={totals} />
      <PracticeBody
        view={view}
        error={error}
        notReady={notReady}
        question={question}
        index={session.index}
        queueLength={session.queue.length}
        totals={totals}
        revealed={session.revealed}
        busy={session.busy}
        note={session.note}
        status={session.status}
        onReveal={() => session.setRevealed(true)}
        onGrade={session.record}
        onNote={session.setNote}
        onNext={session.next}
      />
      <PracticeDisclaimer />
    </section>
  );
}

function PracticeDisclaimer() {
  return (
    <footer {...stylex.props(styles.roadmapDetails)}>
      Questions are AI-written from the evidence each roadmap unit cites, and are replaced whenever that roadmap
      revision changes. Your attempt history is kept separately and survives every regeneration. Check the linked
      material before trusting an answer.
    </footer>
  );
}
