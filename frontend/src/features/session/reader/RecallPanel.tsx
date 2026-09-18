import * as stylex from '@stylexjs/stylex';
/**
 * Active recall for the unit being studied, inside the workspace.
 *
 * Reading a page and then answering a question about it in the same place is
 * the whole reason the workspace exists: the attempt lands as evidence next to
 * the reading that produced it, rather than in a separate flashcard app that
 * knows nothing about either.
 *
 * Grading stays self-graded here, as phase one shipped it. Answering never
 * completes or un-completes a scheduled action — practice informs the plan, it
 * does not overrule the learner.
 */

import { useMemo, useState } from 'react';

import { Alert, AlertDescription } from '@/components/ui/alert';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Card } from '@/components/ui/card';
import { Textarea } from '@/components/ui/textarea';
import { commonStyles } from '@/styles/common';
import { colors, spacing, typography } from '@/styles/tokens.stylex';
import type { PracticeQuestion, PracticeView } from '@/features/session/reader/types';

import { api, requestKey } from './api';

const OUTCOMES: [string, string][] = [
  ['correct', 'Got it'],
  ['partial', 'Partly'],
  ['incorrect', 'Not yet'],
  ['skipped', 'Skip'],
];

type RecallOutcome = (typeof OUTCOMES)[number][0];

interface RecallQuestion extends PracticeQuestion {
  unit_title?: string;
}

const styles = stylex.create({
  container: {
    padding: '0.75rem',
    display: 'flex',
    flexDirection: 'column',
    height: '100%',
  },
  centerHint: {
    padding: '1rem',
    gap: '0.5rem',
    alignItems: 'center',
    color: colors.textSecondary,
    display: 'flex',
    flexDirection: 'column',
    fontSize: '0.75rem',
    justifyContent: 'center',
    lineHeight: typography.leadingRelaxed,
    textAlign: 'center',
    height: '100%',
  },
  code: {
    borderRadius: '0.25rem',
    marginInline: '0.25rem',
    paddingInline: '0.25rem',
    backgroundColor: colors.surface,
  },
  link: {
    textDecoration: 'underline',
    color: colors.textPrimary,
    textUnderlineOffset: '2px',
  },
  queueHeader: {
    gap: '0.5rem',
    alignItems: 'center',
    color: colors.textSecondary,
    display: 'flex',
    fontSize: '0.68rem',
    justifyContent: 'space-between',
    letterSpacing: typography.trackingWide,
    textTransform: 'uppercase',
    marginBottom: spacing.unit,
  },
  unitTitle: {
    overflow: 'hidden',
    textOverflow: 'ellipsis',
    whiteSpace: 'nowrap',
  },
  queueBadge: {
    paddingBlock: 0,
    paddingInline: '0.375rem',
    flexShrink: 0,
    fontSize: '0.62rem',
    fontWeight: typography.weightNormal,
    textTransform: 'none',
  },
  card: {
    padding: '0.75rem',
    fontSize: '0.75rem',
    lineHeight: typography.leadingRelaxed,
    marginBottom: '0.75rem',
  },
  answerText: {
    marginBottom: '0.5rem',
  },
  promptCard: {
    padding: '0.75rem',
    marginBottom: '0.75rem',
  },
  promptText: {
    margin: 0,
    fontSize: '0.85rem',
    fontWeight: 500,
    lineHeight: typography.leadingRelaxed,
  },
  pointsList: {
    margin: 0,
    gap: '0.25rem',
    color: colors.textPrimarySoft,
    display: 'flex',
    flexDirection: 'column',
    listStyleType: 'disc',
    paddingLeft: '1rem',
  },
  answerBtn: {
    alignSelf: 'flex-start',
    marginBottom: '0.75rem',
  },
  outcomesGrid: {
    gap: spacing.unit,
    display: 'grid',
    gridTemplateColumns: 'repeat(2, minmax(0, 1fr))',
  },
  outcomeBtn: {
    width: '100%',
  },
  errorAlert: {
    paddingBlock: '0.5rem',
    paddingInline: '0.75rem',
    fontSize: '0.75rem',
    marginTop: '0.5rem',
  },
  textarea: {
    fontSize: '0.75rem',
    marginBottom: '0.5rem',
  },
  progress: {
    margin: 0,
    color: colors.textSecondary,
    fontSize: '0.75rem',
    marginBottom: '0.5rem',
  },
});

function NoPracticeHint({ courseId }: { courseId: string | number }) {
  return (
    <div {...stylex.props(styles.centerHint)}>
      <p>No practice questions exist for this unit yet.</p>
      <p>
        Question generation is a separate worker lane. It is switched off until{' '}
        <code {...stylex.props(styles.code)}>KNOWLEDGE_AI_PRACTICE_ENABLED</code> is turned on.
      </p>
      {courseId && (
        <a {...stylex.props(styles.link)} href={`/courses/${courseId}#roadmap`}>
          Open the course roadmap
        </a>
      )}
    </div>
  );
}

function NothingDue({ courseId }: { courseId: string | number }) {
  return (
    <div {...stylex.props(styles.centerHint)}>
      <p>Nothing due for this unit right now.</p>
      {courseId && (
        <a {...stylex.props(styles.link)} href={`/courses/${courseId}#roadmap`}>
          Review the course roadmap
        </a>
      )}
    </div>
  );
}

function QueueHeader({
  unitKey,
  unitTitle,
  queueLength,
}: {
  unitKey?: string;
  unitTitle?: string;
  queueLength: number;
}) {
  return (
    <p {...stylex.props(styles.queueHeader)}>
      <span {...stylex.props(styles.unitTitle)}>{unitTitle || unitKey || 'Recall'}</span>
      <Badge variant="outline" {...stylex.props(styles.queueBadge)}>
        {queueLength} in queue
      </Badge>
    </p>
  );
}

function RevealedAnswer({ expectedAnswer, answerPoints }: { expectedAnswer?: string; answerPoints?: string[] }) {
  return (
    <Card style={styles.card}>
      <p {...stylex.props(styles.answerText)}>{expectedAnswer}</p>
      {answerPoints?.length ? (
        <ul {...stylex.props(styles.pointsList)}>
          {answerPoints.map((point, index) => (
            <li key={index}>{point}</li>
          ))}
        </ul>
      ) : null}
    </Card>
  );
}

function AnswerReveal({
  revealed,
  expectedAnswer,
  answerPoints,
  onReveal,
}: {
  revealed: boolean;
  expectedAnswer?: string;
  answerPoints?: string[];
  onReveal: () => void;
}) {
  if (revealed) {
    return <RevealedAnswer expectedAnswer={expectedAnswer} answerPoints={answerPoints} />;
  }
  return (
    <Button type="button" variant="outline" size="sm" style={styles.answerBtn} onClick={onReveal}>
      Show the answer
    </Button>
  );
}

function OutcomeButtons({ busy, onRecord }: { busy: boolean; onRecord: (outcome: RecallOutcome) => void }) {
  return (
    <div {...stylex.props(styles.outcomesGrid)}>
      {OUTCOMES.map(([value, label]) => (
        <Button
          key={value}
          type="button"
          variant="outline"
          size="sm"
          style={styles.outcomeBtn}
          disabled={busy}
          onClick={() => onRecord(value)}
        >
          {label}
        </Button>
      ))}
    </div>
  );
}

function ErrorAlert({ message }: { message: string }) {
  return (
    <Alert variant="destructive" {...stylex.props(styles.errorAlert)}>
      <AlertDescription>{message}</AlertDescription>
    </Alert>
  );
}

function useAttemptRecorder(
  courseId: string | number,
  question: RecallQuestion | undefined,
  answer: string,
  onRecorded?: (practice: PracticeView) => void,
  onResetAttempt?: () => void,
) {
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [startedAt, setStartedAt] = useState(() => Date.now());

  const record = async (outcome: RecallOutcome) => {
    if (!question) return;
    setBusy(true);
    setError(null);
    try {
      const result = await api.practiceAttempt({
        course_id: courseId,
        question_id: question.question_id || question.id || '',
        outcome,
        confidence: null,
        seconds: Math.max(0, Math.round((Date.now() - startedAt) / 1000)),
        answer: answer.trim() || null,
        idempotency_key: requestKey('recall'),
      });
      onRecorded?.(result.practice);
      onResetAttempt?.();
      setStartedAt(Date.now());
    } catch (attemptError) {
      setError(attemptError instanceof Error ? attemptError.message : 'That attempt could not be recorded.');
    } finally {
      setBusy(false);
    }
  };

  return { record, busy, error };
}

function useRecallQueue(practice: PracticeView | null): RecallQuestion[] {
  return useMemo(
    () =>
      (practice?.units || [])
        .flatMap((unit) => (unit.questions || []).map((item) => ({ ...item, unit_title: unit.title })))
        .sort((a, b) => (a.queue_rank ?? 0) - (b.queue_rank ?? 0)),
    [practice],
  );
}

function QuestionCard({ prompt }: { prompt?: string }) {
  return (
    <Card style={styles.promptCard}>
      <p {...stylex.props(styles.promptText)}>{prompt}</p>
    </Card>
  );
}

function AnswerField({ answer, onChange }: { answer: string; onChange: (value: string) => void }) {
  return (
    <>
      <label {...stylex.props(commonStyles.srOnly)} htmlFor="recall-answer">
        Your recall answer
      </label>
      <Textarea
        id="recall-answer"
        value={answer}
        onChange={(event) => onChange(event.target.value)}
        rows={4}
        placeholder="Answer from memory first…"
        {...stylex.props(styles.textarea)}
      />
    </>
  );
}

export interface RecallPanelProps {
  courseId: string | number;
  practice: PracticeView | null;
  unitKey?: string;
  onRecorded?: (practice: PracticeView) => void;
}

export default function RecallPanel({ courseId, practice, unitKey, onRecorded }: RecallPanelProps) {
  const [revealed, setRevealed] = useState(false);
  const [answer, setAnswer] = useState('');
  const queue = useRecallQueue(practice);
  const question = queue[0];
  const { record, busy, error } = useAttemptRecorder(courseId, question, answer, onRecorded, () => {
    setRevealed(false);
    setAnswer('');
  });
  if (!practice?.enabled && !queue.length) return <NoPracticeHint courseId={courseId} />;
  if (!question) return <NothingDue courseId={courseId} />;
  return (
    <div {...stylex.props(styles.container)}>
      <QueueHeader unitKey={unitKey} unitTitle={question.unit_title} queueLength={queue.length} />
      <p {...stylex.props(styles.progress)} role="status">
        {queue.length} question{queue.length === 1 ? '' : 's'} waiting
      </p>
      <QuestionCard prompt={question.prompt} />
      <AnswerField answer={answer} onChange={setAnswer} />
      <AnswerReveal
        revealed={revealed}
        expectedAnswer={question.expected_answer}
        answerPoints={question.answer_points}
        onReveal={() => setRevealed(true)}
      />
      {revealed ? <OutcomeButtons busy={busy} onRecord={record} /> : null}
      {error ? <ErrorAlert message={error} /> : null}
    </div>
  );
}
