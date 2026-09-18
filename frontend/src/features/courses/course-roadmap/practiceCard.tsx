import * as stylex from '@stylexjs/stylex';
import { Icon } from '@/components/Icon';
import * as React from 'react';
import { buttonStyles } from '@/components/ui/styles';
import { lookup } from '@/lib/display';
import type { PracticeQuestion, PracticeView } from '@/features/session/reader/types';
import { media } from '@/styles/constants.stylex';
import { colors, layout, spacing, typography } from '@/styles/tokens.stylex';

const styles = stylex.create({
  practiceCard: {
    padding: {
      default: spacing.lg,
      [media.mobile]: spacing.md,
    },
    borderColor: colors.border,
    borderRadius: layout.radiusLarge,
    borderStyle: 'solid',
    borderWidth: 1,
    gap: spacing.sm,
    backgroundColor: colors.surface,
    display: 'flex',
    flexDirection: 'column',
  },
  roadmapEyebrow: {
    color: colors.success,
    fontSize: typography.sizeXs,
    fontWeight: 750,
    letterSpacing: '.09em',
    textTransform: 'uppercase',
  },
  practicePrompt: {
    margin: 0,
    fontSize: typography.sizeLg,
    lineHeight: 1.45,
  },
  roadmapMeta: {
    color: colors.textSecondary,
    fontSize: '0.8125rem',
  },
  practiceAnswer: {
    padding: spacing.md,
    borderColor: colors.border,
    borderRadius: layout.radius,
    borderStyle: 'solid',
    borderWidth: 1,
    backgroundColor: colors.surfaceRaised,
    width: '100%',
  },
  practiceAnswerText: {
    margin: 0,
    lineHeight: 1.5,
    whiteSpace: 'pre-wrap',
    marginBottom: spacing.sm,
  },
  practiceAnswerLabel: {
    marginInline: 0,
    fontSize: typography.sizeSm,
    fontWeight: typography.weightSemibold,
    letterSpacing: '.04em',
    opacity: 0.72,
    textTransform: 'uppercase',
    marginBottom: spacing.unit,
    marginTop: spacing.sm,
  },
  practicePoints: {
    margin: 0,
    paddingInlineStart: '1.25rem',
  },
  practiceMistakes: {
    color: colors.danger,
  },
  pointItem: {
    marginBottom: '0.1875rem',
  },
  roadmapEventButtons: {
    gap: '.375rem',
    display: 'flex',
    flexWrap: 'wrap',
  },
  practiceGradeOption: {
    borderColor: colors.border,
    borderRadius: 6,
    borderStyle: 'solid',
    borderWidth: 1,
    paddingBlock: 6,
    paddingInline: 10,
    backgroundColor: { default: colors.surfaceRaised, ':hover': colors.surfaceHover },
    color: colors.textPrimary,
    cursor: 'pointer',
  },
  practiceGradeOptionDone: {
    borderColor: colors.success,
    backgroundColor: colors.surfaceSuccess,
    color: colors.success,
  },
  practiceGrade: {
    gap: spacing.sm,
    display: 'flex',
    flexDirection: 'column',
  },
  practiceNote: {
    gap: '.5rem',
    alignItems: 'center',
    display: 'flex',
    fontSize: '0.88rem',
  },
  practiceNoteInput: {
    flexBasis: '0%',
    flexGrow: 1,
    flexShrink: 1,
    minWidth: 0,
  },

  studyEventStatus: {
    marginInline: 0,
    color: colors.success,
    fontSize: '.75rem',
    marginBlockEnd: 0,
    marginBlockStart: '.375rem',
    minHeight: '1em',
  },
  practiceNav: {
    gap: spacing.md,
    alignItems: 'center',
    display: 'flex',
    flexWrap: 'wrap',
  },
  practiceReveal: {
    gap: spacing.sm,
    alignItems: 'flex-start',
    display: 'flex',
    flexDirection: 'column',
  },
});

export type PracticeOutcome = 'correct' | 'partial' | 'incorrect' | 'skipped';

const GRADE_OPTIONS: [PracticeOutcome, string, string][] = [
  ['correct', 'check2-circle', 'Got it'],
  ['partial', 'pie-chart', 'Partly'],
  ['incorrect', 'arrow-counterclockwise', 'Not yet'],
  ['skipped', 'skip-forward', 'Skip'],
];

export interface QueueQuestion extends PracticeQuestion {
  unit_title?: string;
}

const STATE_LABELS = {
  needs_work: 'Needs work',
  unattempted: 'New',
  review: 'One more pass',
  learned: 'Learned',
} satisfies Record<string, string>;

export function QuestionMeta({ question }: { question: QueueQuestion }) {
  return (
    <>
      <span {...stylex.props(styles.roadmapEyebrow)}>
        {question.unit_title || question.unit_key} · {lookup(STATE_LABELS, question.state || '', question.state || '')}
      </span>
      <h3 {...stylex.props(styles.practicePrompt)}>{question.prompt}</h3>
      <p {...stylex.props(styles.roadmapMeta)}>
        {[
          String(question.response_mode || '').replaceAll('_', ' '),
          question.difficulty,
          question.estimated_minutes ? `~${question.estimated_minutes} min` : null,
          question.attempts ? `${question.attempts} attempt${question.attempts === 1 ? '' : 's'}` : 'never attempted',
        ]
          .filter(Boolean)
          .join(' · ')}
      </p>
    </>
  );
}

export function AnswerContent({ question }: { question: QueueQuestion }) {
  return (
    <div {...stylex.props(styles.practiceAnswer)}>
      <p {...stylex.props(styles.practiceAnswerText)}>{question.expected_answer}</p>
      {question.answer_points?.length ? (
        <AnswerPointList title="Your answer should contain:" items={question.answer_points} />
      ) : null}
      {question.common_mistakes?.length ? (
        <AnswerPointList title="Common mistakes:" items={question.common_mistakes} mistakes />
      ) : null}
    </div>
  );
}

function AnswerPointList({ title, items, mistakes = false }: { title: string; items: string[]; mistakes?: boolean }) {
  return (
    <>
      <p {...stylex.props(styles.practiceAnswerLabel)}>{title}</p>
      <ul {...stylex.props(styles.practicePoints, mistakes && styles.practiceMistakes)}>
        {items.map((item) => (
          <li key={item} {...stylex.props(styles.pointItem)}>
            {item}
          </li>
        ))}
      </ul>
    </>
  );
}

function GradeButtons({ busy, onGrade }: { busy: boolean; onGrade: (outcome: PracticeOutcome) => void }) {
  return (
    <div {...stylex.props(styles.roadmapEventButtons)} role="group" aria-label="Grade your answer">
      {GRADE_OPTIONS.map(([value, icon, label]) => (
        <button
          type="button"
          {...stylex.props(styles.practiceGradeOption, value === 'correct' && styles.practiceGradeOptionDone)}
          key={value}
          disabled={busy}
          onClick={() => onGrade(value)}
        >
          <Icon name={icon} aria-hidden="true" /> {label}
        </button>
      ))}
    </div>
  );
}

export function GradeControls({
  busy,
  note,
  onNote,
  onGrade,
}: {
  busy: boolean;
  note: string;
  onNote: (value: string) => void;
  onGrade: (outcome: PracticeOutcome) => void;
}) {
  return (
    <div {...stylex.props(styles.practiceGrade)}>
      <p {...stylex.props(styles.practiceAnswerLabel)}>How did you do?</p>
      <GradeButtons busy={busy} onGrade={onGrade} />
      <label {...stylex.props(styles.practiceNote)}>
        Note{' '}
        <input
          type="text"
          value={note}
          onChange={(event) => onNote(event.target.value)}
          placeholder="What did you miss?"
          {...stylex.props(styles.practiceNoteInput)}
        />
      </label>
    </div>
  );
}

export function AnswerReveal({
  question,
  revealed,
  onReveal,
  busy,
  note,
  onNote,
  onGrade,
}: {
  question: QueueQuestion;
  revealed: boolean;
  onReveal: () => void;
  busy: boolean;
  note: string;
  onNote: (value: string) => void;
  onGrade: (outcome: PracticeOutcome) => void;
}) {
  if (!revealed)
    return (
      <button type="button" {...stylex.props(buttonStyles.base, buttonStyles.secondary)} onClick={onReveal}>
        <Icon name="eye" aria-hidden="true" /> Show answer
      </button>
    );
  return (
    <>
      <AnswerContent question={question} />
      <GradeControls busy={busy} note={note} onNote={onNote} onGrade={onGrade} />
    </>
  );
}

export function PracticeStatus({ status }: { status: string }) {
  return (
    <p
      {...stylex.props(styles.studyEventStatus)}
      data-practice-status
      role="status"
      aria-live="polite"
      data-state={status.startsWith('Recorded') ? 'success' : status ? 'error' : ''}
    >
      {status}
    </p>
  );
}

export function PracticeNav({
  index,
  queueLength,
  onNext,
}: {
  index: number;
  queueLength: number;
  onNext: () => void;
}) {
  return (
    <div {...stylex.props(styles.practiceNav)}>
      <button type="button" {...stylex.props(buttonStyles.base, buttonStyles.secondary)} onClick={onNext}>
        <Icon name="arrow-right" aria-hidden="true" /> Next question
      </button>
      <span {...stylex.props(styles.roadmapMeta)}>
        {index + 1} of {queueLength} in the queue
      </span>
    </div>
  );
}

export interface PracticeQuestionCardProps {
  question: QueueQuestion;
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

export function PracticeQuestionCard(props: PracticeQuestionCardProps) {
  const { question, index, queueLength, totals, revealed, busy, note, status, onReveal, onGrade, onNote, onNext } =
    props;
  return (
    <article {...stylex.props(styles.practiceCard)} data-question-id={question.question_id}>
      <QuestionMeta question={question} />
      <div {...stylex.props(styles.practiceReveal)}>
        <AnswerReveal
          question={question}
          revealed={revealed}
          onReveal={onReveal}
          busy={busy}
          note={note}
          onNote={onNote}
          onGrade={onGrade}
        />
      </div>
      <PracticeStatus status={status} />
      <p {...stylex.props(styles.roadmapMeta)}>
        {totals.due} due · {totals.learned} learned · {totals.questions} total
      </p>
      <PracticeNav index={index} queueLength={queueLength} onNext={onNext} />
    </article>
  );
}
