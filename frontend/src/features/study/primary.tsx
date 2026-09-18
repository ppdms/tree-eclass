import * as stylex from '@stylexjs/stylex';
import { Icon } from '@/components/Icon';
import * as React from 'react';
import { Clock3, Compass, Flag } from 'lucide-react';
import { buttonStyles } from '@/components/ui/styles';
import type { StudyAction } from '@/lib/types';
import { Button } from '@/components/ui/button';
import { media } from '@/styles/constants.stylex';
import { colors, typography } from '@/styles/tokens.stylex';
import StudyLevel from './StudyLevel';
import { Evidence } from './evidence';
import { RecordOutcome } from './outcome';

const styles = stylex.create({
  studyFocusCard: {
    borderColor: colors.border,
    borderRadius: '1rem',
    borderStyle: 'solid',
    borderWidth: 1,
    backgroundColor: colors.surface,
    display: 'flex',
    flexDirection: 'column',
    paddingBottom: 'clamp(1.15rem, 2.5vw, 1.6rem)',
    paddingLeft: 'clamp(1.15rem, 2.5vw, 1.6rem)',
    paddingRight: 'clamp(1.15rem, 2.5vw, 1.6rem)',
    paddingTop: 'clamp(1.15rem, 2.5vw, 1.6rem)',
  },
  studyFocusHeading: {
    color: colors.textPrimary,
    fontSize: 'clamp(1.25rem, 2vw, 1.6rem)',
    fontWeight: 650,
    letterSpacing: '-0.035em',
    lineHeight: 1.22,
    marginBottom: '1.2rem',
    marginLeft: 0,
    marginRight: 0,
    marginTop: '1rem',
    maxWidth: '34ch',
  },
  studyFocusTopline: {
    gap: {
      default: '1rem',
      [media.narrow]: '0.6rem',
    },
    alignItems: {
      default: 'center',
      [media.narrow]: 'flex-start',
    },
    display: 'flex',
    flexDirection: {
      default: 'row',
      [media.narrow]: 'column',
    },
    flexWrap: 'wrap',
    justifyContent: 'space-between',
  },
  studyFocusCourse: {
    alignItems: 'center',
    color: colors.textSecondary,
    display: 'flex',
    flexWrap: 'wrap',
    fontSize: typography.sizeSm,
  },
  studyCourseChip: {
    textDecoration: 'none',
    color: colors.textPrimarySoft,
    fontWeight: 650,
  },
  studyFocusMeta: {
    gap: '0.8rem',
    alignItems: 'center',
    color: colors.textSecondary,
    display: 'flex',
    flexWrap: 'wrap',
    justifyContent: {
      default: 'flex-end',
      [media.narrow]: 'flex-start',
    },
  },
  studyFocusMetaItem: {
    gap: '0.4rem',
    alignItems: 'center',
    color: colors.textSecondary,
    display: 'inline-flex',
    fontSize: typography.sizeXs,
    fontWeight: 550,
  },
  studyFocusMetaSvg: {
    height: '0.95rem',
    width: '0.95rem',
  },
  studyFocusFooter: {
    gap: '0.5rem',
    alignItems: 'center',
    display: 'flex',
    flexWrap: 'wrap',
    justifyContent: 'space-between',
    borderTopColor: colors.border,
    borderTopStyle: 'solid',
    borderTopWidth: 1,
    paddingTop: '1rem',
  },
  studyPrimaryActions: {
    margin: 0,
    gap: '0.5rem',
    alignItems: 'center',
    display: 'flex',
    flexWrap: 'wrap',
  },
  studyStatusControl: {
    gap: '0.35rem',
    alignItems: 'center',
    color: colors.textSecondary,
    display: 'inline-flex',
    fontSize: typography.sizeSm,
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
  successIcon: {
    color: colors.success,
    fontSize: '1.5rem',
  },
});

function ActionContext({ action, fallback }: { action: StudyAction; fallback: boolean }) {
  return (
    <div {...stylex.props(styles.studyFocusMeta)}>
      {(action.minutes || action.reading_minutes) && (
        <span {...stylex.props(styles.studyFocusMetaItem)}>
          <Clock3 aria-hidden="true" {...stylex.props(styles.studyFocusMetaSvg)} />{' '}
          {action.minutes || action.reading_minutes} min
        </span>
      )}
      {action.days_to_exam != null && (
        <span {...stylex.props(styles.studyFocusMetaItem)}>
          <Flag aria-hidden="true" {...stylex.props(styles.studyFocusMetaSvg)} /> {action.days_to_exam} days to exam
        </span>
      )}
      {fallback && (
        <span {...stylex.props(styles.studyFocusMetaItem)}>
          <Compass aria-hidden="true" {...stylex.props(styles.studyFocusMetaSvg)} /> File suggestion
        </span>
      )}
    </div>
  );
}

function FallbackActions({ action }: { action: StudyAction }) {
  return (
    <div {...stylex.props(styles.studyPrimaryActions)}>
      <Button
        href={action.redirect_url || (action.source_path ? `/files${encodeURI(action.source_path)}` : '#')}
        target="_blank"
        rel="noopener"
      >
        Open file
      </Button>
      <Button variant="outline" href={`/courses/${action.course_id}`}>
        Course
      </Button>
      <span {...stylex.props(styles.studyStatusControl)}>
        Current level{' '}
        <StudyLevel
          courseId={action.course_id}
          filePath={action.source_path || action.file_path}
          initial={action.level}
        />
      </span>
    </div>
  );
}

function PrimaryRecordedState() {
  return (
    <div {...stylex.props(styles.studyEmptyCard)} role="status">
      <span {...stylex.props(styles.successIcon)}>
        <Icon name="check-circle" aria-hidden="true" />
      </span>
      <h2>Progress recorded</h2>
      <p>Refresh to open the next session.</p>
      <button
        type="button"
        {...stylex.props(buttonStyles.base, buttonStyles.primary)}
        onClick={() => window.location.reload()}
      >
        Open next session
      </button>
    </div>
  );
}

function PrimaryCourseInfo({ action }: { action: StudyAction }) {
  return (
    <div {...stylex.props(styles.studyFocusCourse)}>
      <a {...stylex.props(styles.studyCourseChip)} href={`/courses/${action.course_id}`}>
        {action.course_name}
      </a>
    </div>
  );
}

function conciseInstruction(action: StudyAction): string {
  const instruction = action.instruction || action.recommended_action || 'Open the next course source.';
  return instruction.split(/\s+covering\s+/i)[0] || instruction;
}

export function PrimaryAction({ action, fallback = false }: { action: StudyAction; fallback?: boolean }) {
  const [recorded, setRecorded] = React.useState(false);
  if (recorded) return <PrimaryRecordedState />;
  return (
    <article {...stylex.props(styles.studyFocusCard)} data-study-action>
      <div {...stylex.props(styles.studyFocusTopline)}>
        <PrimaryCourseInfo action={action} />
        <ActionContext action={action} fallback={fallback} />
      </div>
      <h2 {...stylex.props(styles.studyFocusHeading)}>{conciseInstruction(action)}</h2>
      <div {...stylex.props(styles.studyFocusFooter)}>
        {fallback ? (
          <FallbackActions action={action} />
        ) : (
          <RecordOutcome action={action} onRecorded={() => setRecorded(true)} />
        )}
        <Evidence action={action} />
      </div>
    </article>
  );
}
