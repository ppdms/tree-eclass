import * as stylex from '@stylexjs/stylex';
import { CalendarDays, SlidersHorizontal } from 'lucide-react';
import { Button } from '@/components/ui/button';
import type { PlannerRow } from '@/lib/types';
import { daysUntilCalendarDate, formatCalendarDate, formatTimeOfDay, parseTimestamp } from '@/lib/format';
import { media } from '@/styles/constants.stylex';
import { colors, typography } from '@/styles/tokens.stylex';

const styles = stylex.create({
  studyExamCard: {
    borderColor: colors.border,
    borderRadius: '1rem',
    borderStyle: 'solid',
    borderWidth: 1,
    overflow: 'hidden',
    backgroundColor: colors.surface,
    display: 'flex',
    flexDirection: 'column',
    position: {
      default: 'sticky',
      [media.tablet]: 'static',
    },
    top: '5.75rem',
  },
  studySectionHeading: {
    paddingBlock: '0.85rem',
    paddingInline: '1rem',
    alignItems: 'center',
    display: 'flex',
    justifyContent: 'space-between',
    borderBottomColor: colors.border,
    borderBottomStyle: 'solid',
    borderBottomWidth: 1,
    minHeight: '3.5rem',
  },
  studySectionHeadingTitle: {
    gap: '0.5rem',
    alignItems: 'center',
    color: colors.textPrimary,
    display: 'flex',
    fontSize: typography.sizeBase,
    fontWeight: 650,
  },
  studySectionHeadingSvg: {
    height: '0.95rem',
    width: '0.95rem',
  },
  studyExamNext: {
    gap: '1.25rem',
    alignItems: 'center',
    display: 'grid',
    gridTemplateColumns: 'minmax(0, 1fr) auto',
    paddingBottom: '1.25rem',
    paddingLeft: '1rem',
    paddingRight: '1rem',
    paddingTop: '1.15rem',
  },
  studyExamDetails: {
    gap: '0.3rem',
    display: 'grid',
    minWidth: 0,
  },
  studyExamKicker: {
    color: colors.textSecondary,
    fontSize: typography.sizeXs,
    fontWeight: 650,
    letterSpacing: '0.04em',
    textTransform: 'uppercase',
  },
  studyExamNextLink: {
    overflow: 'hidden',
    textDecoration: 'none',
    WebkitBoxOrient: 'vertical',
    WebkitLineClamp: 2,
    color: colors.textPrimary,
    display: '-webkit-box',
    fontWeight: 650,
    lineHeight: 1.3,
  },
  studyExamTime: {
    color: colors.textSecondary,
    fontSize: typography.sizeXs,
  },
  studyExamCountdown: {
    borderColor: colors.success,
    borderRadius: '0.85rem',
    borderStyle: 'solid',
    borderWidth: 1,
    gap: '0.15rem',
    placeContent: 'center',
    alignItems: 'center',
    backgroundColor: colors.surfaceSuccess,
    display: 'grid',
    textAlign: 'center',
    minHeight: '4.25rem',
    width: '4.25rem',
  },
  studyExamCountdownStrong: {
    color: colors.textPrimary,
    fontSize: '1.5rem',
    fontVariantNumeric: 'tabular-nums',
    fontWeight: 700,
    lineHeight: 1,
  },
  studyExamCountdownSpan: {
    color: colors.textSecondary,
    fontSize: typography.sizeXs,
  },
  studyExamList: {
    marginInline: '1rem',
    display: 'flex',
    flexDirection: 'column',
    position: 'relative',
    borderTopColor: colors.border,
    borderTopStyle: 'solid',
    borderTopWidth: 1,
    marginBottom: '1rem',
    marginTop: 0,
    paddingLeft: '0.85rem',
    paddingTop: '0.65rem',
  },
  studyExamTimelineLine: {
    backgroundColor: colors.borderLight,
    position: 'absolute',
    bottom: '0.95rem',
    left: '0.125rem',
    top: '1.6rem',
    width: '1px',
  },
  studyExamRow: {
    gap: '0.5rem',
    textDecoration: 'none',
    alignItems: 'center',
    color: { default: colors.textPrimarySoft, ':hover': colors.textPrimary },
    display: 'grid',
    fontSize: typography.sizeSm,
    gridTemplateColumns: 'minmax(0, 1fr) auto auto',
    position: 'relative',
    minHeight: '2.65rem',
  },
  studyExamDot: {
    borderColor: colors.surface,
    borderRadius: '50%',
    borderStyle: 'solid',
    borderWidth: 2,
    backgroundColor: colors.textSecondary,
    boxSizing: 'content-box',
    position: 'absolute',
    height: '0.45rem',
    left: '-0.96rem',
    width: '0.45rem',
  },
  studyExamName: {
    overflow: 'hidden',
    fontWeight: 600,
    textOverflow: 'ellipsis',
    whiteSpace: 'nowrap',
  },
  studyExamRowDays: {
    color: colors.textSecondary,
    fontFamily: typography.fontMono,
    fontSize: typography.sizeXs,
    fontWeight: 700,
    textAlign: 'right',
    minWidth: '2rem',
  },
  studyExamEmpty: {
    padding: '1rem',
    gap: '0.5rem',
    placeItems: 'center',
    alignItems: 'center',
    color: colors.textSecondary,
    display: 'grid',
    textAlign: 'center',
    minHeight: '14rem',
  },
  studyExamEmptySvg: {
    height: '2rem',
    width: '2rem',
  },
});

interface ExamRow extends PlannerRow {
  date: Date;
  daysLeft: number;
}

function examRows(rows: PlannerRow[]): ExamRow[] {
  const today = new Date();
  return rows
    .filter((row) => row.enabled && row.exam_at)
    .map((row) => {
      const date = parseTimestamp(row.exam_at) ?? new Date(Number.NaN);
      return {
        ...row,
        date,
        daysLeft: daysUntilCalendarDate(date, today),
      };
    })
    .filter((row) => !Number.isNaN(row.date.getTime()) && row.daysLeft >= 0)
    .sort((a, b) => a.date.getTime() - b.date.getTime());
}

function dayLabel(days: number): string {
  if (days === 0) return 'today';
  if (days === 1) return 'day';
  return 'days';
}

function ExamCountdown({ exam }: { exam: ExamRow }) {
  return (
    <div {...stylex.props(styles.studyExamNext)}>
      <div {...stylex.props(styles.studyExamDetails)}>
        <span {...stylex.props(styles.studyExamKicker)}>Next exam</span>
        <a {...stylex.props(styles.studyExamNextLink)} href={`/courses/${exam.course_id}`}>
          {exam.course_name}
        </a>
        <time {...stylex.props(styles.studyExamTime)} dateTime={exam.exam_at}>
          {formatCalendarDate(exam.date)} · {formatTimeOfDay(exam.date)}
        </time>
      </div>
      <div
        {...stylex.props(styles.studyExamCountdown)}
        aria-label={exam.daysLeft > 0 ? `${exam.daysLeft} days until exam` : 'Exam is today'}
      >
        <strong {...stylex.props(styles.studyExamCountdownStrong)}>{exam.daysLeft || 'Now'}</strong>
        {exam.daysLeft > 0 && <span {...stylex.props(styles.studyExamCountdownSpan)}>{dayLabel(exam.daysLeft)}</span>}
      </div>
    </div>
  );
}

function ExamTimelineRow({ exam }: { exam: ExamRow }) {
  return (
    <a {...stylex.props(styles.studyExamRow)} href={`/courses/${exam.course_id}`}>
      <span {...stylex.props(styles.studyExamDot)} aria-hidden="true" />
      <span {...stylex.props(styles.studyExamName)}>{exam.short_name || exam.course_name}</span>
      <time {...stylex.props(styles.studyExamTime)} dateTime={exam.exam_at}>
        {formatCalendarDate(exam.date)}
      </time>
      <strong {...stylex.props(styles.studyExamRowDays)}>{exam.daysLeft}d</strong>
    </a>
  );
}

function EmptyExamHorizon({ onEdit }: { onEdit: () => void }) {
  return (
    <aside {...stylex.props(styles.studyExamCard)}>
      <header {...stylex.props(styles.studySectionHeading)}>
        <span {...stylex.props(styles.studySectionHeadingTitle)}>
          <CalendarDays aria-hidden="true" {...stylex.props(styles.studySectionHeadingSvg)} /> Exams
        </span>
      </header>
      <div {...stylex.props(styles.studyExamEmpty)}>
        <CalendarDays aria-hidden="true" {...stylex.props(styles.studyExamEmptySvg)} />
        <p>No exams planned</p>
        <Button type="button" variant="outline" size="sm" onClick={onEdit}>
          Add dates
        </Button>
      </div>
    </aside>
  );
}

export function ExamHorizon({ rows, onEdit }: { rows: PlannerRow[]; onEdit: () => void }) {
  const exams = examRows(rows);
  const [nextExam, ...laterExams] = exams;
  if (!nextExam) return <EmptyExamHorizon onEdit={onEdit} />;
  return (
    <aside {...stylex.props(styles.studyExamCard)} aria-labelledby="exam-horizon-title">
      <header {...stylex.props(styles.studySectionHeading)}>
        <span id="exam-horizon-title" {...stylex.props(styles.studySectionHeadingTitle)}>
          <CalendarDays aria-hidden="true" {...stylex.props(styles.studySectionHeadingSvg)} /> Exams
        </span>
        <Button
          type="button"
          variant="ghost"
          size="sm"
          onClick={onEdit}
          icon={<SlidersHorizontal aria-hidden="true" />}
        >
          Edit
        </Button>
      </header>
      <ExamCountdown exam={nextExam} />
      {laterExams.length > 0 && (
        <div {...stylex.props(styles.studyExamList)}>
          <div {...stylex.props(styles.studyExamTimelineLine)} aria-hidden="true" />
          {laterExams.map((exam) => (
            <ExamTimelineRow exam={exam} key={exam.course_id} />
          ))}
        </div>
      )}
    </aside>
  );
}
