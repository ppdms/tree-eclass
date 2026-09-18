import * as stylex from '@stylexjs/stylex';
import * as React from 'react';
import { ChevronDown } from 'lucide-react';
import { Input } from '@/components/ui/input';
import { Textarea } from '@/components/ui/textarea';
import { Selector } from '@astryxdesign/core/Selector';
import type { PlannerRow } from '@/lib/types';
import { media } from '@/styles/constants.stylex';
import { colors, layout, typography } from '@/styles/tokens.stylex';

const styles = stylex.create({
  studyPlanCourse: {
    padding: '1rem',
    borderColor: colors.border,
    borderRadius: layout.radiusLarge,
    borderStyle: 'solid',
    borderWidth: 1,
    backgroundColor: colors.surface,
    display: 'flex',
    flexDirection: 'column',
  },
  studyPlanCourseDisabled: {
    opacity: 0.58,
  },
  courseHeader: {
    gap: '0.7rem',
    alignItems: 'center',
    display: 'flex',
    marginBottom: '0.9rem',
  },
  courseTitleLink: {
    textDecoration: 'none',
    color: colors.textPrimary,
    fontSize: typography.sizeBase,
    fontWeight: 650,
  },
  studyCourseToggle: {
    alignItems: 'center',
    cursor: 'pointer',
    display: 'inline-flex',
    position: 'relative',
  },
  toggleInput: {
    opacity: 0,
    position: 'absolute',
  },
  toggleTrack: {
    padding: '0.125rem',
    borderColor: colors.borderLight,
    borderRadius: layout.radiusPill,
    borderStyle: 'solid',
    borderWidth: 1,
    alignItems: 'center',
    backgroundColor: colors.surfaceRaised,
    cursor: 'pointer',
    display: 'inline-flex',
    transitionDuration: '150ms',
    transitionProperty: 'background-color, border-color',
    height: '1.125rem',
    width: '2rem',
  },
  toggleTrackChecked: {
    borderColor: colors.success,
    backgroundColor: colors.surfaceSuccess,
  },
  toggleThumb: {
    borderRadius: '50%',
    backgroundColor: colors.textSecondary,
    display: 'block',
    transitionDuration: '150ms',
    transitionProperty: 'transform, background-color',
    height: '0.75rem',
    width: '0.75rem',
  },
  toggleThumbChecked: {
    backgroundColor: colors.success,
    transform: 'translateX(0.875rem)',
  },
  studyPlanCourseFields: {
    gap: '0.65rem',
    display: 'grid',
    gridTemplateColumns: {
      default: '0.75fr 1.6fr 0.6fr 1fr',
      [media.tablet]: '1fr 1fr',
      [media.narrow]: '1fr',
    },
  },
  fieldLabel: {
    gap: '0.35rem',
    color: colors.textSecondary,
    display: 'grid',
    fontSize: typography.sizeXs,
  },
  studyPlanDateField: {
    gridColumn: {
      default: 'auto',
      [media.tablet]: '1 / -1',
      [media.narrow]: 'auto',
    },
  },
  studyPlanNotes: {
    marginTop: '0.65rem',
  },
  notesSummary: {
    gap: '0.35rem',
    listStyle: 'none',
    alignItems: 'center',
    color: colors.textSecondary,
    cursor: 'pointer',
    display: 'flex',
    fontSize: typography.sizeXs,
    width: 'max-content',
  },
  savedBadge: {
    borderRadius: layout.radiusPill,
    paddingBlock: '0.1rem',
    paddingInline: '0.35rem',
    backgroundColor: colors.surfaceRaised,
  },
  notesSvg: {
    height: '0.8rem',
    width: '0.8rem',
  },
  notesTextarea: {
    marginTop: '0.55rem',
  },
});

function CourseFields({ row }: { row: PlannerRow }) {
  const id = String(row.course_id);
  const [commitment, setCommitment] = React.useState(row.commitment || 'conditional');
  return (
    <div {...stylex.props(styles.studyPlanCourseFields)}>
      <label {...stylex.props(styles.fieldLabel)}>
        <span>Short name</span>
        <Input name={`short_name_${id}`} defaultValue={row.short_name || ''} maxLength={24} />
      </label>
      <label {...stylex.props(styles.fieldLabel, styles.studyPlanDateField)}>
        <span>Exam</span>
        <Input
          id={`exam_at_${id}`}
          type="datetime-local"
          name={`exam_at_${id}`}
          defaultValue={row.exam_at || ''}
          required={row.enabled}
        />
      </label>
      <label {...stylex.props(styles.fieldLabel)}>
        <span>Target</span>
        <Input
          type="number"
          name={`target_grade_${id}`}
          min="0"
          max="10"
          step="0.1"
          defaultValue={row.target_grade ?? 5}
        />
      </label>
      <label {...stylex.props(styles.fieldLabel)}>
        <span>Priority</span>
        <Selector
          label="Priority"
          isLabelHidden
          options={[
            { value: 'must_pass', label: 'Must pass' },
            { value: 'committed', label: 'Committed' },
            { value: 'conditional', label: 'Conditional' },
            { value: 'skipped', label: 'Skipped' },
          ]}
          value={commitment}
          onChange={setCommitment}
          htmlName={`commitment_${id}`}
        />
      </label>
    </div>
  );
}

function CourseNotes({ row }: { row: PlannerRow }) {
  const id = String(row.course_id);
  return (
    <details {...stylex.props(styles.studyPlanNotes)}>
      <summary {...stylex.props(styles.notesSummary)}>
        Notes {row.planning_notes ? <span {...stylex.props(styles.savedBadge)}>Saved</span> : null}{' '}
        <ChevronDown aria-hidden="true" {...stylex.props(styles.notesSvg)} />
      </summary>
      <Textarea
        name={`planning_notes_${id}`}
        maxLength={2000}
        defaultValue={row.planning_notes || ''}
        placeholder="Topics, grade rules, or anything the plan should remember"
        style={styles.notesTextarea}
        rows={3}
      />
    </details>
  );
}

export function PlannerCourseCard({ row }: { row: PlannerRow }) {
  const [enabled, setEnabled] = React.useState(row.enabled);
  const id = String(row.course_id);
  return (
    <article
      {...stylex.props(styles.studyPlanCourse, !enabled && styles.studyPlanCourseDisabled)}
      data-enabled={enabled}
    >
      <header {...stylex.props(styles.courseHeader)}>
        <label {...stylex.props(styles.studyCourseToggle)}>
          <input
            type="checkbox"
            name={`enabled_${id}`}
            checked={enabled}
            aria-label={`Include ${row.course_name}`}
            {...stylex.props(styles.toggleInput)}
            onChange={(event) => {
              setEnabled(event.target.checked);
              // SAFETY: this id belongs to the datetime input rendered by
              // CourseFields for the same planner row.
              const exam = document.getElementById(`exam_at_${id}`) as HTMLInputElement | null;
              if (exam) exam.required = event.target.checked;
            }}
          />
          <span aria-hidden="true" {...stylex.props(styles.toggleTrack, enabled && styles.toggleTrackChecked)}>
            <span {...stylex.props(styles.toggleThumb, enabled && styles.toggleThumbChecked)} />
          </span>
        </label>
        <a {...stylex.props(styles.courseTitleLink)} href={`/courses/${row.course_id}`}>
          {row.course_name}
        </a>
      </header>
      <CourseFields row={row} />
      <CourseNotes row={row} />
    </article>
  );
}
