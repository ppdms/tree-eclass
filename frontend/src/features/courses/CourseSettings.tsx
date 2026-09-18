import * as stylex from '@stylexjs/stylex';
import * as React from 'react';
import { Settings2 } from 'lucide-react';
import type { CourseSummary } from '@/lib/types';
import { media } from '@/styles/constants.stylex';
import { colors, layout, typography } from '@/styles/tokens.stylex';
import { useCourseActions } from './courseSettingsActions';
import {
  CourseRenameForm,
  CourseActionButtons,
  CourseDangerButtons,
  CourseSettingsConfirm,
} from './courseSettingsSections';

const styles = stylex.create({
  courseSettings: {
    padding: '1rem',
    borderColor: colors.border,
    borderRadius: layout.radiusLarge,
    borderStyle: 'solid',
    borderWidth: 1,
    backgroundColor: colors.surface,
  },
  coursePanelHeader: {
    gap: '1rem',
    paddingInline: 0,
    alignItems: 'center',
    display: 'flex',
    justifyContent: 'space-between',
    paddingBlockEnd: '.75rem',
    paddingBlockStart: 0,
  },
  coursePanelHeading: {
    gap: '.1rem',
    display: 'grid',
  },
  coursePanelCategory: {
    color: colors.textSecondary,
    fontSize: typography.sizeXs,
    fontWeight: 650,
    letterSpacing: '.07em',
    textTransform: 'uppercase',
  },
  coursePanelTitle: {
    margin: 0,
    gap: '.5rem',
    alignItems: 'center',
    display: 'flex',
    fontSize: '1.15rem',
    letterSpacing: '-0.025em',
    marginBottom: '1rem',
  },
  courseSettingsGrid: {
    gap: '.85rem',
    display: 'grid',
    gridTemplateColumns: {
      default: 'repeat(2, minmax(0, 1fr))',
      [media.tablet]: '1fr',
    },
  },
  courseSettingsError: {
    marginInline: 0,
    color: colors.danger,
    fontSize: '.75rem',
    marginBlockEnd: 0,
    marginBlockStart: '.75rem',
  },
});

export default function CourseSettings({ course }: { course: CourseSummary }) {
  const { busy, feedback, confirmation, rename, runAction, askAction, cancelConfirmation } = useCourseActions(course);
  return (
    <section {...stylex.props(styles.courseSettings)} aria-labelledby="course-settings-title">
      <header {...stylex.props(styles.coursePanelHeader)}>
        <div {...stylex.props(styles.coursePanelHeading)}>
          <span {...stylex.props(styles.coursePanelCategory)}>Preferences</span>
          <h2 id="course-settings-title" {...stylex.props(styles.coursePanelTitle)}>
            <Settings2 /> Course settings
          </h2>
        </div>
      </header>
      <div {...stylex.props(styles.courseSettingsGrid)}>
        <CourseRenameForm course={course} busy={busy} onSubmit={rename} />
        <CourseActionButtons course={course} busy={busy} onAction={askAction} />
        <CourseDangerButtons course={course} busy={busy} onAction={askAction} />
      </div>
      {feedback && (
        <p {...stylex.props(styles.courseSettingsError)} role="alert">
          {feedback.message}
        </p>
      )}
      <CourseSettingsConfirm confirmation={confirmation} onCancel={cancelConfirmation} onConfirm={runAction} />
    </section>
  );
}
