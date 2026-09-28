import { navigate } from '@/app/navigation';
import * as stylex from '@stylexjs/stylex';
import { Icon } from '@/components/Icon';
import * as React from 'react';
import { buttonStyles } from '@/components/ui/styles';
import type { CourseSummary } from '@/lib/types';
import { media } from '@/styles/constants.stylex';
import { colors, typography } from '@/styles/tokens.stylex';
import { folderLabel } from './addCourse';

const styles = stylex.create({
  hiddenCourseRow: {
    gap: '1rem',
    alignItems: {
      default: 'center',
      [media.tablet]: 'flex-start',
    },
    display: 'flex',
    flexDirection: {
      default: 'row',
      [media.tablet]: 'column',
    },
    justifyContent: 'space-between',
  },
  folderSub: {
    color: colors.textSecondary,
    display: 'block',
    fontSize: '0.75rem',
  },
  settingsFormError: {
    color: colors.danger,
    fontSize: typography.sizeSm,
    marginTop: '0.75rem',
  },
});

export interface HiddenCourseRowProps {
  course: CourseSummary;
}

function HiddenCourseAction({ course, busy, onShow }: { course: CourseSummary; busy: boolean; onShow: () => void }) {
  return (
    <button
      {...stylex.props(buttonStyles.base, buttonStyles.secondary)}
      type="button"
      onClick={onShow}
      disabled={busy}
      aria-label={`Show ${course.name}`}
    >
      {busy ? (
        'Restoring…'
      ) : (
        <>
          <Icon name="eye" aria-hidden="true" /> Show
        </>
      )}
    </button>
  );
}

export function HiddenCourseRow({ course }: HiddenCourseRowProps) {
  const [state, setState] = React.useState<'busy' | string | null>(null);
  const show = async () => {
    setState('busy');
    try {
      const response = await fetch(`/api/v1/courses/${course.id}/show`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
        body: '{}',
      });
      if (!response.ok) throw new Error('Could not restore this course.');
      navigate('/settings#hidden-courses');
    } catch (error) {
      setState(error instanceof Error ? error.message : 'Could not restore this course.');
    }
  };
  return (
    <div {...stylex.props(styles.hiddenCourseRow)}>
      <span lang="el">
        {course.name}
        <small title={course.webdav_folder} {...stylex.props(styles.folderSub)}>
          {folderLabel(course.webdav_folder)}
        </small>
      </span>
      <HiddenCourseAction course={course} busy={state === 'busy'} onShow={show} />
      {state && state !== 'busy' && (
        <span {...stylex.props(styles.settingsFormError)} role="alert">
          {state}
        </span>
      )}
    </div>
  );
}
