import * as stylex from '@stylexjs/stylex';
import { buttonStyles } from '@/components/ui/styles';
import type { CourseSummary } from '@/lib/types';
import { commonStyles } from '@/styles/common';
import { media } from '@/styles/constants.stylex';
import { colors, layout, typography } from '@/styles/tokens.stylex';
import { CourseShelf } from './CourseShelf';

const styles = stylex.create({
  courseShelfSection: {
    marginBlock: 0,
    marginInline: 0,
    paddingBlock: 0,
    paddingInline: 0,
  },
  courseCardGrid: {
    gap: {
      default: '1rem',
      [media.tabletOnly]: '1.125rem',
    },
    listStyle: 'none',
    marginBlock: 0,
    marginInline: 0,
    paddingBlock: 0,
    paddingInline: 0,
    display: 'grid',
    gridTemplateColumns: {
      default: 'repeat(3, minmax(0, 1fr))',
      [media.tabletOnly]: 'repeat(2, minmax(0, 1fr))',
      [media.mobile]: 'minmax(0, 1fr)',
    },
  },
  courseCardGridLoading: {
    opacity: 0.6,
  },
  courseGridState: {
    padding: '1.75rem',
    borderColor: colors.border,
    borderRadius: layout.radiusLarge,
    borderStyle: 'solid',
    borderWidth: 1,
    marginBlock: '2rem',
    marginInline: 'auto',
    backgroundColor: colors.surface,
    textAlign: 'center',
    maxWidth: '34rem',
  },
  courseGridStateTitle: {
    fontSize: typography.sizeXl,
    marginBlockEnd: '0.625rem',
    marginBlockStart: 0,
  },
  courseGridStateDesc: {
    color: colors.textSecondary,
    marginBlockEnd: '1.125rem',
    marginBlockStart: 0,
  },
});

export function CourseHeader() {
  return <h1 {...stylex.props(commonStyles.srOnly)}>Courses</h1>;
}

export { CourseShelf } from './CourseShelf';

export function CourseShelfSection({ courses, error }: { courses: CourseSummary[] | undefined; error: string | null }) {
  return (
    <section {...stylex.props(styles.courseShelfSection)} aria-label="Active courses">
      {error ? (
        <div {...stylex.props(styles.courseGridState)} role="alert">
          <h2 {...stylex.props(styles.courseGridStateTitle)}>Courses are unavailable</h2>
          <p {...stylex.props(styles.courseGridStateDesc)}>{error}</p>
          <a href="/courses" {...stylex.props(buttonStyles.base, buttonStyles.secondary)}>
            Try again
          </a>
        </div>
      ) : courses === undefined ? (
        <div
          {...stylex.props(styles.courseCardGrid, styles.courseCardGridLoading)}
          role="status"
          aria-label="Opening courses"
        >
          {[0, 1, 2, 3].map((card) => (
            <span key={card} />
          ))}
        </div>
      ) : courses.length ? (
        <CourseShelf courses={courses} />
      ) : (
        <div {...stylex.props(styles.courseGridState)}>
          <h2 {...stylex.props(styles.courseGridStateTitle)}>No courses yet</h2>
          <p {...stylex.props(styles.courseGridStateDesc)}>
            Add a course in Settings to start a source-backed workspace.
          </p>
          <a href="/settings#add-course" {...stylex.props(buttonStyles.base, buttonStyles.primary)}>
            Add course
          </a>
        </div>
      )}
    </section>
  );
}
