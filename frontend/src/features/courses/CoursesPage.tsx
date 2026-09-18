import * as stylex from '@stylexjs/stylex';
import type { CoursesCoverage } from '@/lib/types';
import { CourseHeader, CourseShelfSection } from './coursesParts';

const styles = stylex.create({
  coursesPage: {
    marginInline: 'auto',
    maxWidth: '73.75rem',
    minWidth: 0,
    width: '100%',
  },
});

export default function CoursesPage({ initialData = null }: { initialData?: CoursesCoverage | null }) {
  const courses = initialData?.courses;
  return (
    <div {...stylex.props(styles.coursesPage)}>
      <CourseHeader />
      <CourseShelfSection courses={courses} error={null} />
    </div>
  );
}
