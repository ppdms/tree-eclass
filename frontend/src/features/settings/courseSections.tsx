import * as stylex from '@stylexjs/stylex';
import * as React from 'react';
import type { SettingsPayload } from './types';
import { Section } from './settingsForm';
import { AddCourseForm } from './addCourse';
import { HiddenCourseRow } from './hiddenCourses';

const styles = stylex.create({
  hiddenCourseList: {
    gap: '0.6rem',
    display: 'flex',
    flexDirection: 'column',
  },
});

export interface CourseSectionsProps {
  data: SettingsPayload;
  dirtySections: Set<string>;
}

export function CourseSections({ data, dirtySections }: CourseSectionsProps) {
  return (
    <>
      <Section
        id="add-course"
        title="Add course"
        status="New download"
        dirty={dirtySections.has('add-course')}
        description="Course checks download its files into local document storage."
      >
        <AddCourseForm data={data} />
      </Section>
      <Section
        id="hidden-courses"
        title="Hidden courses"
        dirty={dirtySections.has('hidden-courses')}
        description={
          data.hidden_courses?.length
            ? 'Hidden courses remain synchronized but stay out of the daily workspace.'
            : 'No hidden courses.'
        }
      >
        {data.hidden_courses && data.hidden_courses.length > 0 && (
          <div {...stylex.props(styles.hiddenCourseList)}>
            {data.hidden_courses.map((course) => (
              <HiddenCourseRow course={course} key={String(course.id)} />
            ))}
          </div>
        )}
      </Section>
    </>
  );
}
