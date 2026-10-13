import { useState } from 'react';
import { AppLink } from '../app/links';
import { Button, Panel } from '../ui/controls';
import { Checkbox, Field, Select } from '../ui/forms';
import { Text, View } from '../ui/primitives';
import { COMMITMENTS, type CourseDraft } from './plannerModel';
import { styles } from './styles';

function examInputValue(value: string): string {
  // Date-only exams are valid stored commitments but not valid native datetime-local values.
  return /^\d{4}-\d{2}-\d{2}$/.test(value) ? `${value}T00:00` : value;
}

function CourseFields({ course, onChange }: { course: CourseDraft; onChange: (course: CourseDraft) => void }) {
  const id = String(course.row.course_id);
  return (
    <View style={styles.courseGrid}>
      <Field
        id={`short_name_${id}`}
        label="Short name"
        value={course.shortName}
        maxLength={24}
        onChange={(shortName) => onChange({ ...course, shortName })}
      />
      <Field
        id={`exam_at_${id}`}
        label="Exam"
        type="datetime-local"
        value={examInputValue(course.examAt)}
        required={course.enabled}
        onChange={(examAt) => onChange({ ...course, examAt })}
      />
      <Field
        id={`target_grade_${id}`}
        label="Target"
        type="number"
        value={course.targetGrade}
        min={0}
        max={10}
        step={0.1}
        required
        onChange={(targetGrade) => onChange({ ...course, targetGrade })}
      />
      <Select
        id={`commitment_${id}`}
        label="Priority"
        value={course.commitment}
        options={COMMITMENTS}
        onChange={(commitment) => onChange({ ...course, commitment })}
      />
    </View>
  );
}

function CourseNotes({ course, onChange }: { course: CourseDraft; onChange: (course: CourseDraft) => void }) {
  const [open, setOpen] = useState(false);
  const id = String(course.row.course_id);
  return (
    <View style={styles.stack}>
      <View style={styles.actions}>
        <Button
          id={`study-planner-notes-${id}`}
          expanded={open}
          controls={`planning_notes_${id}`}
          onClick={() => setOpen(!open)}
        >
          {open ? 'Hide notes' : 'Notes'}
        </Button>
        {!!course.row.planning_notes && <Text style={styles.count}>Saved</Text>}
      </View>
      {open && (
        <Field
          id={`planning_notes_${id}`}
          label="Planning notes"
          value={course.notes}
          multiline
          rows={3}
          maxLength={2000}
          placeholder="Topics, grade rules, or anything the plan should remember"
          onChange={(notes) => onChange({ ...course, notes })}
        />
      )}
    </View>
  );
}

export function PlannerCourse({ course, onChange }: { course: CourseDraft; onChange: (course: CourseDraft) => void }) {
  const id = String(course.row.course_id);
  return (
    <Panel id={`study-planner-course-${id}`} style={{ opacity: course.enabled ? 1 : 0.58 }}>
      <View style={styles.stack}>
        <View style={{ ...styles.row, mobile: { flexDirection: 'column', alignItems: 'stretch' } }}>
          <Checkbox
            id={`enabled_${id}`}
            label={`Include ${course.row.course_name}`}
            checked={course.enabled}
            style={{ minWidth: 0, maxWidth: '100%' }}
            onChange={(enabled) => onChange({ ...course, enabled })}
          />
          <AppLink id={`study-planner-course-link-${id}`} href={`/courses/${id}`} style={styles.link}>
            {course.row.course_name}
          </AppLink>
        </View>
        <CourseFields course={course} onChange={onChange} />
        <CourseNotes course={course} onChange={onChange} />
      </View>
    </Panel>
  );
}
