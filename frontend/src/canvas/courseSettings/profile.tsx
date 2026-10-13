import { useState } from 'react';
import type { CourseSummary } from '@/lib/types';
import { AppLink } from '../app/links';
import { Button } from '../ui/controls';
import { Field } from '../ui/forms';
import { Text, View } from '../ui/primitives';
import { styles } from './styles';

function CourseSourceDetails({ course }: { course: CourseSummary }) {
  const [open, setOpen] = useState(false);
  return (
    <View style={styles.source}>
      <Button
        id={`course-${course.id}-source-toggle`}
        expanded={open}
        controls={`course-${course.id}-source-details`}
        onClick={() => setOpen(!open)}
      >
        {open ? '▾ Source details' : '▸ Source details'}
      </Button>
      {open && (
        <View id={`course-${course.id}-source-details`} style={styles.copy}>
          <View style={styles.sourceRow}>
            <Text style={styles.label}>Course ID</Text>
            <Text style={styles.value}>{course.id}</Text>
          </View>
          <View style={styles.sourceRow}>
            <Text style={styles.label}>Folder</Text>
            <Text style={styles.folder}>{course.webdav_folder || ''}</Text>
          </View>
          <View style={styles.sourceRow}>
            <Text style={styles.label}>eClass</Text>
            <AppLink
              id={`course-${course.id}-eclass-source`}
              href={`https://eclass.aueb.gr/modules/document/index.php?course=INF${course.id}`}
              target="_blank"
              rel="noopener noreferrer"
              style={styles.link}
            >
              Open course ↗
            </AppLink>
          </View>
        </View>
      )}
    </View>
  );
}

export function CourseProfile({
  course,
  busy,
  onRename,
}: {
  course: CourseSummary;
  busy: boolean;
  onRename: (name: string) => void;
}) {
  const [name, setName] = useState(course.name);
  return (
    <View style={{ ...styles.card, ...styles.profile }}>
      <View style={styles.copy}>
        <Text style={styles.title}>Course name</Text>
        <Text style={styles.description}>Used throughout your workspace</Text>
      </View>
      <Field
        id={`course-${course.id}-name`}
        label="Course name"
        required
        value={name}
        onChange={setName}
        onSubmit={() => onRename(name)}
      />
      <Button id={`course-${course.id}-rename`} disabled={busy} onClick={() => onRename(name)}>
        {busy ? 'Saving…' : 'Save'}
      </Button>
      <CourseSourceDetails course={course} />
    </View>
  );
}
