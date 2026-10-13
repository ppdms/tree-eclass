import { useMemo, useState } from 'react';
import type { CourseSummary } from '@/lib/types';
import { AppLink } from '../app/links';
import { Text, TextField, View } from '../ui/primitives';
import { CourseCard } from './card';
import { CoverageLegend } from './coverage';
import type { CoursesCatalogData } from './orderState';
import { styles } from './styles';
import { useCourseOrder, type CourseOrder } from './useCourseOrder';

const SAVE_MESSAGES = {
  idle: '',
  saving: 'Saving course order…',
  saved: 'Course order saved',
  error: 'Could not save the new course order. The previous order was restored.',
};

interface CourseGridProps {
  courses: CourseSummary[];
  order: CourseOrder;
  draggingId: string | null;
  setDraggingId: (id: string | null) => void;
}

function CourseGrid({ courses, order, draggingId, setDraggingId }: CourseGridProps) {
  const saving = order.saveState === 'saving';
  const finish = (cancelled: boolean) => {
    if (cancelled) order.cancel();
    else order.commit();
    setDraggingId(null);
  };
  return (
    <View id="course-list" style={styles.grid}>
      {courses.map((course, index) => (
        <CourseCard
          key={course.id}
          course={course}
          position={index + 1}
          total={courses.length}
          dragging={draggingId === String(course.id)}
          saving={saving}
          onDragStart={() => {
            order.beginDrag();
            setDraggingId(String(course.id));
          }}
          onDragOver={(sourceId) => {
            if (!saving) order.previewMove(sourceId, String(course.id));
          }}
          onDragEnd={finish}
          onMove={(offset) => {
            const target = courses[index + offset];
            if (!target || saving || draggingId) return;
            order.previewMove(String(course.id), String(target.id));
            order.commit();
          }}
        />
      ))}
    </View>
  );
}

function CourseShelf({ data }: { data: CoursesCatalogData }) {
  const order = useCourseOrder(data);
  const [search, setSearch] = useState('');
  const [draggingId, setDraggingId] = useState<string | null>(null);
  const visibleCourses = useMemo(() => {
    const query = search.trim().toLowerCase();
    if (!query) return order.ordered;
    return order.ordered.filter(
      (course) =>
        (course.name || '').toLowerCase().includes(query) || (course.code || '').toLowerCase().includes(query),
    );
  }, [order.ordered, search]);
  const changeSearch = (value: string) => {
    if (draggingId) {
      order.cancel();
      setDraggingId(null);
    }
    setSearch(value);
  };
  if (!order.ordered.length) return <EmptyCourses />;
  return (
    <View style={{ gap: 14, minWidth: 0 }}>
      <View style={styles.controls}>
        <TextField
          id="courses-search"
          value={search}
          onChange={changeSearch}
          placeholder="Filter courses…"
          style={styles.search}
        />
        {SAVE_MESSAGES[order.saveState] && (
          <Text
            id="courses-order-status"
            style={{ ...styles.feedback, ...(order.saveState === 'error' && styles.failure) }}
          >
            {SAVE_MESSAGES[order.saveState]}
          </Text>
        )}
      </View>
      {visibleCourses.length ? (
        <CourseGrid courses={visibleCourses} order={order} draggingId={draggingId} setDraggingId={setDraggingId} />
      ) : (
        <Text id="courses-no-results" style={styles.description}>
          No courses match “{search}”.
        </Text>
      )}
    </View>
  );
}

function EmptyCourses() {
  return (
    <View id="courses-empty" style={styles.state}>
      <Text style={styles.stateTitle}>No courses yet</Text>
      <Text style={styles.description}>Add a course in Settings to start a source-backed workspace.</Text>
      <AppLink id="courses-add-course" href="/settings#add-course" style={styles.action}>
        Add course
      </AppLink>
    </View>
  );
}

export function CoursesPage({ data }: { data: CoursesCatalogData }) {
  return (
    <View id="courses-content" ready style={styles.page}>
      <View style={styles.intro}>
        <Text style={styles.title}>Courses</Text>
        <Text style={styles.description}>
          Filter by course name or code. Drag cards to reorder, use arrow keys, or choose Move earlier / Move later.
        </Text>
        <Text style={styles.description}>Escape cancels a drag. Changes save automatically.</Text>
      </View>
      <CoverageLegend />
      <CourseShelf data={data} />
    </View>
  );
}
