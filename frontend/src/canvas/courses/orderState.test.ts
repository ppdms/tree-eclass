import assert from 'node:assert/strict';
import test from 'node:test';
import type { CourseSummary } from '@/lib/types';
import {
  confirmSavedOrder,
  createOrderSession,
  nextCourseSequence,
  refreshOrderSession,
  type CoursesCatalogData,
} from './orderState.ts';

const COURSES: CourseSummary[] = [
  { id: 101, name: 'Αρχικό μάθημα', total_files: 1 },
  { id: 204, name: 'Original course', total_files: 2 },
  { id: 302, name: 'Another course', total_files: 3 },
];

function snapshot(courses = COURSES, requestSequence = nextCourseSequence()): CoursesCatalogData {
  return { courses, recent_materials: [], requestSequence };
}

function refreshedCourses() {
  return COURSES.map((course) => ({ ...course, name: `Updated ${course.id}`, total_files: 10 }));
}

test('a held refresh keeps the active drag preview and refreshes rollback metadata', () => {
  const session = createOrderSession(snapshot());
  const held = snapshot(refreshedCourses());
  session.dragging = true;
  session.ordered = [COURSES[1]!, COURSES[0]!, COURSES[2]!];
  refreshOrderSession(session, held);
  assert.deepEqual(
    session.ordered.map((course) => course.id),
    [204, 101, 302],
  );
  assert.equal(session.dragging, true);
  assert.deepEqual(session.ordered[0], held.courses[1]);
  assert.deepEqual(
    session.saved.map((course) => course.id),
    [101, 204, 302],
  );
  assert.deepEqual(session.saved[0], held.courses[0]);
});

test('a refresh during persistence retains the pending order and metadata after success', () => {
  const session = createOrderSession(snapshot());
  const attempted = [COURSES[1]!, COURSES[0]!, COURSES[2]!];
  session.ordered = attempted;
  session.pending = attempted;
  const held = snapshot(refreshedCourses());
  refreshOrderSession(session, held);
  assert.deepEqual(
    session.pending?.map((course) => course.id),
    [204, 101, 302],
  );
  confirmSavedOrder(session, attempted);
  assert.deepEqual(
    session.saved.map((course) => course.id),
    [204, 101, 302],
  );
  assert.deepEqual(session.saved[0], held.courses[1]);
  assert.equal(session.pending, null);
});

test('a GET begun before save completion cannot undo it; a later GET is authoritative', () => {
  const session = createOrderSession(snapshot());
  const held = snapshot(refreshedCourses());
  confirmSavedOrder(session, [COURSES[1]!, COURSES[0]!, COURSES[2]!]);
  refreshOrderSession(session, held);
  assert.deepEqual(
    session.ordered.map((course) => course.id),
    [204, 101, 302],
  );
  assert.deepEqual(session.ordered[0], held.courses[1]);
  const later = snapshot([...held.courses].reverse());
  refreshOrderSession(session, later);
  assert.deepEqual(
    session.ordered.map((course) => course.id),
    [302, 204, 101],
  );
  assert.deepEqual(session.ordered, later.courses);
});

test('out-of-order GET completion does not regress refreshed metadata', () => {
  const session = createOrderSession(snapshot());
  const older = snapshot();
  const newer = snapshot(refreshedCourses());
  refreshOrderSession(session, newer);
  refreshOrderSession(session, older);
  assert.deepEqual(session.ordered[0], newer.courses[0]);
});

test('incoming membership is reconciled without resetting surviving preview order', () => {
  const session = createOrderSession(snapshot());
  session.dragging = true;
  session.ordered = [COURSES[1]!, COURSES[0]!, COURSES[2]!];
  const replacement: CourseSummary = { id: 405, name: 'New course', total_files: 1 };
  const incoming = snapshot([COURSES[0]!, COURSES[1]!, replacement]);
  refreshOrderSession(session, incoming);
  assert.deepEqual(
    session.ordered.map((course) => course.id),
    [204, 101, 405],
  );
  assert.deepEqual(
    session.saved.map((course) => course.id),
    [101, 204, 405],
  );
});
