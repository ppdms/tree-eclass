import assert from 'node:assert/strict';
import test from 'node:test';
import type { CourseDetailPayload } from '@/lib/types';
import {
  actionUnitTitle,
  courseStudyHref,
  headerProgress,
  overviewPercent,
  shortInstruction,
  tabFromHash,
} from './model';

const overview: CourseDetailPayload = {
  course: { id: 42, name: 'Course' },
  course_blueprint: { progress: { completed_actions: 2, total_actions: 3, percent: 140.4 } },
};

test('all five source hashes map directly and unknown hashes stay on overview', () => {
  for (const tab of ['overview', 'roadmap', 'files', 'activity', 'settings']) {
    assert.equal(tabFromHash(`#${tab}`), tab);
  }
  for (const hash of ['', '#', '#unknown', '#updates', '#Files', '#files/']) {
    assert.equal(tabFromHash(hash), 'overview');
  }
});

test('header completion ratio stays independent from backend overview percentage', () => {
  assert.deepEqual(headerProgress(overview), { completed: 2, total: 3, percent: 67 });
  assert.equal(overviewPercent(overview), 140);
  assert.deepEqual(headerProgress({ course: overview.course }), { completed: 0, total: 0, percent: 0 });
  assert.equal(overviewPercent({ course: overview.course }), 0);
});

test('both course study destinations name the scoped dashboard, never the reader', () => {
  assert.equal(courseStudyHref(42), '/study?course_id=42');
  assert.equal(courseStudyHref('001'), '/study?course_id=001');
});

test('up-next instruction preserves source covering and punctuation trimming', () => {
  assert.equal(shortInstruction(undefined), 'Choose your next study action');
  assert.equal(shortInstruction('Read lecture notes covering recurrence relations.'), 'Read lecture notes');
  assert.equal(shortInstruction('Practice exercises:'), 'Practice exercises');
  assert.equal(shortInstruction('Finish this!'), 'Finish this!');
});

test('up-next unit association uses semantic action identity and explicit title wins', () => {
  const units = [{ key: 'recurrence', title: 'Recurrences', actions: [{ action_id: 'solve:recurrence' }] }];
  assert.equal(actionUnitTitle(units, { action_id: 'solve:recurrence' }), 'Recurrences');
  assert.equal(actionUnitTitle(units, { action_id: 'solve:recurrence', unit_title: 'Next unit' }), 'Next unit');
  assert.equal(actionUnitTitle(units, { action_id: 'read:another' }), undefined);
  assert.equal(actionUnitTitle(units), undefined);
});
