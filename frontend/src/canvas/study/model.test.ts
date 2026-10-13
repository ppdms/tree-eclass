import assert from 'node:assert/strict';
import test from 'node:test';
import type { StudyAction, StudyPayload } from '@/lib/types';
import { alternativeHref, evidenceHref, fallbackHref, studyPath, studySelection } from './model.ts';
import { outcomeBody } from './outcomeModel.ts';
import { createPlannerDraft, plannerErrorMessages, plannerPayload, validatePlanner } from './plannerModel.ts';

const ACTION: StudyAction = {
  course_id: 101,
  action_id: 'read:NUL\0/source',
  course_name: 'Course',
  blueprint_revision_id: 'revision-1',
  plan_revision: 'old-revision',
  minutes: 25,
  source_path: '/Courses/101/eclass/Δένδρα.pdf',
};
const DATA: StudyPayload = {
  planner_rows: [
    { course_id: 101, course_name: 'Visible', enabled: true, exam_at: '2026-11-15T10:30', target_grade: 8.5 },
    { course_id: 102, course_name: 'Hidden enabled commitment', enabled: true, exam_at: '2026-11-16T11:00' },
    { course_id: 103, course_name: 'Not included', enabled: false, planning_notes: 'Keep this note' },
  ],
  planner_settings: { weekly_minutes: { '0': 90 }, blackout_dates: ['2026-11-01'] },
};

test('adaptive selection wins, filters the primary semantic action, and retains full alternatives', () => {
  const focus = { ...ACTION, action_id: 'focus' };
  const adaptive = { ...ACTION, action_id: 'adaptive' };
  const next = { ...ACTION, action_id: 'next' };
  const result = studySelection({
    adaptive_plan: { next_session: adaptive, today_queue: [adaptive, next] },
    study_intelligence: { focus_queue: [focus] },
    inbox: [{ course_id: 102, file_path: '/inbox' }],
  });
  assert.equal(result.primary, adaptive);
  assert.equal(result.fallback, false);
  assert.deepEqual(result.alternatives, [next]);
});

test('focus/inbox precedence keeps an empty focus queue from becoming inbox alternatives', () => {
  const inbox = [
    { course_id: 101, file_path: '/one', study_level: 3 as const },
    { course_id: 102, file_path: '/two' },
  ];
  const result = studySelection({ study_intelligence: { focus_queue: [] }, inbox });
  assert.equal(result.primary?.level, 3);
  assert.equal(result.primary?.file_path, '/one');
  assert.equal(result.fallback, true);
  assert.deepEqual(result.alternatives, []);
  assert.equal(studySelection({ inbox }).alternatives.length, 1);
  const focused = studySelection({ study_intelligence: { focus_queue: [ACTION] }, inbox });
  assert.equal(focused.primary, ACTION);
});

test('source and reader links preserve identity, source bytes, and display-only page semantics', () => {
  assert.equal(studyPath('101'), '/api/v1/study?course_id=101');
  assert.equal(studyPath(null), '/api/v1/study');
  const fallback = { course_id: 101, file_path: '/Courses/101/eclass/name with space.pdf' };
  assert.equal(fallbackHref(fallback), '/files/Courses/101/eclass/name%20with%20space.pdf');
  assert.equal(alternativeHref(fallback, true), fallbackHref(fallback));
  const reader = evidenceHref(ACTION, { renderable: true, document_id: 'doc:NUL\0', page_number: 14 });
  assert.ok(reader);
  const params = new URL(reader, 'https://example.invalid').searchParams;
  assert.equal(params.get('action_id'), ACTION.action_id);
  assert.equal(params.get('document_id'), 'doc:NUL\0');
  assert.equal(params.has('page'), false);
  assert.equal(evidenceHref(ACTION, { label: 'Unavailable' }), null);
  assert.equal(evidenceHref(ACTION, { url: 'https://example.invalid/original' }), 'https://example.invalid/original');
});

test('planner serialization is complete across tabs, omissions, hidden courses, and raw datetime values', () => {
  const draft = createPlannerDraft(DATA);
  draft.courses[0]!.notes = 'New notes\0';
  draft.courses[0]!.commitment = 'must_pass';
  draft.courses[1]!.shortName = 'Κρυφό';
  draft.weekly[4] = '120';
  const payload = plannerPayload(draft);
  assert.equal(payload.enabled_101, 'on');
  assert.equal(payload.enabled_102, 'on');
  assert.equal(Object.hasOwn(payload, 'enabled_103'), false);
  assert.equal(payload.exam_at_101, '2026-11-15T10:30');
  assert.equal(payload.short_name_102, 'Κρυφό');
  assert.equal(payload.planning_notes_101, 'New notes\0');
  assert.equal(payload.planning_notes_103, 'Keep this note');
  assert.equal(payload.commitment_101, 'must_pass');
  assert.equal(payload.weekly_minutes_4, '120');
  assert.equal(payload.block_minutes, '45');
  assert.equal(payload.max_courses_per_day, '3');
  assert.ok(Object.values(payload).every((value) => value === String(value)));
  assert.deepEqual(validatePlanner(draft), []);
});

test('planner validation rejects malformed dates and ranges atomically, with deduplicated issues', () => {
  const draft = createPlannerDraft(DATA);
  draft.weekly[0] = '1.5';
  draft.blockMinutes = '14';
  draft.maxCourses = '8';
  draft.blackoutDates = '2026-02-30,not-a-date';
  draft.courses[0]!.examAt = '';
  draft.courses[1]!.examAt = '';
  draft.courses[0]!.targetGrade = 'NaN';
  draft.courses[1]!.notes = 'x'.repeat(2001);
  const errors = validatePlanner(draft);
  assert.ok(errors.some((message) => message.startsWith('Monday minutes')));
  assert.ok(errors.some((message) => message.startsWith('Session length')));
  assert.ok(errors.some((message) => message.startsWith('Maximum courses per day')));
  assert.equal(errors.filter((message) => message === 'Enabled courses need an exam date.').length, 1);
  assert.ok(errors.includes('Target grades must be between 0 and 10.'));
  assert.ok(errors.includes('Planning notes must be at most 2000 characters.'));
  assert.ok(errors.some((message) => message.includes('2026-02-30')));
});

test('planner clears notes and omissions without discarding unincluded course fields or server issues', () => {
  const draft = createPlannerDraft(DATA);
  draft.courses[1]!.enabled = false;
  draft.courses[2]!.notes = '';
  const payload = plannerPayload(draft);
  assert.equal(Object.hasOwn(payload, 'enabled_102'), false);
  assert.equal(payload.exam_at_102, '2026-11-16T11:00');
  assert.equal(payload.planning_notes_103, '');
  assert.deepEqual(plannerErrorMessages({ detail: ['First', { msg: 'Second' }] }, 422), ['First', 'Second']);
  assert.deepEqual(plannerErrorMessages({ detail: 'Conflict' }, 409), ['Conflict']);
});

test('study outcomes preserve the dashboard revision, optional fields, and partial-minutes semantics', () => {
  const result = outcomeBody(ACTION, 'completed', { minutes: '25', confidence: '', note: '  Learned\0  ' });
  assert.ok('body' in result);
  assert.deepEqual(result.body, {
    course_id: 101,
    action_id: ACTION.action_id,
    event_type: 'completed',
    plan_revision: 'revision-1',
    actual_minutes: 25,
    note: 'Learned\0',
  });
  const partial = outcomeBody(ACTION, 'partial', { minutes: '', confidence: '3', note: '' });
  assert.deepEqual(partial, { error: 'Partial progress requires a positive minutes value.' });
  const deferred = outcomeBody(ACTION, 'deferred', { minutes: '', confidence: '5', note: '' });
  assert.ok('body' in deferred);
  assert.equal(deferred.body.confidence, 5);
  assert.equal(Object.hasOwn(deferred.body, 'actual_minutes'), false);
});
