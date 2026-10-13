import assert from 'node:assert/strict';
import test from 'node:test';
import { isCheckAcknowledged, runningCheckStatus } from './checkState';

test('only success or a verified busy conflict acknowledges an admitted check', () => {
  assert.equal(isCheckAcknowledged(true, 200, { message: 'Check started in background' }), true);
  assert.equal(isCheckAcknowledged(false, 409, { is_checking: true }), true);
  assert.equal(isCheckAcknowledged(false, 409, {}), false);
  assert.equal(isCheckAcknowledged(false, 409, { is_checking: false }), false);
  assert.equal(isCheckAcknowledged(false, 503, { is_checking: true }), false);
  assert.equal(isCheckAcknowledged(false, 409, { code: 'runtime_changed', is_checking: true }), false);
});

test('admission retains prior metadata without presenting the previous completed run as current', () => {
  const previous = {
    is_checking: false,
    course_name: 'Previous course',
    last_check_at: '2026-10-01T08:00:00Z',
    last_check_result: 'success',
    last_files_added: 5,
    last_error: 'Previous failure',
  };
  const running = runningCheckStatus(previous, {});
  assert.equal(running.is_checking, true);
  assert.equal(running.last_check_at, previous.last_check_at);
  assert.equal(running.last_files_added, 5);
  assert.equal(running.course_name, null);
  assert.equal(running.last_error, null);
  assert.equal(previous.is_checking, false);
  assert.equal(runningCheckStatus(null, { current_course: 'Algorithms' }).course_name, 'Algorithms');
});
