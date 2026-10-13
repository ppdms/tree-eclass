import assert from 'node:assert/strict';
import test from 'node:test';
import {
  beginCatalogRead,
  confirmCatalogRegistration,
  patchCatalogRow,
  queueCatalogRows,
  reconcileCatalogRegistrations,
  reconcileCatalogRows,
  type AvailableCourse,
  type CatalogRegistrations,
  type CatalogRows,
} from './catalogState';

const COURSE: AvailableCourse = {
  id: '101',
  title: 'Algorithms',
  name: 'Algorithms',
  code: 'CS101',
  professor: '',
  registered: false,
};
const NEW_COURSE: AvailableCourse = { ...COURSE, id: '204', name: 'New portfolio course' };

function queuedRows() {
  let committed = reconcileCatalogRows({}, [COURSE]);
  const current = { current: committed };
  const queue: ((previous: CatalogRows) => CatalogRows)[] = [];
  const update = (transform: (previous: CatalogRows) => CatalogRows) =>
    queueCatalogRows(current, (action) => queue.push(action), transform);
  const flush = () => {
    for (const action of queue.splice(0)) committed = action(committed);
    return committed;
  };
  return { current, update, flush };
}

test('Add completion cannot discard a new catalog row queued before React commits', () => {
  const rows = queuedRows();
  rows.update((previous) => patchCatalogRow(previous, COURSE.id, { name: 'Edited course', busy: true }));
  rows.flush();
  rows.update((previous) => reconcileCatalogRows(previous, [COURSE, NEW_COURSE]));
  rows.update((previous) => patchCatalogRow(previous, COURSE.id, { busy: false, error: null }));
  assert.equal(rows.current.current[NEW_COURSE.id]?.name, NEW_COURSE.name);
  const committed = rows.flush();
  assert.deepEqual(committed, rows.current.current);
  assert.equal(committed[COURSE.id]?.name, 'Edited course');
  assert.equal(committed[COURSE.id]?.busy, false);
  assert.equal(committed[NEW_COURSE.id]?.name, NEW_COURSE.name);
});

test('submission reads the latest name and short name even before queued edits commit', () => {
  const rows = queuedRows();
  rows.update((previous) => patchCatalogRow(previous, COURSE.id, { name: '  Submitted name  ' }));
  rows.update((previous) => patchCatalogRow(previous, COURSE.id, { shortName: '  SN  ' }));
  rows.update((previous) => reconcileCatalogRows(previous, [COURSE, NEW_COURSE]));
  const snapshot = rows.current.current[COURSE.id]!;
  assert.equal(snapshot.name.trim(), 'Submitted name');
  assert.equal(snapshot.shortName.trim(), 'SN');
  rows.update((previous) => patchCatalogRow(previous, COURSE.id, { busy: true, error: null }));
  rows.update((previous) => patchCatalogRow(previous, COURSE.id, { name: 'Newer unsaved edit' }));
  rows.update((previous) => patchCatalogRow(previous, COURSE.id, { busy: false }));
  const committed = rows.flush();
  assert.deepEqual(committed, rows.current.current);
  assert.equal(committed[COURSE.id]?.name, 'Newer unsaved edit');
  assert.equal(snapshot.name.trim(), 'Submitted name');
  assert.equal(committed[COURSE.id]?.shortName.trim(), 'SN');
});

test('a late Add error cannot recreate a row removed by a newer portfolio read', () => {
  const rows = queuedRows();
  rows.update((previous) => reconcileCatalogRows(previous, [NEW_COURSE]));
  rows.update((previous) => patchCatalogRow(previous, COURSE.id, { busy: false, error: 'Rejected' }));
  const committed = rows.flush();
  assert.deepEqual(committed, rows.current.current);
  assert.equal(committed[COURSE.id], undefined);
  assert.ok(committed[NEW_COURSE.id]);
});

test('an older catalog read retains committed registration; a fresh read expires its confirmation', () => {
  const registrations: CatalogRegistrations = { sequence: 0, confirmed: new Map() };
  const older = beginCatalogRead(registrations);
  confirmCatalogRegistration(registrations, COURSE.id);
  assert.equal(reconcileCatalogRegistrations(registrations, [COURSE], older)[0]?.registered, true);
  assert.equal(registrations.confirmed.has(COURSE.id), true);
  const acknowledged = beginCatalogRead(registrations);
  assert.equal(
    reconcileCatalogRegistrations(registrations, [{ ...COURSE, registered: true }], acknowledged)[0]?.registered,
    true,
  );
  assert.equal(registrations.confirmed.has(COURSE.id), false);
  const deletedInAnotherTab = beginCatalogRead(registrations);
  assert.equal(reconcileCatalogRegistrations(registrations, [COURSE], deletedInAnotherTab)[0]?.registered, false);
});

test('fresh catalog absence is authoritative, but an older read cannot expire a newer Add', () => {
  const registrations: CatalogRegistrations = { sequence: 0, confirmed: new Map() };
  confirmCatalogRegistration(registrations, COURSE.id);
  const pending = beginCatalogRead(registrations);
  confirmCatalogRegistration(registrations, NEW_COURSE.id);
  const result = reconcileCatalogRegistrations(registrations, [COURSE, NEW_COURSE], pending);
  assert.equal(result[0]?.registered, false);
  assert.equal(result[1]?.registered, true);
  assert.equal(registrations.confirmed.has(COURSE.id), false);
  assert.equal(registrations.confirmed.has(NEW_COURSE.id), true);
});
