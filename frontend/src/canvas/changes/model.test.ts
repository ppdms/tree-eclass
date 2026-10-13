import assert from 'node:assert/strict';
import test from 'node:test';
import type { ChangeItem } from '@/lib/types';
import { buildChangeRows, changeCounts, changeKind, changeScope } from './model.ts';

const SOURCE_CHANGES: ChangeItem[] = [
  { change_type: 'added_directory', file_path: 'New' },
  { change_type: 'added_file', file_path: 'New/Αρχείο + #.pdf' },
  { change_type: 'deleted_directory', file_path: 'Removed' },
  { change_type: 'deleted_file', file_path: 'Removed/no-copy.pdf' },
  { change_type: 'modified_directory', file_path: 'Updated' },
  { change_type: 'modified_file', file_path: 'Updated/current.pdf', diff_webdav_path: '/comparison/original.pdf' },
];

test('all file and directory kinds retain presentation and exact file-link eligibility', () => {
  const rows = buildChangeRows(SOURCE_CHANGES, '/INF123/eclass/');
  const types = rows.map((row) => row.type);
  assert.deepEqual(types, [
    'added_directory',
    'added_file',
    'deleted_directory',
    'deleted_file',
    'modified_directory',
    'modified_file',
  ]);
  assert.deepEqual(
    rows.map((row) => row.kind),
    ['added', 'added', 'deleted', 'deleted', 'modified', 'modified'],
  );
  assert.equal(rows[1]?.href, '/files/INF123/eclass/New/%CE%91%CF%81%CF%87%CE%B5%CE%AF%CE%BF%20%2B%20%23.pdf');
  assert.equal(rows[3]?.href, null);
  assert.equal(rows[5]?.href, '/files/INF123/eclass/Updated/current.pdf');
  assert.equal(rows[5]?.diffHref, '/files/comparison/original.pdf');
  assert.ok(rows.filter((row) => row.directory).every((row) => row.href === null && row.diffHref === null));
});

test('old redirects remain exact while no-copy and non-modified comparison variants remain unlinked', () => {
  const changes: ChangeItem[] = [
    { change_type: 'deleted_file', file_path: 'a.pdf', redirect_url: 'https://source.invalid/old?x=%2F#revision' },
    { change_type: 'deleted_file', file_path: 'b.pdf', diff_webdav_path: '/not-a-deleted-comparison.pdf' },
    { change_type: 'added_file', file_path: 'c.pdf', diff_webdav_path: '/not-an-added-comparison.pdf' },
    { change_type: 'added', file_path: 'd.pdf' },
    { change_type: 'modified_file', file_path: 'e.pdf', diff_webdav_path: '/real%20comparison.pdf' },
  ];
  const rows = buildChangeRows(changes);
  assert.equal(rows[0]?.href, changes[0]?.redirect_url);
  assert.ok(rows.slice(1).every((row) => row.href === null));
  assert.ok(rows.slice(0, 4).every((row) => row.diffHref === null));
  assert.equal(rows[4]?.diffHref, '/files/real%2520comparison.pdf');
  assert.equal(buildChangeRows([changes[3]!], 'INF/eclass')[0]?.href, null);
});

test('inferred nested directories retain mixed changes and directory-first alphabetical order', () => {
  const rows = buildChangeRows([
    { change_type: 'added_file', file_path: 'Parent/Child/z.pdf' },
    { change_type: 'deleted_file', file_path: 'Parent/Child/a.pdf' },
    { change_type: 'added_file', file_path: 'Parent/b.pdf' },
    { change_type: 'modified_file', file_path: 'z-root.pdf' },
  ]);
  assert.deepEqual(
    rows.map((row) => row.path),
    ['Parent', 'Parent/Child', 'Parent/Child/a.pdf', 'Parent/Child/z.pdf', 'Parent/b.pdf', 'z-root.pdf'],
  );
  assert.equal(rows[0]?.kind, 'mixed');
  assert.equal(rows[1]?.kind, 'mixed');
  assert.equal(rows[2]?.prefix, '│   │   ├── ');
  assert.equal(rows[3]?.prefix, '│   │   └── ');
  assert.equal(rows[4]?.prefix, '│   └── ');
});

test('logical file and comparison paths preserve literal percent identities through browser decoding', () => {
  const rows = buildChangeRows(
    [{ change_type: 'modified_file', file_path: 'a%2Fb#.pdf', diff_webdav_path: '/_diffs/a%2Fb#.pdf' }],
    'Course %/eclass',
  );
  assert.equal(rows[0]?.href, '/files/Course%20%25/eclass/a%252Fb%23.pdf');
  assert.equal(rows[0]?.diffHref, '/files/_diffs/a%252Fb%23.pdf');
});

test('semantic composite IDs are stable across unrelated insertions and distinguish scalar ID types', () => {
  const change: ChangeItem = { id: 1, change_type: 'modified_file', file_path: 'a.pdf' };
  const original = buildChangeRows([change])[0]!;
  const inserted = buildChangeRows([{ change_type: 'added_file', file_path: '0.pdf' }, change]);
  assert.equal(inserted.find((row) => row.path === 'a.pdf')?.identity, original.identity);
  assert.notEqual(buildChangeRows([{ ...change, id: '1' }])[0]?.identity, original.identity);
  assert.notEqual(buildChangeRows([{ ...change, redirect_url: '/previous' }])[0]?.identity, original.identity);
  assert.equal(new Set(buildChangeRows([change, change]).map((row) => row.identity)).size, 2);
  assert.notEqual(changeScope(123, '4'), changeScope('123', '4'));
  assert.notEqual(changeScope('123', '4'), changeScope('12', '34'));
});

test('summary preserves first-kind order, counts and legacy kind fallbacks', () => {
  assert.deepEqual(
    changeCounts([{ change_type: 'added_file' }, { type: 'deleted_file' }, {}, { change_type: 'added_file' }]),
    [
      ['added_file', 2],
      ['deleted_file', 1],
      ['modified', 1],
    ],
  );
  assert.deepEqual(buildChangeRows([]), []);
  assert.equal(changeKind('mixed'), 'mixed');
  assert.equal(changeKind('unchanged'), 'unchanged');
});
