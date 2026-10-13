import assert from 'node:assert/strict';
import test from 'node:test';
import type { CourseFileNode, FileInsight } from '@/lib/types';
import { fileHref, fileIdentity, fileTarget, filterTree, guideIdentity, relativePath } from './helpers.ts';

const TREE: CourseFileNode = {
  name: 'Course',
  local_path: '/Courses/101/eclass',
  files: [{ name: 'Intro.pdf', local_path: '/Courses/101/eclass/Intro.pdf' }],
  children: [
    {
      name: 'Past papers',
      local_path: '/Courses/101/eclass/Past papers',
      files: [
        { name: 'January.pdf', local_path: '/Courses/101/eclass/Past papers/January.pdf' },
        { name: 'June.pdf', local_path: '/Courses/101/eclass/Past papers/June.pdf' },
      ],
      children: [
        {
          name: 'Answers',
          files: [{ name: 'January answer.pdf' }, { name: 'June answer.pdf' }],
        },
      ],
    },
  ],
};

test('folder self-matches preserve the full subtree and untouched identity', () => {
  assert.equal(filterTree(TREE, ''), TREE);
  const result = filterTree(TREE, 'past papers');
  assert.deepEqual(result?.files, []);
  assert.equal(result?.children?.[0], TREE.children?.[0]);
  assert.equal(result?.children?.[0]?.children?.[0]?.files?.length, 2);
});

test('recursive name and full-path matches prune only unmatched branches without mutating the tree', () => {
  const result = filterTree(TREE, 'january');
  assert.equal(result?.children?.[0]?.files?.length, 1);
  assert.equal(result?.children?.[0]?.children?.[0]?.files?.[0]?.name, 'January answer.pdf');
  assert.equal(filterTree(TREE, 'unavailable'), null);
  assert.equal(TREE.children?.[0]?.files?.length, 2);
  assert.equal(filterTree(TREE, '/courses/101/eclass/intro.pdf')?.files?.[0], TREE.files?.[0]);
});

test('logical file paths retain reserved characters and complete opaque archive-member identity', () => {
  const path = '/.tree-eclass/archive-members/doc_α/nested.zip%21%2Ffolder%2Fα%20%2B%20b.pdf';
  const href = fileHref(path);
  assert.equal(decodeURIComponent(href.slice('/files'.length)), path);
  assert.ok(href.includes('%252F'));
  assert.equal(
    fileHref('/Courses/101/eclass/α #?+%:;.pdf'),
    '/files/Courses/101/eclass/%CE%B1%20%23%3F%2B%25%3A%3B.pdf',
  );
  assert.equal(fileHref('Courses/101/a.pdf'), '/files/Courses/101/a.pdf');
});

test('official targets preserve redirect precedence and versions strip only the actual folder prefix', () => {
  const file = { name: 'Notes.pdf', local_path: '/Course/Notes.pdf', url: 'https://example.invalid/source' };
  assert.equal(fileTarget(file), '/files/Course/Notes.pdf');
  assert.equal(
    fileTarget({ ...file, redirect_url: 'https://example.invalid/redirect' }),
    'https://example.invalid/redirect',
  );
  assert.equal(relativePath('/Course/Notes.pdf', '/Course/'), 'Notes.pdf');
  assert.equal(relativePath('/Other/Notes.pdf', '/Course'), '/Other/Notes.pdf');
  assert.equal(relativePath(undefined, '/Course'), '');
});

test('guide cache identity isolates every immutable payload generation but ignores learner/page readiness', () => {
  const insight: FileInsight = {
    course_id: 101,
    id: 'doc:a',
    source_hash: 'hash:b',
    enrichment_model: 'model:c',
    enrichment_analysis_version: 'version:d',
    enrichment_generated_at: '2026-10-12T08:00:00Z',
  };
  const key = guideIdentity(insight);
  const fields = [
    'course_id',
    'id',
    'source_hash',
    'enrichment_model',
    'enrichment_analysis_version',
    'enrichment_generated_at',
  ] as const;
  for (const field of fields) assert.notEqual(guideIdentity({ ...insight, [field]: 'changed' }), key);
  assert.equal(guideIdentity({ ...insight, reading_minutes: 15, page_analysis_ready: 2 }), key);
  assert.notEqual(guideIdentity({ ...insight, id: 'doc', source_hash: 'a:hash:b' }), key);
});

test('semantic control identities are reversible and cannot alias mixed source fields', () => {
  const parts = ['101', '/archive/a%2Fb', 'α\u0000notes.pdf'];
  const id = fileIdentity('file', ...parts);
  assert.deepEqual(JSON.parse(decodeURIComponent(id.slice('file-'.length))), parts);
  assert.notEqual(fileIdentity('file', 101, 'a:b'), fileIdentity('file', '101', 'a', 'b'));
});
