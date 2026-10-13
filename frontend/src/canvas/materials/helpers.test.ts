import assert from 'node:assert/strict';
import test from 'node:test';
import type { ExternalMaterial } from '@/lib/types';
import {
  classificationLabel,
  groupMaterials,
  materialDetail,
  materialId,
  MATERIAL_TYPES,
  materialTypeValue,
  replaceMaterial,
  statusLabel,
} from './helpers.ts';

const MATERIAL: ExternalMaterial = {
  id: 'opaque:1',
  document_id: 'opaque:1',
  course_id: 101,
  display_name: 'Exam.pdf',
  source_path: 'external/Παλιά θέματα/Exam.pdf',
  source_origin: 'external',
  material_type: 'past_paper',
  classification_source: 'manual',
  status: 'ready',
  document_kind: 'pdf',
  source_label: 'Browser upload',
  source_size_bytes: 2048,
  page_count: 2,
  open_url: '/study/session?course_id=101&document_id=opaque%3A1',
  download_url: '/api/study/document/opaque%3A1/content?course_id=101',
};

test('all seven groups retain source order and omit empty categories', () => {
  const rows = MATERIAL_TYPES.map((material_type) => ({ ...MATERIAL, material_type, document_id: material_type }));
  assert.deepEqual(
    groupMaterials(rows.reverse(), '').map((group) => group.type),
    MATERIAL_TYPES,
  );
  assert.deepEqual(
    groupMaterials([MATERIAL], '').map((group) => group.type),
    ['past_paper'],
  );
});

test('search is trimmed and case-insensitive across name, source path, category, and source label', () => {
  for (const query of [' EXAM.PDF ', 'παλιά θέματα', 'Past PAPERS', 'BROWSER UPLOAD']) {
    assert.equal(groupMaterials([MATERIAL], query)[0]?.materials[0], MATERIAL);
  }
  assert.deepEqual(groupMaterials([MATERIAL], 'no matching file'), []);
});

test('source metadata and readiness labels preserve the original presentation', () => {
  assert.equal(materialDetail(MATERIAL), 'pdf · 2 pages · 2.0 KB · Browser upload · Manually classified');
  assert.deepEqual(['ready', 'pending', 'running', 'unsupported', 'failed'].map(statusLabel), [
    'Ready',
    'Indexing',
    'Indexing',
    'Unsupported',
    'Needs attention',
  ]);
  assert.equal(classificationLabel('pending'), 'AI classification pending');
  assert.equal(classificationLabel('ai'), 'AI classified');
  assert.equal(classificationLabel('folder'), 'Folder fallback');
  assert.equal(materialTypeValue('invalid'), 'other');
});

test('acknowledgement replaces and moves only its exact document while preserving real URLs', () => {
  const other = { ...MATERIAL, id: 'other', document_id: 'other' };
  const acknowledged = { ...MATERIAL, page_count: 9 };
  assert.deepEqual(replaceMaterial([other, MATERIAL], acknowledged), [acknowledged, other]);
  assert.equal(acknowledged.open_url, MATERIAL.open_url);
  assert.equal(acknowledged.download_url, MATERIAL.download_url);
  assert.equal(materialId(MATERIAL), 'material:101:opaque%3A1');
});
