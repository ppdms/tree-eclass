import assert from 'node:assert/strict';
import test from 'node:test';
import type { ExternalMaterial } from '@/lib/types';
import { isStateUpdater } from '../practice/stateAction.ts';
import { replaceMaterial } from './helpers.ts';
import { changeMaterialType, reconcileMaterials } from './mutations.ts';
import type { MaterialEdits, PendingTypeChange, TypeChangeContext } from './mutations.ts';

const MATERIAL: ExternalMaterial = {
  id: 'opaque:1',
  document_id: 'opaque:1',
  course_id: 101,
  display_name: 'Notes.pdf',
  source_path: 'external/Notes.pdf',
  source_origin: 'external',
  material_type: 'student_notes',
  classification_source: 'pending',
  status: 'pending',
};

interface MaterialHarnessState {
  materials: ExternalMaterial[];
  edits: MaterialEdits;
  acknowledged: ExternalMaterial | null;
  publications: number;
}

function contextFor(material: ExternalMaterial) {
  const state: MaterialHarnessState = {
    materials: [material],
    edits: {},
    acknowledged: null,
    publications: 0,
  };
  const context: TypeChangeContext = {
    pending: { current: new Map<string, PendingTypeChange>() },
    mounted: { current: true },
    setMaterials: (action) => {
      state.materials = isStateUpdater(action) ? action(state.materials) : action;
    },
    setEdits: (action) => {
      state.edits = isStateUpdater(action) ? action(state.edits) : action;
    },
    acknowledge: (row) => {
      state.acknowledged = row;
      state.materials = replaceMaterial(state.materials, row);
    },
    publish: () => {
      state.publications += 1;
    },
  };
  return { context, state };
}

test('a type choice is optimistic, disabled until acknowledgement, and uses the exact PATCH contract', async (t) => {
  const { context, state } = contextFor(MATERIAL);
  let finish!: (response: Response) => void;
  const response = new Promise<Response>((resolve) => {
    finish = resolve;
  });
  t.mock.method(globalThis, 'fetch', (path: RequestInfo | URL, options?: RequestInit) => {
    assert.equal(path, '/api/v1/courses/101/materials/opaque%3A1');
    assert.equal(options?.method, 'PATCH');
    assert.equal(new Headers(options?.headers).get('Content-Type'), 'application/json');
    assert.equal(options?.body, JSON.stringify({ material_type: 'textbook' }));
    return response;
  });
  const operation = changeMaterialType(context, MATERIAL, 'textbook');
  assert.equal(state.materials[0]?.material_type, 'textbook');
  assert.equal(state.edits[MATERIAL.document_id]?.busy, true);
  assert.equal(state.publications, 0);
  const acknowledged = { ...MATERIAL, material_type: 'textbook', classification_source: 'manual' };
  finish(Response.json({ material: acknowledged }));
  await operation;
  assert.deepEqual(state.acknowledged, acknowledged);
  assert.equal(state.edits[MATERIAL.document_id]?.busy, false);
  assert.equal(context.pending.current.size, 0);
  assert.equal(state.publications, 1);
});

test('a rejected type choice rolls back category without throwing away newer source metadata', async (t) => {
  const { context, state } = contextFor(MATERIAL);
  let finish!: (response: Response) => void;
  const response = new Promise<Response>((resolve) => {
    finish = resolve;
  });
  t.mock.method(globalThis, 'fetch', () => response);
  const operation = changeMaterialType(context, MATERIAL, 'past_paper');
  const indexed = {
    ...MATERIAL,
    material_type: 'textbook' as const,
    classification_source: 'ai' as const,
    status: 'ready',
    page_count: 12,
    download_url: '/real/content?course_id=101',
  };
  state.materials = reconcileMaterials([indexed], state.materials, MATERIAL.document_id, 1, context.pending.current);
  assert.equal(state.materials[0]?.material_type, 'past_paper');
  finish(Response.json({ detail: 'The original runtime changed.' }, { status: 409 }));
  await operation;
  assert.equal(state.materials[0]?.material_type, 'textbook');
  assert.equal(state.materials[0]?.page_count, 12);
  assert.equal(state.materials[0]?.download_url, indexed.download_url);
  assert.equal(state.edits[MATERIAL.document_id]?.busy, false);
  assert.equal(state.publications, 0);
});

test('polling retains an absent acknowledged row until the final successful bounded read', () => {
  const pending = new Map<string, PendingTypeChange>();
  assert.deepEqual(reconcileMaterials([], [MATERIAL], MATERIAL.document_id, 19, pending), [MATERIAL]);
  assert.deepEqual(reconcileMaterials([], [MATERIAL], MATERIAL.document_id, 20, pending), []);
  const authoritative = { ...MATERIAL, status: 'unsupported', classification_source: 'folder' as const };
  assert.deepEqual(reconcileMaterials([authoritative], [MATERIAL], MATERIAL.document_id, 1, pending), [authoritative]);
});

test('in-flight category edits remain present through a final read and suppress duplicate PATCHes', async (t) => {
  const { context, state } = contextFor(MATERIAL);
  let finish!: (response: Response) => void;
  const response = new Promise<Response>((resolve) => {
    finish = resolve;
  });
  const fetch = t.mock.method(globalThis, 'fetch', () => response);
  const operation = changeMaterialType(context, MATERIAL, 'study_guide');
  await changeMaterialType(context, MATERIAL, 'textbook');
  assert.equal(fetch.mock.callCount(), 1);
  assert.deepEqual(
    reconcileMaterials([], state.materials, MATERIAL.document_id, 20, context.pending.current),
    state.materials,
  );
  finish(Response.json({ material: { ...MATERIAL, material_type: 'study_guide', classification_source: 'manual' } }));
  await operation;
});
