import type { Dispatch, MutableRefObject, SetStateAction } from 'react';
import type { ExternalMaterial, ExternalMaterialType } from '@/lib/types';
import { requestJson } from '../app/api';
import { materialsPath } from './helpers';

export interface MaterialEdit {
  busy: boolean;
  error: string;
}

export type MaterialEdits = Record<string, MaterialEdit>;

export interface PendingTypeChange {
  previousType: ExternalMaterialType;
  nextType: ExternalMaterialType;
}

export type PendingChanges = MutableRefObject<Map<string, PendingTypeChange>>;
export type SetMaterials = Dispatch<SetStateAction<ExternalMaterial[]>>;

export interface TypeChangeContext {
  setMaterials: SetMaterials;
  setEdits: Dispatch<SetStateAction<MaterialEdits>>;
  pending: PendingChanges;
  mounted: MutableRefObject<boolean>;
  acknowledge: (material: ExternalMaterial) => void;
  publish: () => void;
}

export async function changeMaterialType(
  context: TypeChangeContext,
  material: ExternalMaterial,
  nextType: ExternalMaterialType,
): Promise<void> {
  const { setMaterials, setEdits, pending, mounted, acknowledge, publish } = context;
  const id = material.document_id;
  if (pending.current.has(id) || nextType === material.material_type) return;
  const edit = { previousType: material.material_type, nextType };
  pending.current.set(id, edit);
  setEdits((current) => ({ ...current, [id]: { busy: true, error: '' } }));
  setMaterials((current) => current.map((row) => (row.document_id === id ? { ...row, material_type: nextType } : row)));
  let payload: { material: ExternalMaterial };
  try {
    payload = await requestJson<{ material: ExternalMaterial }>(
      `${materialsPath(material.course_id)}/${encodeURIComponent(id)}`,
      {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ material_type: nextType }),
      },
    );
  } catch (error) {
    pending.current.delete(id);
    if (!mounted.current) return;
    setMaterials((current) =>
      current.map((row) => (row.document_id === id ? { ...row, material_type: edit.previousType } : row)),
    );
    setEdits((current) => ({
      ...current,
      [id]: {
        busy: false,
        error: error instanceof Error ? error.message : 'Could not change the type.',
      },
    }));
    return;
  }
  pending.current.delete(id);
  if (!mounted.current) return;
  acknowledge(payload.material);
  setEdits((current) => ({ ...current, [id]: { busy: false, error: '' } }));
  publish();
}

export function reconcileMaterials(
  fetched: ExternalMaterial[],
  current: ExternalMaterial[],
  trackedId: string,
  attempts: number,
  pending: Map<string, PendingTypeChange>,
): ExternalMaterial[] {
  const rows = fetched.map((material) => {
    const edit = pending.get(material.document_id);
    if (edit) edit.previousType = material.material_type;
    return edit ? { ...material, material_type: edit.nextType } : material;
  });
  const present = new Set(rows.map((material) => material.document_id));
  for (const material of current) {
    const id = material.document_id;
    if (!present.has(id) && (pending.has(id) || (id === trackedId && attempts < 20))) rows.push(material);
  }
  return rows;
}
