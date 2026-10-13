import { useEffect, useReducer, useState } from 'react';
import type { ExternalMaterial } from '@/lib/types';
import { requestJson } from '../app/api';
import { materialsPath } from './helpers';

export interface MaterialsPayload {
  materials: ExternalMaterial[];
}

export function useInitialMaterials(courseId: number | string, open: boolean) {
  const [materials, setMaterials] = useState<ExternalMaterial[] | null>(null);
  const [failed, setFailed] = useState(false);
  const [attempt, retry] = useReducer((value: number) => value + 1, 0);
  useEffect(() => {
    if (!open || materials !== null) return;
    const controller = new AbortController();
    setFailed(false);
    void requestJson<MaterialsPayload>(materialsPath(courseId), { signal: controller.signal })
      .then((payload) => {
        if (!controller.signal.aborted) setMaterials(payload.materials);
      })
      .catch(() => {
        if (!controller.signal.aborted) setFailed(true);
      });
    return () => controller.abort();
  }, [courseId, open, materials, attempt]);
  return { materials, failed, retry };
}
