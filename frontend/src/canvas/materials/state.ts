import { useCallback, useEffect, useRef, useState } from 'react';
import type { ExternalMaterial, ExternalMaterialType } from '@/lib/types';
import { publishCatalogChanged } from '../app/events';
import { replaceMaterial } from './helpers';
import { changeMaterialType } from './mutations';
import type { MaterialEdits, PendingTypeChange } from './mutations';
import { useMaterialPolling } from './polling';

export function useMaterialLibrary(
  courseId: number | string,
  initialMaterials: ExternalMaterial[],
  onPublished?: () => void,
) {
  const [materials, setMaterials] = useState(initialMaterials);
  const [edits, setEdits] = useState<MaterialEdits>({});
  const pending = useRef(new Map<string, PendingTypeChange>());
  const mounted = useRef(false);
  const publication = useRef(onPublished);
  publication.current = onPublished;
  useEffect(() => {
    mounted.current = true;
    return () => {
      mounted.current = false;
    };
  }, []);
  const publish = useCallback(() => {
    publishCatalogChanged();
    publication.current?.();
  }, []);
  const track = useMaterialPolling(courseId, setMaterials, pending, publish);
  const acknowledge = useCallback(
    (material: ExternalMaterial) => {
      track(material);
      setMaterials((current) => replaceMaterial(current, material));
    },
    [track],
  );
  const uploaded = useCallback(
    (material: ExternalMaterial) => {
      acknowledge(material);
      if (material.status === 'ready') publish();
    },
    [acknowledge, publish],
  );
  const changeType = useCallback(
    (material: ExternalMaterial, nextType: ExternalMaterialType) =>
      changeMaterialType({ setMaterials, setEdits, pending, mounted, acknowledge, publish }, material, nextType),
    [acknowledge, publish],
  );
  return { materials, edits, uploaded, changeType };
}
