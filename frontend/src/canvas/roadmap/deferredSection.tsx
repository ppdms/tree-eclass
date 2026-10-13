import { useEffect, useState, type ReactNode } from 'react';
import type { RoadmapBlueprint } from '@/lib/types';
import { Button, Notice } from '../ui/controls';
import { Text, View } from '../ui/primitives';
import { isRevisionConflict, useRoadmapResource } from './resource';
import { styles } from './styles';

type Roadmap = NonNullable<RoadmapBlueprint['blueprint']>;

interface DeferredProps {
  id: string;
  courseId: string | number;
  revision?: string;
  version: number;
  section: 'strategy' | 'strategy-evidence' | 'support';
  open: boolean;
  onRefresh: () => void;
  children: (roadmap: Roadmap) => ReactNode;
}

function useDeferredAdmission(error: Error | null, generation: number, acceptedRevision?: string, revision?: string) {
  // A retry clears the request error before it has accepted fresh section data.
  const [rejectedGeneration, setRejectedGeneration] = useState<number | null>(null);
  useEffect(() => {
    if (isRevisionConflict(error)) setRejectedGeneration(generation);
  }, [error, generation]);
  return (
    isRevisionConflict(error) ||
    (rejectedGeneration !== null && generation <= rejectedGeneration) ||
    Boolean(acceptedRevision && acceptedRevision !== revision)
  );
}

export function DeferredSection({
  id,
  courseId,
  revision,
  version,
  section,
  open,
  onRefresh,
  children,
}: DeferredProps) {
  const base = `/api/v1/courses/${encodeURIComponent(courseId)}/roadmap/sections/${section}`;
  const url = `${base}?revision=${encodeURIComponent(revision || '')}`;
  const resource = useRoadmapResource<{ roadmap: Roadmap; revision_id?: string }>(
    url,
    open && Boolean(revision),
    version,
  );
  const stale = useDeferredAdmission(resource.error, resource.generation, resource.data?.revision_id, revision);
  if (!open) return null;
  if (!revision || stale)
    return (
      <Notice tone="warning">
        <Text>The roadmap changed. Refresh it to open the current evidence.</Text>
        <Button id={`${id}:refresh`} onClick={onRefresh}>
          Refresh roadmap
        </Button>
      </Notice>
    );
  return (
    <View style={styles.stack}>
      {resource.error && (
        <Notice tone="danger">
          <Text>{`This section could not load. ${resource.error.message}`}</Text>
          <Button id={`${id}:retry`} onClick={resource.retry}>
            Try again
          </Button>
        </Notice>
      )}
      {resource.pending && <Text style={styles.meta}>Loading section…</Text>}
      {resource.data && children(resource.data.roadmap)}
    </View>
  );
}
