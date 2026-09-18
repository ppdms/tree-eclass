import type { ReactNode } from 'react';
import { Button } from '@/components/ui/button';
import { useDeferredResource } from '@/lib/useDeferredResource';
import type { RoadmapBlueprint } from '@/lib/types';

type Roadmap = NonNullable<RoadmapBlueprint['blueprint']>;
export function DeferredSection({
  courseId,
  revision,
  section,
  open,
  children,
}: {
  courseId: string | number;
  revision?: string;
  section: string;
  open: boolean;
  children: (roadmap: Roadmap) => ReactNode;
}) {
  const path = `/api/v1/courses/${courseId}/roadmap/sections/${section}?revision=${encodeURIComponent(revision || '')}`;
  const { data, error, retry } = useDeferredResource<{ roadmap: Roadmap }>(path, open);
  if (!open) return null;
  if (error)
    return (
      <p role="alert">
        Section could not load. <Button onClick={retry}>Try again</Button>
      </p>
    );
  if (!data) return <p role="status">Loading section…</p>;
  return children(data.roadmap);
}
