import * as React from 'react';
import { Button } from '@/components/ui/button';
import { fetchJson } from '@/lib/api';
import type { ExternalMaterial } from '@/lib/types';
import ExternalMaterials from './materials';

export function LazyExternalMaterials({ courseId, query }: { courseId: string | number; query: string }) {
  const [open, setOpen] = React.useState(false);
  const [materials, setMaterials] = React.useState<ExternalMaterial[] | null>(null);
  const [error, setError] = React.useState(false);
  const [attempt, retry] = React.useReducer((n: number) => n + 1, 0);
  React.useEffect(() => {
    setMaterials(null);
    setOpen(false);
  }, [courseId]);
  React.useEffect(() => {
    if (!open || materials) return;
    const controller = new AbortController();
    setError(false);
    fetchJson<{ materials: ExternalMaterial[] }>(`/api/v1/courses/${courseId}/materials`, { signal: controller.signal })
      .then((value) => {
        if (!controller.signal.aborted) setMaterials(value.materials);
      })
      .catch(() => {
        if (!controller.signal.aborted) setError(true);
      });
    return () => controller.abort();
  }, [courseId, open, materials, attempt]);
  return (
    <details onToggle={(event) => setOpen(event.currentTarget.open)}>
      <summary>External library</summary>
      {open && error && (
        <p role="alert">
          External library could not load. <Button onClick={retry}>Try again</Button>
        </p>
      )}
      {open && !materials && !error && <p role="status">Loading external library…</p>}
      {materials && <ExternalMaterials courseId={courseId} initialMaterials={materials} query={query} />}
    </details>
  );
}
