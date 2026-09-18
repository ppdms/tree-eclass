import { useDeferredResource } from '@/lib/useDeferredResource';
import { Button } from '@/components/ui/button';
import RecallPanel from '../reader/RecallPanel';
import type { PracticeView } from '../reader/types';
import type { ContextPanelProps } from './ContextPanel';

export function DeferredRecall(props: ContextPanelProps) {
  const { courseId, unitKey, practice, onRecorded } = props;
  const params = new URLSearchParams({ course_id: String(courseId) });
  if (unitKey) params.set('unit_key', unitKey);
  const { data, error, retry, update } = useDeferredResource<PracticeView>(`/api/study/practice?${params}`, !practice);
  if (error)
    return (
      <p role="alert">
        Recall could not load. <Button onClick={retry}>Try again</Button>
      </p>
    );
  if (!practice && !data) return <p role="status">Loading recall…</p>;
  return (
    <RecallPanel
      courseId={courseId}
      practice={practice || data}
      unitKey={unitKey}
      onRecorded={(value) => {
        update(value);
        onRecorded?.(value);
      }}
    />
  );
}
