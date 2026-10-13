import { useCallback, useEffect, useRef, useState } from 'react';
import type { ExternalMaterial } from '@/lib/types';
import { requestJson } from '../app/api';
import { materialsPath } from './helpers';
import type { MaterialsPayload } from './load';
import { reconcileMaterials } from './mutations';
import type { PendingChanges, SetMaterials } from './mutations';

interface TrackedMaterial {
  id: string;
  epoch: number;
  notifyWhenReady: boolean;
}

interface PollJob {
  courseId: number | string;
  tracked: TrackedMaterial;
  attempts: number;
  controller: AbortController;
  cancelTimer?: () => void;
  current: () => boolean;
  consume: (materials: ExternalMaterial[]) => void;
  publish: () => void;
  finish: () => void;
}

async function pollMaterial(job: PollJob): Promise<void> {
  if (!job.current()) return;
  job.attempts += 1;
  let payload: MaterialsPayload | undefined;
  try {
    payload = await requestJson<MaterialsPayload>(materialsPath(job.courseId), { signal: job.controller.signal });
  } catch {
    // Failed reads do not discard the acknowledged upload; retries remain bounded.
  }
  if (!job.current()) return;
  if (payload) {
    const found = payload.materials.find((material) => material.document_id === job.tracked.id);
    job.consume(payload.materials);
    if (found?.status === 'ready' && job.tracked.notifyWhenReady) {
      job.tracked.notifyWhenReady = false;
      job.publish();
    }
    if (found && !['pending', 'running'].includes(found.status) && found.classification_source !== 'pending') {
      job.finish();
      return;
    }
  }
  if (job.attempts >= 20) job.finish();
  else scheduleMaterialPoll(job, 3000);
}

function scheduleMaterialPoll(job: PollJob, delay: number): void {
  const timer = setTimeout(() => {
    void pollMaterial(job);
  }, delay);
  job.cancelTimer = () => clearTimeout(timer);
}

export function useMaterialPolling(
  courseId: string | number,
  setMaterials: SetMaterials,
  pending: PendingChanges,
  publish: () => void,
) {
  const epoch = useRef(0);
  const [tracked, setTracked] = useState<TrackedMaterial | null>(null);
  useEffect(() => {
    if (!tracked) return;
    const controller = new AbortController();
    const current = () => !controller.signal.aborted && epoch.current === tracked.epoch;
    const job: PollJob = {
      courseId,
      tracked: { ...tracked },
      controller,
      attempts: 0,
      current,
      publish,
      consume: (rows) => {
        const attempts = job.attempts;
        setMaterials((previous) =>
          current() ? reconcileMaterials(rows, previous, tracked.id, attempts, pending.current) : previous,
        );
      },
      finish: () => {
        if (current()) setTracked(null);
      },
    };
    scheduleMaterialPoll(job, 1500);
    return () => {
      controller.abort();
      job.cancelTimer?.();
    };
  }, [courseId, pending, publish, setMaterials, tracked]);
  return useCallback((material: ExternalMaterial) => {
    epoch.current += 1;
    setTracked({ id: material.document_id, epoch: epoch.current, notifyWhenReady: material.status !== 'ready' });
  }, []);
}
