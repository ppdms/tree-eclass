import { lazy, Suspense } from 'react';
import { SessionLoading } from '@/app/SessionLoading';
import type { SessionProps } from '@/features/session/Session';

const Session = lazy(() => import('@/features/session/Session'));
export function SessionIsland(props: SessionProps) {
  return (
    <Suspense fallback={<SessionLoading />}>
      <Session {...props} />
    </Suspense>
  );
}
