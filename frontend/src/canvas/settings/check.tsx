import { useRef, useState } from 'react';
import { z } from 'zod/v4';
import { request } from '../app/api';
import { Button, Notice, Panel } from '../ui/controls';
import { Text } from '../ui/primitives';
import { formatDateTime, isSuccessfulResult } from '@/features/settings/format';
import { usePollResource } from './resources';
import { Hint, message, styles } from './shared';
import { isCheckAcknowledged, runningCheckStatus, type CheckStatus } from './checkState';

const detailItemSchema = z.object({ msg: z.unknown().optional(), message: z.unknown().optional() });
const checkResponseSchema = z.object({
  message: z.string().optional(),
  detail: z.unknown().optional(),
  code: z.string().optional(),
  is_checking: z.boolean().optional(),
  current_course: z.string().optional(),
});

function checkError(body: z.infer<typeof checkResponseSchema>, status: number): string {
  if (!body.detail) return `Could not start the course check (HTTP ${status}).`;
  if (!Array.isArray(body.detail)) return String(body.detail);
  return body.detail
    .map((item) => {
      const parsed = detailItemSchema.safeParse(item);
      return String(parsed.success ? (parsed.data.msg ?? parsed.data.message ?? item) : item);
    })
    .join('. ');
}

function useCourseCheck() {
  const resource = usePollResource<CheckStatus>('/api/check-status', 5000, true, true);
  const [busy, setBusy] = useState(false);
  const inFlight = useRef(false);
  const [error, setError] = useState<string | null>(null);
  const [acknowledgement, setAcknowledgement] = useState<string | null>(null);
  const start = async () => {
    if (inFlight.current || resource.data?.is_checking) return;
    inFlight.current = true;
    setBusy(true);
    setError(null);
    setAcknowledgement(null);
    try {
      const response = await request('/api/run-check', {
        method: 'POST',
        headers: { Accept: 'application/json', 'Content-Type': 'application/json' },
        body: '{}',
      });
      const body = checkResponseSchema.parse(await response.json());
      const check = { code: body.code, is_checking: body.is_checking, current_course: body.current_course };
      if (isCheckAcknowledged(response.ok, response.status, check)) {
        resource.acknowledge((previous) => runningCheckStatus(previous, check));
      }
      if (!response.ok) {
        throw new Error(checkError(body, response.status));
      }
      setAcknowledgement(body.message || 'Check accepted by the background worker.');
    } catch (failure) {
      setError(message(failure, 'Could not start the course check.'));
    } finally {
      inFlight.current = false;
      setBusy(false);
    }
  };
  return { resource, busy, error, acknowledgement, start };
}
export function CheckStatusHeader() {
  const { resource, busy, error, acknowledgement, start } = useCourseCheck();
  return (
    <Panel id="settings-course-sync">
      <Text style={styles.heading}>Course sync</Text>
      <Hint>Refresh course files, announcements, and study evidence from the university source.</Hint>
      {resource.data?.is_checking ? (
        <Text>{`Course check in progress${resource.data.course_name ? ` · ${resource.data.course_name}` : ''}…`}</Text>
      ) : (
        <Button id="settings-run-check" disabled={busy} onClick={() => void start()}>
          {busy ? 'Starting…' : 'Run course check'}
        </Button>
      )}
      {acknowledgement && <Notice tone="info">{acknowledgement}</Notice>}
      <CheckMessages status={resource.data} error={error || resource.error || resource.data?.last_error} />
    </Panel>
  );
}
function CheckMessages({ status, error }: { status: CheckStatus | null; error?: string | null }) {
  const counts =
    status && (status.last_files_added || status.last_files_changed)
      ? ` · ${status.last_files_added ?? 0} file change(s), ${status.last_files_changed ?? 0} exercise event(s)`
      : '';
  return (
    <>
      {status?.last_check_at && !status.is_checking && (
        <Notice tone={isSuccessfulResult(status.last_check_result) ? 'success' : 'danger'}>
          {`Last check: ${formatDateTime(status.last_check_at)} · ` +
            `${isSuccessfulResult(status.last_check_result) ? 'succeeded' : 'failed'}${counts}`}
        </Notice>
      )}
      {error && <Notice tone="danger">{error}</Notice>}
    </>
  );
}
