export interface CheckStatus {
  is_checking?: boolean;
  course_name?: string | null;
  last_check_at?: string | null;
  last_check_result?: string | null;
  last_error?: string | null;
  last_files_added?: number | null;
  last_files_changed?: number | null;
}
export interface CheckResponse {
  message?: string;
  detail?: string;
  code?: string;
  is_checking?: boolean;
  current_course?: string;
}
export function isCheckAcknowledged(ok: boolean, status: number, body: CheckResponse): boolean {
  return body.code !== 'runtime_changed' && (ok || (status === 409 && body.is_checking === true));
}
export function runningCheckStatus(previous: CheckStatus | null, body: CheckResponse): CheckStatus {
  return { ...previous, is_checking: true, course_name: body.current_course ?? null, last_error: null };
}
