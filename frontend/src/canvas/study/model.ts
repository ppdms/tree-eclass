import type { EvidenceLink, StudyAction, StudyInboxItem, StudyPayload } from '@/lib/types';

export function studyPath(courseId: string | null): string {
  return `/api/v1/study${courseId ? `?course_id=${encodeURIComponent(courseId)}` : ''}`;
}

export function studySelection(data: StudyPayload) {
  const adaptive = data.adaptive_plan?.next_session;
  const inbox = (data.inbox || []).map(inboxAction);
  const focus = data.study_intelligence?.focus_queue;
  return {
    primary: adaptive || focus?.[0] || inbox[0],
    fallback: !adaptive,
    alternatives: adaptive
      ? (data.adaptive_plan?.today_queue || []).filter((item) => item.action_id !== adaptive.action_id)
      : (focus || inbox).slice(1),
  };
}

function inboxAction(item: StudyInboxItem): StudyAction {
  return { ...item, level: item.study_level };
}

export function actionIdentity(action: StudyAction): string {
  return encodeURIComponent(
    JSON.stringify([
      action.course_id,
      action.action_id || '',
      action.blueprint_revision_id || action.plan_revision || '',
      action.source_path || action.file_path || '',
      action.redirect_url || '',
    ]),
  );
}

export function conciseInstruction(action: StudyAction): string {
  const instruction = action.instruction || action.recommended_action || 'Open the next course source.';
  return instruction.split(/\s+covering\s+/i)[0] || instruction;
}

export function sessionHref(action: StudyAction): string {
  return (
    `/study/session?course_id=${encodeURIComponent(String(action.course_id))}` +
    `&action_id=${encodeURIComponent(action.action_id || '')}`
  );
}

export function fallbackHref(action: StudyAction): string {
  const path = action.source_path || action.file_path;
  return action.redirect_url || (path ? `/files${encodeURI(path)}` : '#');
}

export function alternativeHref(action: StudyAction, fallback: boolean): string {
  return fallback
    ? action.redirect_url || `/files${encodeURI(action.source_path || action.file_path || '')}`
    : sessionHref(action);
}

export function evidenceHref(action: StudyAction, link: EvidenceLink): string | null {
  if (link.renderable && link.document_id) {
    return sessionHref(action) + `&document_id=${encodeURIComponent(String(link.document_id))}`;
  }
  return link.url || (link.source_path ? `/files${encodeURI(link.source_path)}` : null);
}

export function evidenceIdentity(link: EvidenceLink): string {
  return encodeURIComponent(
    JSON.stringify([
      link.evidence_class || '',
      link.evidence_ref || '',
      link.document_id ?? '',
      link.source_path || '',
      link.url || '',
      link.page_number ?? '',
      link.label || '',
      link.title || '',
      link.source_name || '',
    ]),
  );
}
