import { storageKey } from '@/lib/browserStorage';
import { useCallback, useState, type Dispatch, type SetStateAction } from 'react';

import { errorLikeSchema, type ErrorLike } from '@/lib/errors';
import { api, requestKey } from '@/features/session/reader/api';
import type {
  Annotation,
  AnnotationRect,
  SessionContext,
  SessionDocument,
  SessionInfo,
} from '@/features/session/reader/types';

import type { AnnotationUpdatePayload } from '@/features/session/reader/api';

/** The working shape of a pending mark before its course/document are filled in. */
export interface MarkInput {
  pageNumber: number;
  kind?: string;
  quote?: string;
  prefix?: string;
  suffix?: string;
  char_start?: number;
  char_end?: number;
  rects?: AnnotationRect[];
  color?: string;
  body?: string | null;
}

/** The selection awaiting a save: an anchor plus the page it was made on. */
export interface PendingSelection extends MarkInput {
  pageNumber: number;
  screenRect?: { top: number; bottom: number; left: number; width: number };
}

type AnnotationSetter = Dispatch<SetStateAction<Annotation[]>>;
type PendingSetter = Dispatch<SetStateAction<PendingSelection | null>>;
type ErrorSetter = Dispatch<SetStateAction<string | null>>;
type AnnounceSetter = Dispatch<SetStateAction<string>>;

interface AnnotationDeps {
  courseId: string | number;
  context: SessionContext | null;
  session: SessionInfo | null;
  activeDocument: SessionDocument | null;
  setAnnotations: AnnotationSetter;
  setPendingSelection: PendingSetter;
  setError: ErrorSetter;
  setAnnotationAnnouncement: AnnounceSetter;
}

/** Plain-language fallback for write failures; raw engine text never reaches the UI. */
function presentationMessage(raw: ErrorLike): string {
  const text = String(raw.message ?? '').trim();
  if (!text) return 'Something went wrong while saving your work. Try again.';
  if (/circular structure|__reactFiber|stateNode|state object/i.test(text))
    return 'Something went wrong while saving your work. Try again.';
  if (/network|failed to fetch|connection/i.test(text))
    return 'Could not reach the server. Check the connection and try again.';
  const stripped = text
    .replace(/https?:\/\/[^\s'"]+/g, '')
    .replace(/['"`][^'"`]*['"`]/g, '')
    .replace(/\s+/g, ' ')
    .trim();
  return stripped || 'Something went wrong while saving your work. Try again.';
}

function normalizedRects(rects: AnnotationRect[] | undefined): AnnotationRect[] {
  return Array.isArray(rects)
    ? rects.map((rect) => ({
        x: Number(rect.x) || 0,
        y: Number(rect.y) || 0,
        w: Number(rect.w) || 0,
        h: Number(rect.h) || 0,
      }))
    : [];
}

function annotationPayload(deps: AnnotationDeps, mark: MarkInput) {
  const documentId = deps.activeDocument?.document_id;
  if (documentId === undefined) return null;
  return {
    course_id: deps.courseId,
    document_id: documentId,
    page_number: mark.pageNumber,
    kind: mark.kind ?? '',
    quote: mark.quote,
    prefix: mark.prefix,
    suffix: mark.suffix,
    char_start: mark.char_start,
    char_end: mark.char_end,
    rects: normalizedRects(mark.rects),
    color: mark.color,
    body: mark.body,
    action_id: deps.context?.action?.action_id,
    unit_key: deps.context?.unit_key,
    plan_revision: deps.context?.plan_revision,
    session_id: deps.session?.id,
    idempotency_key: requestKey('ann'),
  };
}

async function saveAnnotationAction(deps: AnnotationDeps, mark: MarkInput): Promise<void> {
  const { activeDocument, setAnnotations, setAnnotationAnnouncement, setPendingSelection, setError } = deps;
  if (!activeDocument) return;
  const payload = annotationPayload(deps, mark);
  if (!payload) return;
  try {
    const result = await api.createAnnotation(payload);
    setAnnotations((current) => [...current, result.annotation]);
    setAnnotationAnnouncement(`${result.annotation.kind || 'Annotation'} saved.`);
    setPendingSelection(null);
    window.getSelection()?.removeAllRanges();
  } catch (saveError) {
    const parsed = errorLikeSchema.safeParse(saveError);
    setError(presentationMessage(parsed.success ? parsed.data : { message: String(saveError) }));
  }
}

async function updateAnnotationAction(
  deps: AnnotationDeps,
  id: string | number,
  patch: AnnotationUpdatePayload,
): Promise<Annotation | null> {
  const { setAnnotations, setAnnotationAnnouncement, setError } = deps;
  try {
    const result = await api.updateAnnotation(id, patch);
    setAnnotations((current) => current.map((item) => (item.id === id ? result.annotation : item)));
    setAnnotationAnnouncement('Annotation updated.');
    return result.annotation;
  } catch (updateError) {
    const parsed = errorLikeSchema.safeParse(updateError);
    setError(
      presentationMessage(parsed.success ? parsed.data : { message: String(updateError) }) ||
        'Could not update this mark.',
    );
    return null;
  }
}

async function deleteAnnotationAction(deps: AnnotationDeps, id: string | number): Promise<boolean> {
  const { setAnnotations, setAnnotationAnnouncement, setError } = deps;
  try {
    await api.deleteAnnotation(id);
    setAnnotations((current) => current.filter((item) => item.id !== id));
    setAnnotationAnnouncement('Annotation deleted.');
    return true;
  } catch (deleteError) {
    const parsed = errorLikeSchema.safeParse(deleteError);
    setError(
      presentationMessage(parsed.success ? parsed.data : { message: String(deleteError) }) ||
        'Could not delete this mark.',
    );
    return false;
  }
}

export interface UseAnnotationActionsArgs {
  courseId: string | number;
  context: SessionContext | null;
  session: SessionInfo | null;
  activeDocument: SessionDocument | null;
  setAnnotations: AnnotationSetter;
  setPendingSelection: PendingSetter;
  setError: ErrorSetter;
}

export function useAnnotationActions({
  courseId,
  context,
  session,
  activeDocument,
  setAnnotations,
  setPendingSelection,
  setError,
}: UseAnnotationActionsArgs) {
  const [annotationAnnouncement, setAnnotationAnnouncement] = useState('');
  const deps: AnnotationDeps = {
    courseId,
    context,
    session,
    activeDocument,
    setAnnotations,
    setPendingSelection,
    setError,
    setAnnotationAnnouncement,
  };
  const saveAnnotation = useCallback(
    (mark: MarkInput) => saveAnnotationAction(deps, mark),
    [activeDocument, courseId, context, session, setAnnotations, setError, setPendingSelection],
  );
  const updateAnnotation = useCallback(
    (id: string | number, patch: AnnotationUpdatePayload) => updateAnnotationAction(deps, id, patch),
    [setAnnotations, setError],
  );
  const deleteAnnotation = useCallback(
    (id: string | number) => deleteAnnotationAction(deps, id),
    [setAnnotations, setError],
  );

  return {
    saveAnnotation,
    updateAnnotation,
    deleteAnnotation,
    annotationAnnouncement,
  };
}

export interface UseFinishSessionArgs {
  session: SessionInfo | null;
  courseId: string | number;
  context: SessionContext | null;
  activeDocument: SessionDocument | null;
  setClosing: Dispatch<SetStateAction<string | null>>;
  setClosed: Dispatch<SetStateAction<SessionInfo | null>>;
  setError: ErrorSetter;
}

export function useFinishSession({
  session,
  courseId,
  context,
  activeDocument,
  setClosing,
  setClosed,
  setError,
}: UseFinishSessionArgs) {
  const finish = useCallback(
    async (outcome: string) => {
      if (!session) return;
      setClosing(outcome);
      try {
        const result = await api.finishSession({
          session_id: session.id,
          outcome,
          note: null,
        });
        setClosed(result);
        const sessionKey = context?.action?.action_id || `doc:${activeDocument?.document_id}`;
        if (sessionKey) {
          try {
            window.sessionStorage.removeItem(storageKey(`study-session:${courseId}:${sessionKey}`));
          } catch {
            /* Clearing the key is a convenience, not a requirement. */
          }
        }
      } catch (finishError) {
        const parsed = errorLikeSchema.safeParse(finishError);
        setError(presentationMessage(parsed.success ? parsed.data : { message: String(finishError) }));
      } finally {
        setClosing(null);
      }
    },
    [session, courseId, context, activeDocument, setClosed, setClosing, setError],
  );

  return { finish };
}
