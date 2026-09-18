import { storageKey } from '@/lib/browserStorage';
import { useEffect, useState, useRef, type Dispatch, type SetStateAction } from 'react';

import { errorLikeSchema, errorMessage } from '@/lib/errors';
import { api, requestKey } from '@/features/session/reader/api';
import type {
  Annotation,
  PracticeView,
  SessionContext,
  SessionDocument,
  SessionInfo,
} from '@/features/session/reader/types';

type ErrorSetter = Dispatch<SetStateAction<string | null>>;

/** One key per sitting, so a reload re-adopts rather than double-counts. */
function sessionKeyFor(courseId: string | number, actionId: string): string {
  const key = storageKey(`study-session:${courseId}:${actionId}`);
  try {
    const existing = window.sessionStorage.getItem(key);
    if (existing) return existing;
    const created = requestKey('sess');
    window.sessionStorage.setItem(key, created);
    return created;
  } catch {
    return requestKey('sess');
  }
}

export interface UseSessionContextResult {
  context: SessionContext | null;
  error: string | null;
  setError: ErrorSetter;
  annotations: Annotation[];
  setAnnotations: Dispatch<SetStateAction<Annotation[]>>;
  practice: PracticeView | null;
  setPractice: Dispatch<SetStateAction<PracticeView | null>>;
}

/** Loads the sitting: the blueprint action, documents, marks, recall queue. */
export function useSessionContext(
  courseId: string | number,
  actionId: string | null,
  documentId: string | null,
): UseSessionContextResult {
  const [context, setContext] = useState<SessionContext | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [annotations, setAnnotations] = useState<Annotation[]>([]);
  const [practice, setPractice] = useState<PracticeView | null>(null);

  useEffect(() => {
    const controller = new AbortController();
    setContext(null);
    setError(null);
    api
      .context(courseId, actionId, documentId, controller.signal)
      .then((result) => {
        if (controller.signal.aborted) return;
        setContext(result);
        setAnnotations(result.annotations || []);
        setPractice(null);
      })
      .catch((loadError) => {
        if (!controller.signal.aborted) {
          const parsed = errorLikeSchema.safeParse(loadError);
          const errorLike = parsed.success ? parsed.data : { message: String(loadError) };
          setError(errorMessage(errorLike, 'Could not load the session.'));
        }
      });
    return () => {
      controller.abort();
    };
  }, [courseId, actionId, documentId]);

  return { context, error, setError, annotations, setAnnotations, practice, setPractice };
}

// Opening the workspace is what starts the clock. There is no separate
// "start" button, because a button that must be remembered is exactly the
// manual bookkeeping this replaces.
// A document opened from the file tree has no action, so the sitting is
// keyed on the document instead. It still measures; it simply has no plan
// action to report an outcome against.
export function useSessionStart(
  context: SessionContext | null,
  courseId: string | number,
  activeDocument: SessionDocument | null,
  session: SessionInfo | null,
  setSession: Dispatch<SetStateAction<SessionInfo | null>>,
  setError: ErrorSetter,
): void {
  useEffect(() => {
    if (!context || session) return;
    const action = context.action;
    const key = action ? action.action_id : `doc:${activeDocument?.document_id}`;
    if (!key) return;
    api
      .startSession({
        course_id: courseId,
        session_key: sessionKeyFor(courseId, key),
        action_id: action?.action_id || '',
        unit_key: context.unit_key || '',
        plan_revision: context.plan_revision || '',
        planned_minutes: action?.estimated_minutes ?? null,
      })
      .then((result) => setSession(result.session))
      .catch((startError) => {
        const parsed = errorLikeSchema.safeParse(startError);
        const errorLike = parsed.success ? parsed.data : { message: String(startError) };
        setError(errorMessage(errorLike, 'Could not start the session.'));
      });
  }, [context, courseId, session, activeDocument?.document_id, setError, setSession]);
}

// Marks belong to a document, so they are reloaded — and re-checked against
// the document's current bytes — whenever the reader changes document.
export function useAnnotationsReload(
  courseId: string | number,
  activeDocument: SessionDocument | null,
  setAnnotations: Dispatch<SetStateAction<Annotation[]>>,
  context: SessionContext | null,
): void {
  const adopted = useRef<SessionContext | null>(null);
  useEffect(() => {
    if (!activeDocument) return;
    if (context?.annotations_document_id === String(activeDocument.document_id) && adopted.current !== context) {
      adopted.current = context;
      return;
    }
    const controller = new AbortController();
    setAnnotations([]);
    api
      .annotations(courseId, activeDocument.document_id, controller.signal)
      .then((result) => {
        if (!controller.signal.aborted) setAnnotations(result.annotations || []);
      })
      .catch(() => {
        /* A failed read must not show marks from the previous document. */
      });
    return () => controller.abort();
  }, [courseId, activeDocument?.document_id, setAnnotations, context]);
}
