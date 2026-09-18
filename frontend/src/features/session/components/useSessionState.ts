import { useEffect, useMemo, useRef, useState, type Dispatch, type RefObject, type SetStateAction } from 'react';

import { useAttention } from '@/features/session/reader/useAttention';
import type {
  Annotation,
  PracticeView,
  SessionContext,
  SessionDocument,
  SessionInfo,
} from '@/features/session/reader/types';

import type { PendingSelection } from './useSessionActions';
import { useAnnotationActions, useFinishSession } from './useSessionActions';
import { useAnnotationsReload, useSessionContext, useSessionStart } from './useSessionEffects';

export type DrawerTab = 'insight' | 'notes' | 'recall';

function useSessionTiming({
  session,
  activeDocument,
  pageNumber,
  closed,
}: {
  session: SessionInfo | null;
  activeDocument: SessionDocument | null;
  pageNumber: number;
  closed: SessionInfo | null;
}) {
  const { activeSeconds, idle } = useAttention({
    sessionId: session?.id,
    documentId: activeDocument?.document_id,
    pageNumber,
    enabled: Boolean(session && activeDocument && !closed),
  });

  // Before the first beat lands the hook has nothing yet, so a resumed sitting
  // shows the total it already banked rather than restarting from zero.
  const measuredSeconds = activeSeconds || session?.active_seconds || 0;

  return { measuredSeconds, idle };
}

interface SessionMutationsArgs {
  courseId: string | number;
  context: SessionContext | null;
  session: SessionInfo | null;
  activeDocument: SessionDocument | null;
  setAnnotations: Dispatch<SetStateAction<Annotation[]>>;
  setPendingSelection: Dispatch<SetStateAction<PendingSelection | null>>;
  setClosing: Dispatch<SetStateAction<string | null>>;
  setClosed: Dispatch<SetStateAction<SessionInfo | null>>;
  setError: Dispatch<SetStateAction<string | null>>;
}

function useSessionMutations({
  courseId,
  context,
  session,
  activeDocument,
  setAnnotations,
  setPendingSelection,
  setClosing,
  setClosed,
  setError,
}: SessionMutationsArgs) {
  const { saveAnnotation, updateAnnotation, deleteAnnotation, annotationAnnouncement } = useAnnotationActions({
    courseId,
    context,
    session,
    activeDocument,
    setAnnotations,
    setPendingSelection,
    setError,
  });
  const { finish } = useFinishSession({
    session,
    courseId,
    context,
    activeDocument,
    setClosing,
    setClosed,
    setError,
  });
  return {
    saveAnnotation,
    updateAnnotation,
    deleteAnnotation,
    annotationAnnouncement,
    finish,
  };
}

function useQuestionCount(annotations: Annotation[]): number {
  return useMemo(
    () => annotations.filter((item) => item.kind === 'question' && item.status !== 'deleted').length,
    [annotations],
  );
}

interface WorkspaceCoreArgs {
  courseId: string | number;
  actionId: string | null;
  documentId: string | null;
  initialPageNumber: number | undefined;
}

function usePageClamp(activeDocument: SessionDocument | null, setPageNumber: Dispatch<SetStateAction<number>>) {
  useEffect(() => {
    if (!activeDocument) return;
    const pageCount = Number(activeDocument.page_count);
    if (pageCount > 0) setPageNumber((current) => Math.min(current, pageCount));
  }, [activeDocument, setPageNumber]);
}

function useSessionWorkspaceCore({ courseId, actionId, documentId, initialPageNumber }: WorkspaceCoreArgs) {
  const { context, error, setError, annotations, setAnnotations, practice, setPractice } = useSessionContext(
    courseId,
    actionId,
    documentId,
  );
  const [session, setSession] = useState<SessionInfo | null>(null);
  const [documentIndex, setDocumentIndex] = useState(0);
  const [pageNumber, setPageNumber] = useState(
    Number.isInteger(initialPageNumber) && initialPageNumber! > 0 ? initialPageNumber! : 1,
  );
  const [tab, setTab] = useState<DrawerTab>('insight');
  const [pendingSelection, setPendingSelection] = useState<PendingSelection | null>(null);
  const [closing, setClosing] = useState<string | null>(null);
  const [closed, setClosed] = useState<SessionInfo | null>(null);
  const readerRef = useRef<HTMLDivElement>(null);

  const activeDocument = context?.documents?.[documentIndex] || null;
  usePageClamp(activeDocument, setPageNumber);

  useSessionStart(context, courseId, activeDocument, session, setSession, setError);
  useAnnotationsReload(courseId, activeDocument, setAnnotations, context);

  return {
    context,
    error,
    session,
    documentIndex,
    setDocumentIndex,
    pageNumber,
    setPageNumber,
    tab,
    setTab,
    activeDocument,
    readerRef,
    annotations,
    setAnnotations,
    pendingSelection,
    setPendingSelection,
    practice,
    setPractice,
    closing,
    setClosing,
    closed,
    setClosed,
    setError,
  };
}

export interface SessionState {
  context: SessionContext | null;
  error: string | null;
  setError: Dispatch<SetStateAction<string | null>>;
  session: SessionInfo | null;
  documentIndex: number;
  setDocumentIndex: Dispatch<SetStateAction<number>>;
  pageNumber: number;
  setPageNumber: Dispatch<SetStateAction<number>>;
  activeDocument: SessionDocument | null;
  readerRef: RefObject<HTMLDivElement | null>;
  tab: DrawerTab;
  setTab: Dispatch<SetStateAction<DrawerTab>>;
  annotations: Annotation[];
  pendingSelection: PendingSelection | null;
  setPendingSelection: Dispatch<SetStateAction<PendingSelection | null>>;
  practice: PracticeView | null;
  setPractice: Dispatch<SetStateAction<PracticeView | null>>;
  closing: string | null;
  closed: SessionInfo | null;
  measuredSeconds: number;
  idle: boolean;
  saveAnnotation: (mark: PendingSelection) => Promise<void>;
  updateAnnotation: (
    id: string | number,
    patch: { status?: string; body?: string | null; color?: string },
  ) => Promise<Annotation | null>;
  deleteAnnotation: (id: string | number) => Promise<boolean>;
  annotationAnnouncement: string;
  finish: (outcome: string) => Promise<void>;
  questionCount: number;
}

export function useSessionState(
  courseId: string | number,
  actionId: string | null,
  documentId: string | null,
  initialPageNumber = 1,
): SessionState {
  const core = useSessionWorkspaceCore({ courseId, actionId, documentId, initialPageNumber });
  const { measuredSeconds, idle } = useSessionTiming(core);
  const { saveAnnotation, updateAnnotation, deleteAnnotation, annotationAnnouncement, finish } = useSessionMutations({
    courseId,
    ...core,
  });
  const questionCount = useQuestionCount(core.annotations);
  return {
    context: core.context,
    error: core.error,
    setError: core.setError,
    session: core.session,
    documentIndex: core.documentIndex,
    setDocumentIndex: core.setDocumentIndex,
    pageNumber: core.pageNumber,
    setPageNumber: core.setPageNumber,
    activeDocument: core.activeDocument,
    readerRef: core.readerRef,
    tab: core.tab,
    setTab: core.setTab,
    measuredSeconds,
    idle,
    saveAnnotation,
    updateAnnotation,
    annotations: core.annotations,
    pendingSelection: core.pendingSelection,
    setPendingSelection: core.setPendingSelection,
    practice: core.practice,
    setPractice: core.setPractice,
    closing: core.closing,
    closed: core.closed,
    deleteAnnotation,
    annotationAnnouncement,
    finish,
    questionCount,
  };
}
