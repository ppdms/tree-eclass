import { useEffect, useMemo, useState, type Dispatch, type SetStateAction } from 'react';
import { z } from 'zod/v4';
import type { JsonValue } from '@/lib/display';
import { usePreference } from '../app/preferences';
import { publishCatalogChanged } from '../app/events';
import type { Context, MarkCreate, Selection, SessionQuery, Source } from './types';
import { useFinish, type FinishState } from './useFinish';
import { useMarks, mutationContext, selectionPayload, type MarkActions } from './useMarks';
import { useReaderUI, type ReaderUIState } from './useReaderUI';
import { useRecall, type RecallState } from './useRecall';
import { useSitting, type SittingState } from './useSitting';

export interface WorkspaceModel {
  query: SessionQuery;
  context: Context;
  source: Source | null;
  page: number;
  count: number;
  controls: boolean;
  setControls: (value: boolean) => void;
  finishOpen: boolean;
  setFinishOpen: (value: boolean) => void;
  ui: ReaderUIState;
  sitting: SittingState;
  finish: FinishState;
  marks: MarkActions;
  summary: { count: number; questions: number; bookmarkPages: Set<number> };
  recall: RecallState;
  onPage: (page: number) => void;
  trackPage: (page: number) => void;
  onDocument: (id: string) => void;
  saveSelection: (selection: Selection, kind: MarkCreate['kind'], color: string) => Promise<void>;
  bookmark: (page: number) => void;
}
function parseControlsPreference(value: JsonValue): boolean {
  const parsed = z.boolean().safeParse(value);
  return parsed.success ? parsed.data : true;
}
function useMarkSummary(marks: MarkActions['marks']) {
  return useMemo(() => {
    const value = { count: 0, questions: 0, bookmarkPages: new Set<number>() };
    for (const mark of marks) {
      if (mark.status === 'deleted') continue;
      value.count += 1;
      if (mark.kind === 'question') value.questions += 1;
      if (mark.kind === 'bookmark') value.bookmarkPages.add(mark.page_number);
    }
    return value;
  }, [marks]);
}
interface WorkspaceNavigationArgs extends Pick<WorkspaceModel, 'context' | 'source' | 'count' | 'sitting' | 'ui'> {
  documentId: string;
  setDocumentId: Dispatch<SetStateAction<string>>;
  setPage: Dispatch<SetStateAction<number>>;
}
function workspaceNavigation(args: WorkspaceNavigationArgs) {
  const { source, count, sitting, ui, context, documentId, setPage, setDocumentId } = args;
  const trackPage = (requested: number) => {
    const next = Math.max(1, Math.min(count || 100000, Math.round(requested)));
    if (!Number.isFinite(next)) return;
    sitting.ledger?.setTarget(
      source
        ? {
            documentId: String(source.document_id),
            sourceHash: source.source_hash ?? '',
            page: next,
            ready: ui.state.ready && Boolean(source.source_hash),
          }
        : null,
    );
    setPage(next);
  };
  const onPage = (requested: number) => {
    trackPage(requested);
    ui.reader.current?.goToPage(requested);
  };
  const onDocument = (id: string) => {
    if (id === documentId || !context.documents.some((document) => String(document.document_id) === id)) return;
    sitting.ledger?.setTarget(null);
    ui.clearOverlays();
    setPage(1);
    setDocumentId(id);
  };
  return { trackPage, onPage, onDocument };
}
interface WorkspaceMutationArgs extends Pick<
  WorkspaceModel,
  'query' | 'context' | 'source' | 'sitting' | 'ui' | 'marks'
> {}
function workspaceMarkActions({ query, context, source, sitting, ui, marks }: WorkspaceMutationArgs) {
  const saveSelection = async (selection: Selection, kind: MarkCreate['kind'], color: string) => {
    const payload = selectionPayload(selection, kind, color, kind === 'note' ? ui.draft.body : undefined);
    if (await marks.create(payload)) {
      const patch: Partial<ReaderUIState['draft']> = { note: false };
      if (kind === 'note') patch.body = '';
      ui.changeDraft(patch);
      ui.savedSelection(selection);
    }
  };
  const bookmark = (target: number) => {
    if (source) void marks.bookmark(target, mutationContext(query.courseId, context, source, sitting.session));
  };
  return { saveSelection, bookmark };
}

export function useWorkspace(query: SessionQuery, context: Context): WorkspaceModel {
  const [documentId, setDocumentId] = useState(String(context.documents[0]?.document_id ?? ''));
  const source = context.documents.find((document) => String(document.document_id) === documentId) ?? null;
  const [page, setPage] = useState(query.page);
  const [controls, setControls] = usePreference('tree-eclass:study:show-controls', true, parseControlsPreference);
  const [finishOpen, setFinishOpen] = useState(false);
  const sitting = useSitting(query.courseId, context);
  const ui = useReaderUI(query.courseId, context, source, sitting.session);
  const marks = useMarks(query.courseId, source, context);
  const summary = useMarkSummary(marks.marks);
  const recall = useRecall(query.courseId, context.unit_key, ui.drawer === 'recall');
  const finish = useFinish(sitting);
  const count = ui.state.pageCount || source?.page_count || 0;
  const { trackPage, onPage, onDocument } = workspaceNavigation({
    source,
    count,
    sitting,
    ui,
    context,
    documentId,
    setPage,
    setDocumentId,
  });
  useWorkspaceEffects({ source, page, ui, sitting, query, finish, onPage });
  const { saveSelection, bookmark } = workspaceMarkActions({ query, context, source, sitting, ui, marks });
  return {
    query,
    context,
    source,
    page,
    count,
    controls,
    setControls,
    finishOpen,
    setFinishOpen,
    ui,
    sitting,
    marks,
    summary,
    recall,
    finish,
    onPage,
    trackPage,
    onDocument,
    saveSelection,
    bookmark,
  };
}
function useWorkspaceEffects(args: {
  source: Source | null;
  page: number;
  ui: ReaderUIState;
  sitting: SittingState;
  query: SessionQuery;
  finish: FinishState;
  onPage: (page: number) => void;
}) {
  useEffect(() => {
    args.onPage(args.query.page);
  }, [args.query.page]);
  useEffect(() => {
    args.sitting.ledger?.setTarget(
      args.source && !args.sitting.session?.ended_at && !args.finish.result
        ? {
            documentId: String(args.source.document_id),
            sourceHash: args.source.source_hash ?? '',
            page: args.page,
            ready: args.ui.state.ready && Boolean(args.source.source_hash),
          }
        : null,
    );
  }, [args.sitting.ledger, args.ui.key, args.ui.state.ready, args.page, args.finish.result]);
  useEffect(() => {
    if (args.finish.result?.study_event_id !== null && args.finish.result) publishCatalogChanged();
  }, [args.finish.result]);
}
