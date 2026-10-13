import { useRef, useState, type RefObject } from 'react';
import type { ReaderHandle, ReaderSelection } from './reader/types';
import type { Context, Selection, Sitting, Source } from './types';
import { mutationContext } from './useMarks';

export interface SelectionDraft {
  body: string;
  note: boolean;
}
export interface SelectionUIState {
  selection: Selection | null;
  setSelection: (selection: Selection | null) => void;
  draft: SelectionDraft;
  changeDraft: (patch: Partial<SelectionDraft>) => void;
  clearSelection: () => void;
  beginNote: (selection: Selection) => void;
  savedSelection: (selection: Selection) => void;
  capture: (selection: ReaderSelection | null) => Selection | null;
  onSelection: (selection: ReaderSelection | null) => void;
}
interface SelectionArgs {
  course: string;
  context: Context;
  source: Source | null;
  session: Sitting | null;
  key: string;
  currentSource: { current: string };
  reader: RefObject<ReaderHandle | null>;
}
export function useSelectionUI(args: SelectionArgs): SelectionUIState {
  const [selection, setSelection] = useState<Selection | null>(null);
  const [drafts, setDrafts] = useState<Record<string, SelectionDraft>>({});
  const current = useRef(selection);
  current.current = selection;
  const visible =
    selection &&
    args.source &&
    selection.documentId === String(args.source.document_id) &&
    selection.context.source_hash === args.source.source_hash
      ? selection
      : null;
  const id = visible ? selectionDraftId(visible) : '';
  const draft = drafts[id] ?? { body: '', note: false };
  const changeDraft = (patch: Partial<SelectionDraft>) =>
    setDrafts((all) => ({ ...all, [id]: { ...(all[id] ?? { body: '', note: false }), ...patch } }));
  const capture = (next: ReaderSelection | null): Selection | null => {
    if (!next || !args.source || next.documentId !== String(args.source.document_id)) return null;
    return { ...next, context: mutationContext(args.course, args.context, args.source, args.session) };
  };
  const clearSelection = () => {
    setSelection(null);
    args.reader.current?.clearSelection();
  };
  const savedSelection = (saved: Selection) => {
    if (!current.current || selectionDraftId(current.current) !== selectionDraftId(saved)) return;
    setSelection(null);
    if (args.currentSource.current === args.key) args.reader.current?.clearSelection();
  };
  const beginNote = (selected: Selection) => {
    const target = selectionDraftId(selected);
    setSelection(selected);
    setDrafts((all) => ({ ...all, [target]: { body: all[target]?.body ?? '', note: true } }));
  };
  const onSelection = (next: ReaderSelection | null) => {
    if (!next && draft.note) return;
    if (args.currentSource.current === args.key) setSelection(capture(next));
  };
  return {
    selection: visible,
    setSelection,
    draft,
    changeDraft,
    clearSelection,
    savedSelection,
    beginNote,
    capture,
    onSelection,
  };
}
export function selectionDraftId(selection: Selection): string {
  return JSON.stringify([
    JSON.stringify([selection.documentId, selection.context.source_hash]),
    selection.page,
    selection.char_start,
    selection.char_end,
  ]);
}
