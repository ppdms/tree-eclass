import { useRef, useState } from 'react';
import type { ReaderHandle, ReaderState } from './reader/types';
import type { ReaderMenu } from './contextMenu';
import type { MarkEditor } from './marks';
import type { Context, DrawerTab, Mark, Sitting, Source } from './types';
import { sourceKey } from './useMarks';
import { useSelectionUI, type SelectionUIState } from './useSelection';

const initialReader: ReaderState = { ready: false, pageCount: 0, zoom: 0, mode: 'page', error: null };
export function useReaderUI(
  course: string,
  context: Context,
  source: Source | null,
  session: Sitting | null,
): ReaderUIState {
  const key = source ? sourceKey(source) : '';
  const reader = useRef<ReaderHandle>(null);
  const currentSource = useRef(key);
  currentSource.current = key;
  const [report, setReport] = useState({ key: '', state: initialReader });
  const [drawer, setDrawer] = useState<DrawerTab | null>(null);
  const selection = useSelectionUI({ course, context, source, session, key, currentSource, reader });
  const [menu, setMenu] = useState<ReaderMenu | null>(null);
  const [inline, setInline] = useState<Mark | null>(null);
  const [editors, setEditors] = useState<Record<string, MarkEditor | null>>({});
  return {
    ...selection,
    key,
    reader,
    state: report.key === key ? report.state : initialReader,
    drawer,
    setDrawer,
    onState: (state: ReaderState) => {
      if (currentSource.current === key) setReport({ key, state });
    },
    menu,
    setMenu,
    inline: source && String(inline?.document_id) === String(source.document_id) ? inline : null,
    setInline,
    editor: editors[key] ?? null,
    setEditor: (editor: MarkEditor | null) => setEditors((all) => ({ ...all, [key]: editor })),
    clearOverlays: () => {
      setMenu(null);
      setInline(null);
      selection.setSelection(null);
    },
  };
}
export interface ReaderUIState extends SelectionUIState {
  key: string;
  reader: React.RefObject<ReaderHandle | null>;
  state: ReaderState;
  drawer: DrawerTab | null;
  setDrawer: (tab: DrawerTab | null) => void;
  onState: (state: ReaderState) => void;
  menu: ReaderMenu | null;
  setMenu: (menu: ReaderMenu | null) => void;
  inline: Mark | null;
  setInline: (mark: Mark | null) => void;
  editor: MarkEditor | null;
  setEditor: (editor: MarkEditor | null) => void;
  clearOverlays: () => void;
}
