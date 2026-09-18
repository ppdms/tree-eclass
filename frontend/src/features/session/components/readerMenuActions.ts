import { useCallback, type Dispatch, type RefObject, type SetStateAction } from 'react';

import type { PdfReaderHandle } from '@/features/session/reader/PdfReader';

import type { PendingSelection } from './useSessionActions';
import type { DrawerTab } from './useSessionState';

export interface ReaderMenu {
  x: number;
  y: number;
  pageNumber: number;
}

export function useReaderMenuTrigger(
  pageNumber: number,
  setReaderMenu: Dispatch<SetStateAction<ReaderMenu | null>>,
): (event: React.MouseEvent) => void {
  return useCallback(
    (event) => {
      event.preventDefault();
      // SAFETY: mouse events target DOM elements; the cast narrows the
      // generic EventTarget to read closest/getAttribute properties.
      const target = event.target as HTMLElement | null;
      const page = Number(target?.closest?.('[data-page]')?.getAttribute('data-page')) || pageNumber;
      setReaderMenu({ x: event.clientX, y: event.clientY, pageNumber: page });
    },
    [pageNumber, setReaderMenu],
  );
}

export type ReaderActionName =
  | 'previous'
  | 'next'
  | 'fitPage'
  | 'fitWidth'
  | 'zoomIn'
  | 'zoomOut'
  | 'bookmark'
  | 'marks'
  | 'insight'
  | 'recall'
  | 'toggleChrome'
  | 'copy'
  | 'note'
  | 'question'
  | 'highlight';

interface ReaderMenuActions {
  readerMenu: ReaderMenu | null;
  pageNumber: number;
  goToPage: (page: number, behavior?: ScrollBehavior) => void;
  pdfReaderRef: RefObject<PdfReaderHandle | null>;
  bookmarkPage: (targetPage?: number) => Promise<void>;
  setDrawerTab: Dispatch<SetStateAction<DrawerTab | null>>;
  pendingSelection: PendingSelection | null;
  saveAnnotation: (mark: PendingSelection) => Promise<void>;
  setPendingSelection: Dispatch<SetStateAction<PendingSelection | null>>;
  setSelectionCommand: Dispatch<SetStateAction<'note' | null>>;
  setReaderMenu: Dispatch<SetStateAction<ReaderMenu | null>>;
  setChromeVisible: Dispatch<SetStateAction<boolean>>;
}

function runNavigationAction(actionName: ReaderActionName, actions: ReaderMenuActions): boolean {
  const { readerMenu, pageNumber, goToPage, pdfReaderRef, bookmarkPage, setDrawerTab, setChromeVisible } = actions;
  const targetPage = readerMenu?.pageNumber || pageNumber;
  if (actionName === 'previous') {
    goToPage(targetPage - 1);
    return true;
  }
  if (actionName === 'next') {
    goToPage(targetPage + 1);
    return true;
  }
  if (actionName === 'fitPage') {
    pdfReaderRef.current?.fitPage();
    return true;
  }
  if (actionName === 'fitWidth') {
    pdfReaderRef.current?.fitToWidth();
    return true;
  }
  if (actionName === 'zoomIn') {
    pdfReaderRef.current?.zoomIn();
    return true;
  }
  if (actionName === 'zoomOut') {
    pdfReaderRef.current?.zoomOut();
    return true;
  }
  if (actionName === 'bookmark') {
    void bookmarkPage(targetPage);
    return true;
  }
  if (actionName === 'marks' || actionName === 'insight' || actionName === 'recall') {
    setDrawerTab(actionName === 'marks' ? 'notes' : actionName);
    return true;
  }
  if (actionName === 'toggleChrome') {
    setChromeVisible((visible) => !visible);
    return true;
  }
  return false;
}

function runSelectionAction(actionName: ReaderActionName, actions: ReaderMenuActions): void {
  const { pendingSelection, saveAnnotation, setPendingSelection, setSelectionCommand } = actions;
  if (!pendingSelection) return;
  if (actionName === 'copy') {
    void navigator.clipboard?.writeText(pendingSelection.quote || '');
    return;
  }
  if (actionName === 'note') {
    setSelectionCommand('note');
    return;
  }
  if (actionName === 'question' || actionName === 'highlight') {
    void saveAnnotation({ ...pendingSelection, kind: actionName, color: 'yellow', body: null });
    setPendingSelection(null);
  }
}

function runReaderAction(actionName: ReaderActionName, actions: ReaderMenuActions): void {
  actions.setReaderMenu(null);
  if (runNavigationAction(actionName, actions)) return;
  runSelectionAction(actionName, actions);
}

interface UseReaderMenuActionsArgs {
  readerMenu: ReaderMenu | null;
  pageNumber: number;
  goToPage: (page: number, behavior?: ScrollBehavior) => void;
  pdfReaderRef: RefObject<PdfReaderHandle | null>;
  bookmarkPage: (targetPage?: number) => Promise<void>;
  setDrawerTab: Dispatch<SetStateAction<DrawerTab | null>>;
  pendingSelection: PendingSelection | null;
  saveAnnotation: (mark: PendingSelection) => Promise<void>;
  setPendingSelection: Dispatch<SetStateAction<PendingSelection | null>>;
  setSelectionCommand: Dispatch<SetStateAction<'note' | null>>;
  setReaderMenu: Dispatch<SetStateAction<ReaderMenu | null>>;
  setChromeVisible: Dispatch<SetStateAction<boolean>>;
}

export function useReaderMenuActions(args: UseReaderMenuActionsArgs): (actionName: ReaderActionName) => void {
  return useCallback(
    (actionName) => runReaderAction(actionName, args),
    [
      args.bookmarkPage,
      args.goToPage,
      args.pageNumber,
      args.pendingSelection,
      args.pdfReaderRef,
      args.readerMenu,
      args.saveAnnotation,
      args.setChromeVisible,
      args.setDrawerTab,
      args.setPendingSelection,
      args.setReaderMenu,
      args.setSelectionCommand,
    ],
  );
}
