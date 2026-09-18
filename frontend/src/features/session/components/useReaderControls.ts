import { storageKey } from '@/lib/browserStorage';
import { useCallback, useEffect, useRef, useState, type Dispatch, type RefObject, type SetStateAction } from 'react';

import type { Annotation, SessionDocument } from '@/features/session/reader/types';
import type { PdfReaderHandle } from '@/features/session/reader/PdfReader';

import type { MarkInput } from './useSessionActions';
import { ReaderMenu, ReaderActionName, useReaderMenuActions, useReaderMenuTrigger } from './readerMenuActions';

export type { ReaderMenu, ReaderActionName };
import { isServer } from '@/lib/display';
import type { DrawerTab, SessionState } from './useSessionState';

const CHROME_PREFERENCE_KEY = 'tree-eclass:study:show-controls';
const INTERACTIVE_SELECTOR = [
  'input',
  'textarea',
  'select',
  'button',
  'a',
  'summary',
  '[contenteditable="true"]',
  '[role="button"]',
  '[role="menuitem"]',
  '[role="tab"]',
  '[role="textbox"]',
  '.session-context-menu',
  '.session-drawer',
].join(', ');

function readChromeVisibility(): boolean {
  if (isServer()) return true;
  try {
    return window.localStorage.getItem(storageKey(CHROME_PREFERENCE_KEY)) !== 'false';
  } catch {
    return true;
  }
}

function usePersistedChromeVisibility(): [boolean, Dispatch<SetStateAction<boolean>>] {
  const [visible, setVisible] = useState<boolean>(readChromeVisibility);

  useEffect(() => {
    try {
      window.localStorage.setItem(storageKey(CHROME_PREFERENCE_KEY), String(visible));
    } catch {
      /* A blocked storage area should not make the reader unusable. */
    }
  }, [visible]);

  return [visible, setVisible];
}

interface PageNavigationArgs {
  activeDocument: SessionDocument | null;
  readerRef: RefObject<HTMLElement | null>;
  setPageNumber: Dispatch<SetStateAction<number>>;
}

export function usePageNavigation({
  activeDocument,
  readerRef,
  setPageNumber,
}: PageNavigationArgs): (page: number, behavior?: ScrollBehavior) => void {
  return useCallback(
    (page, behavior = 'smooth') => {
      const pageCount = Number(activeDocument?.page_count) || 1;
      const target = Math.max(1, Math.min(pageCount, Number(page) || 1));
      const reduced = !isServer() && window.matchMedia?.('(prefers-reduced-motion: reduce)').matches;
      setPageNumber(target);
      readerRef.current?.querySelector(`[data-page="${target}"]`)?.scrollIntoView({
        behavior: reduced && behavior === 'smooth' ? 'auto' : behavior,
        block: 'start',
      });
    },
    [activeDocument?.page_count, readerRef, setPageNumber],
  );
}

/** All live bookmarks on a page; legacy data may contain duplicates. */
export function findBookmarks(annotations: Annotation[], page: number): Annotation[] {
  return (annotations || []).filter(
    (item) => item.kind === 'bookmark' && item.page_number === page && item.status !== 'deleted',
  );
}

export function useBookmarkPage(
  pageNumber: number,
  saveAnnotation: (mark: MarkInput) => Promise<void>,
  annotations: Annotation[],
  deleteAnnotation: (id: string | number) => Promise<boolean>,
): (targetPage?: number) => Promise<void> {
  return useCallback(
    async (targetPage = pageNumber) => {
      const existing = findBookmarks(annotations, targetPage);
      if (existing.length) {
        await Promise.all(existing.map((item) => deleteAnnotation(item.id)));
        return;
      }
      await saveAnnotation({ pageNumber: targetPage, kind: 'bookmark', quote: '', rects: [] });
    },
    [annotations, deleteAnnotation, pageNumber, saveAnnotation],
  );
}

interface ReaderKeyContext {
  activeDocument: SessionDocument | null;
  pageNumber: number;
  goToPage: (page: number, behavior?: ScrollBehavior) => void;
  readerRef: RefObject<HTMLElement | null>;
}

function readerKeyHandler(event: KeyboardEvent, ctx: ReaderKeyContext): void {
  const { activeDocument, pageNumber, goToPage, readerRef } = ctx;
  // SAFETY: keyboard events target DOM elements; the cast narrows the
  // generic EventTarget to read tag/closest properties.
  const target = event.target as HTMLElement | null;
  if (target?.closest?.(INTERACTIVE_SELECTOR)) return;
  if (!activeDocument) return;
  const pageCount = Number(activeDocument.page_count) || 1;
  const scroller = readerRef.current?.querySelector?.('.session-reader');
  if (event.key === 'ArrowRight') {
    event.preventDefault();
    goToPage(pageNumber + 1, 'auto');
    return;
  }
  if (event.key === 'ArrowLeft') {
    event.preventDefault();
    goToPage(pageNumber - 1, 'auto');
    return;
  }
  if (event.key === 'ArrowDown' || event.key === 'ArrowUp') {
    event.preventDefault();
    scroller?.scrollBy({ top: event.key === 'ArrowDown' ? 96 : -96, behavior: 'auto' });
    return;
  }
  if (event.key === 'PageDown' || event.key === ' ' || event.key === 'PageUp') {
    event.preventDefault();
    const direction = event.key === 'PageUp' || (event.key === ' ' && event.shiftKey) ? -1 : 1;
    scroller?.scrollBy({ top: direction * (scroller?.clientHeight || 600), behavior: 'auto' });
    return;
  }
  if (event.key === 'Home') {
    event.preventDefault();
    goToPage(1, 'auto');
    return;
  }
  if (event.key === 'End') {
    event.preventDefault();
    goToPage(pageCount, 'auto');
  }
}

export function useReaderKeyboard(
  activeDocument: SessionDocument | null,
  pageNumber: number,
  goToPage: (page: number, behavior?: ScrollBehavior) => void,
  readerRef: RefObject<HTMLElement | null>,
): void {
  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) =>
      readerKeyHandler(event, { activeDocument, pageNumber, goToPage, readerRef });
    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [activeDocument, goToPage, pageNumber, readerRef]);
}

export function useReaderMenuDismissal(
  readerMenu: ReaderMenu | null,
  setReaderMenu: Dispatch<SetStateAction<ReaderMenu | null>>,
): void {
  useEffect(() => {
    if (!readerMenu) return undefined;
    const close = () => setReaderMenu(null);
    const escape = (event: KeyboardEvent) => event.key === 'Escape' && close();
    window.addEventListener('pointerdown', close);
    window.addEventListener('keydown', escape);
    return () => {
      window.removeEventListener('pointerdown', close);
      window.removeEventListener('keydown', escape);
    };
  }, [readerMenu, setReaderMenu]);
}

function useDrawerEscape(drawerTab: DrawerTab | null, setDrawerTab: Dispatch<SetStateAction<DrawerTab | null>>): void {
  useEffect(() => {
    if (!drawerTab) return undefined;
    const closeOnEscape = (event: KeyboardEvent) => {
      if (event.key === 'Escape') setDrawerTab(null);
    };
    window.addEventListener('keydown', closeOnEscape);
    return () => window.removeEventListener('keydown', closeOnEscape);
  }, [drawerTab, setDrawerTab]);
}

function useReaderLocalState() {
  const pdfReaderRef = useRef<PdfReaderHandle>(null);
  const [scale, setScale] = useState<number | null>(null);
  const [fitMode, setFitMode] = useState<'page' | 'width' | 'manual'>('page');
  const [drawerTab, setDrawerTab] = useState<DrawerTab | null>(null);
  const [readerMenu, setReaderMenu] = useState<ReaderMenu | null>(null);
  const [selectionCommand, setSelectionCommand] = useState<'note' | null>(null);
  const [chromeVisible, setChromeVisible] = usePersistedChromeVisibility();
  return {
    pdfReaderRef,
    scale,
    setScale,
    fitMode,
    setFitMode,
    drawerTab,
    setDrawerTab,
    readerMenu,
    setReaderMenu,
    selectionCommand,
    setSelectionCommand,
    chromeVisible,
    setChromeVisible,
  };
}

function useReaderMenuController(state: SessionState, local: ReturnType<typeof useReaderLocalState>) {
  const goToPage = usePageNavigation({
    activeDocument: state.activeDocument,
    readerRef: state.readerRef,
    setPageNumber: state.setPageNumber,
  });
  const bookmarkPage = useBookmarkPage(
    state.pageNumber,
    state.saveAnnotation,
    state.annotations,
    state.deleteAnnotation,
  );
  const pageBookmarked = findBookmarks(state.annotations, state.pageNumber).length > 0;
  const onContextMenu = useReaderMenuTrigger(state.pageNumber, local.setReaderMenu);
  const runReaderAction = useReaderMenuActions({
    readerMenu: local.readerMenu,
    pageNumber: state.pageNumber,
    goToPage,
    pdfReaderRef: local.pdfReaderRef,
    bookmarkPage,
    setDrawerTab: local.setDrawerTab,
    pendingSelection: state.pendingSelection,
    saveAnnotation: state.saveAnnotation,
    setPendingSelection: state.setPendingSelection,
    setSelectionCommand: local.setSelectionCommand,
    setReaderMenu: local.setReaderMenu,
    setChromeVisible: local.setChromeVisible,
  });
  return { goToPage, bookmarkPage, pageBookmarked, onContextMenu, runReaderAction };
}

export interface SessionControls {
  pdfReaderRef: RefObject<PdfReaderHandle | null>;
  scale: number | null;
  setScale: Dispatch<SetStateAction<number | null>>;
  fitMode: 'page' | 'width' | 'manual';
  setFitMode: Dispatch<SetStateAction<'page' | 'width' | 'manual'>>;
  drawerTab: DrawerTab | null;
  setDrawerTab: Dispatch<SetStateAction<DrawerTab | null>>;
  goToPage: (page: number, behavior?: ScrollBehavior) => void;
  onBookmark: (targetPage?: number) => Promise<void>;
  pageBookmarked: boolean;
  onContextMenu: (event: React.MouseEvent) => void;
  runReaderAction: (actionName: ReaderActionName) => void;
  readerMenu: ReaderMenu | null;
  setReaderMenu: Dispatch<SetStateAction<ReaderMenu | null>>;
  selectionCommand: 'note' | null;
  setSelectionCommand: Dispatch<SetStateAction<'note' | null>>;
  chromeVisible: boolean;
  setChromeVisible: Dispatch<SetStateAction<boolean>>;
}

export function useSessionControls(state: SessionState): SessionControls {
  const local = useReaderLocalState();
  useDrawerEscape(local.drawerTab, local.setDrawerTab);
  const menu = useReaderMenuController(state, local);
  useReaderKeyboard(state.activeDocument, state.pageNumber, menu.goToPage, state.readerRef);
  useReaderMenuDismissal(local.readerMenu, local.setReaderMenu);
  return {
    pdfReaderRef: local.pdfReaderRef,
    scale: local.scale,
    setScale: local.setScale,
    fitMode: local.fitMode,
    setFitMode: local.setFitMode,
    drawerTab: local.drawerTab,
    setDrawerTab: local.setDrawerTab,
    goToPage: menu.goToPage,
    onBookmark: menu.bookmarkPage,
    pageBookmarked: menu.pageBookmarked,
    onContextMenu: menu.onContextMenu,
    runReaderAction: menu.runReaderAction,
    readerMenu: local.readerMenu,
    setReaderMenu: local.setReaderMenu,
    selectionCommand: local.selectionCommand,
    setSelectionCommand: local.setSelectionCommand,
    chromeVisible: local.chromeVisible,
    setChromeVisible: local.setChromeVisible,
  };
}
