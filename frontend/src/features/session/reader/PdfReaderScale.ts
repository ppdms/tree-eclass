import { storageKey } from '@/lib/browserStorage';
import { useCallback, useEffect, useRef, useState, type RefObject } from 'react';
import { z } from 'zod/v4';
import { isServer } from '@/lib/display';
import type { PDFDocumentProxy } from 'pdfjs-dist';
import type { SessionDocument } from '@/features/session/reader/types';

export interface PageSize {
  width: number;
  height: number;
}

export interface PageSizeRef {
  current: PageSize | null;
}

export type ScaleSetter = React.Dispatch<React.SetStateAction<number | null>>;

export type FitMode = 'page' | 'width' | 'manual';

export interface AutoFitRef {
  current: FitMode | null;
}

export interface UseScaleManagementResult {
  setManualScale: (updater: (value: number | null) => number | null) => void;
  fitToWidth: () => void;
  fitPage: () => void;
  mode: FitMode;
}

const READER_INSET = 32;

export function fitScale(availableWidth: number, naturalWidth: number): number {
  return Math.max(0.4, Math.min(3, (availableWidth - READER_INSET * 2) / naturalWidth));
}

export function pageScale(scroller: HTMLElement, naturalSize: PageSize): number {
  const width = Math.max(1, scroller.clientWidth - READER_INSET * 2);
  const height = Math.max(1, scroller.clientHeight - READER_INSET);
  return Math.max(0.4, Math.min(3, width / naturalSize.width, height / naturalSize.height));
}

export function readerPreferenceKey(documentId: string | number): string {
  return storageKey(`tree-eclass:study:reader:${documentId}`);
}

interface ReaderPreferences {
  mode?: FitMode;
  zoom?: number;
}

export function readReaderPreferences(documentId: string | number | undefined): ReaderPreferences | null {
  if (!documentId) return null;
  try {
    return JSON.parse(localStorage.getItem(readerPreferenceKey(documentId)) || 'null');
  } catch {
    return null;
  }
}

export function useFitToWidth(
  scrollerRef: RefObject<HTMLElement | null>,
  naturalSize: PageSizeRef,
  setScale: ScaleSetter,
  setMode: (mode: FitMode) => void,
  autoFit: AutoFitRef,
): () => void {
  return useCallback(() => {
    autoFit.current = 'width';
    setMode('width');
    if (naturalSize.current && scrollerRef.current)
      setScale(fitScale(scrollerRef.current.clientWidth, naturalSize.current.width));
  }, [scrollerRef, setScale, setMode, naturalSize, autoFit]);
}

export function useFitToPage(
  scrollerRef: RefObject<HTMLElement | null>,
  naturalSize: PageSizeRef,
  setScale: ScaleSetter,
  setMode: (mode: FitMode) => void,
  autoFit: AutoFitRef,
): () => void {
  return useCallback(() => {
    autoFit.current = 'page';
    setMode('page');
    if (naturalSize.current && scrollerRef.current) setScale(pageScale(scrollerRef.current, naturalSize.current));
  }, [scrollerRef, setScale, setMode, naturalSize, autoFit]);
}

export function scaleForMode(mode: FitMode, node: HTMLElement, naturalSize: PageSize): number {
  return mode === 'page' ? pageScale(node, naturalSize) : fitScale(node.clientWidth, naturalSize.width);
}

export function useNaturalPageSize(
  pdf: PDFDocumentProxy | null,
  scrollerRef: RefObject<HTMLElement | null>,
  setScale: ScaleSetter,
  autoFit: AutoFitRef,
  naturalSize: PageSizeRef,
): void {
  useEffect(() => {
    if (!pdf) return undefined;
    let cancelled = false;
    pdf.getPage(1).then((page) => {
      if (cancelled) return;
      const viewport = page.getViewport({ scale: 1 });
      naturalSize.current = { width: viewport.width, height: viewport.height };
      if (autoFit.current && scrollerRef.current && naturalSize.current)
        setScale(scaleForMode(autoFit.current, scrollerRef.current, naturalSize.current));
    });
    return () => {
      cancelled = true;
    };
  }, [pdf, scrollerRef, setScale, autoFit, naturalSize]);
}

export function useResponsiveAutoFit(
  scrollerRef: RefObject<HTMLElement | null>,
  setScale: ScaleSetter,
  autoFit: AutoFitRef,
  naturalSize: PageSizeRef,
): void {
  useEffect(() => {
    const node = scrollerRef.current;
    if (!node) return undefined;
    const observer = new ResizeObserver(() => {
      if (autoFit.current && naturalSize.current) setScale(scaleForMode(autoFit.current, node, naturalSize.current));
    });
    observer.observe(node);
    return () => observer.disconnect();
  }, [scrollerRef, setScale, autoFit, naturalSize]);
}

export function useScaleManagement(
  doc: SessionDocument | null,
  pdf: PDFDocumentProxy | null,
  scrollerRef: RefObject<HTMLElement | null>,
  setScale: ScaleSetter,
  scale: number | null,
): UseScaleManagementResult {
  const autoFit = useRef<FitMode | null>('page');
  const naturalSize = useRef<PageSize | null>(null);
  const [mode, setMode] = useState<FitMode>('page');
  useEffect(() => {
    const saved = readReaderPreferences(doc?.document_id);
    const mobile = !isServer() && window.matchMedia?.('(max-width: 47.5rem)').matches;
    const initialMode = saved?.mode || (mobile ? 'width' : 'page');
    autoFit.current = initialMode === 'manual' ? null : initialMode;
    setMode(initialMode);
    naturalSize.current = null;
    const savedZoom = z.number().safeParse(saved?.zoom);
    setScale(
      initialMode === 'manual' && savedZoom.success && Number.isFinite(savedZoom.data)
        ? Math.min(3, Math.max(0.3, savedZoom.data))
        : null,
    );
  }, [doc?.document_id, setScale]);
  useNaturalPageSize(pdf, scrollerRef, setScale, autoFit, naturalSize);
  useResponsiveAutoFit(scrollerRef, setScale, autoFit, naturalSize);
  useEffect(() => {
    if (!doc?.document_id || !scale) return;
    try {
      localStorage.setItem(readerPreferenceKey(doc.document_id), JSON.stringify({ mode, zoom: scale }));
    } catch {
      /* storage is a preference, not a dependency */
    }
  }, [doc?.document_id, mode, scale]);
  const setManualScale = useCallback(
    (updater: (value: number | null) => number | null) => {
      autoFit.current = null;
      setMode('manual');
      setScale((value) => updater(value));
    },
    [setScale],
  );
  const fitToWidth = useFitToWidth(scrollerRef, naturalSize, setScale, setMode, autoFit);
  const fitPage = useFitToPage(scrollerRef, naturalSize, setScale, setMode, autoFit);
  return { setManualScale, fitToWidth, fitPage, mode };
}
