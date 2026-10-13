import { useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react';
import type { CanvasKey, HostProps } from '../../ui/types';
import { useViewport } from '../../app/viewport';
import { clampPage, documentLayout } from './layout';
import type { DocumentLayout } from './layout';
import { useReaderNavigation } from './navigation';
import type { ReaderNavigation } from './navigation';
import { useReaderReadiness } from './readiness';
import type { ReaderReadiness } from './readiness';
import { useReaderResources } from './resources';
import type { ReaderResources } from './resources';
import { useReaderScale } from './scale';
import type { ReaderScale } from './scale';
import { ReaderSelectionController } from './selection';
import type { Annotation, Bounds, ReaderProps, ReaderSelection } from './types';

export interface ReaderModel {
  id: string;
  documentId: string;
  resources: ReaderResources;
  scale: ReaderScale;
  layout: DocumentLayout;
  navigation: ReaderNavigation;
  readiness: ReaderReadiness;
  controller: ReaderSelectionController;
  selection: ReaderSelection | null;
  marks: Map<number, Annotation[]>;
  onLayout(bounds: Bounds): void;
  onKeyInput: NonNullable<HostProps['onKeyInput']>;
}

function useSelection(
  documentId: string,
  onChange: ReaderProps['onSelectionChange'],
  navigation: ReaderNavigation,
  clip: Bounds,
) {
  const [selection, setSelection] = useState<ReaderSelection | null>(null);
  const values = useRef({ onChange, navigation, clip });
  values.current = { onChange, navigation, clip };
  const controller = useMemo(
    () =>
      new ReaderSelectionController(
        documentId,
        (next) => {
          setSelection(next);
          values.current.onChange(next);
        },
        (y) => {
          const { clip: bounds, navigation: nav } = values.current;
          const edge = 36;
          const delta =
            y < bounds.y + edge
              ? -Math.min(28, bounds.y + edge - y)
              : y > bounds.y + bounds.height - edge
                ? Math.min(28, y - bounds.y - bounds.height + edge)
                : 0;
          if (delta) nav.scrollBy(delta);
        },
      ),
    [documentId],
  );
  useLayoutEffect(() => {
    values.current.onChange(null);
    return () => controller.destroy();
  }, [controller]);
  return { controller, selection };
}

function readerKey(model: ReaderModel, event: CanvasKey, height: number) {
  if (event.composing || event.ctrl || event.meta || event.alt) return;
  if (model.controller.key(event.key, event.shift)) return true;
  const nav = model.navigation;
  if (event.key === 'ArrowRight') nav.goToPage(nav.current + 1);
  else if (event.key === 'ArrowLeft') nav.goToPage(nav.current - 1);
  else if (event.key === 'ArrowUp') nav.scrollBy(-96);
  else if (event.key === 'ArrowDown') nav.scrollBy(96);
  else if (event.key === 'PageDown' || (event.key === ' ' && !event.shift)) nav.scrollBy(height);
  else if (event.key === 'PageUp' || (event.key === ' ' && event.shift)) nav.scrollBy(-height);
  else if (event.key === 'Home') nav.goToPage(1);
  else if (event.key === 'End') nav.goToPage(model.resources.source?.pageCount ?? 1);
  else return;
  return true;
}

function annotationsByPage(annotations: Annotation[], documentId: string): Map<number, Annotation[]> {
  const pages = new Map<number, Annotation[]>();
  for (const annotation of annotations) {
    if (
      annotation.status === 'deleted' ||
      (annotation.document_id !== undefined && String(annotation.document_id) !== documentId)
    )
      continue;
    let list = pages.get(annotation.page_number);
    if (!list) pages.set(annotation.page_number, (list = []));
    list.push(annotation);
  }
  return pages;
}

function useDocumentLayout(resources: ReaderResources, zoom: number, width: number): DocumentLayout {
  const sizeVersion = resources.cache?.sizeVersion ?? 0;
  return useMemo(
    () =>
      documentLayout(
        resources.source?.pageCount ?? 0,
        resources.source?.firstSize ?? { width: 612, height: 792 },
        resources.cache?.sizes ?? new Map(),
        zoom,
        width,
      ),
    [resources.source, resources.cache, sizeVersion, zoom, width],
  );
}

function useReaderClip(width: number, height: number) {
  const viewport = useViewport();
  const [bounds, setBounds] = useState<Bounds>({ x: 0, y: viewport.top, width, height });
  const top = Math.max(bounds.y, viewport.top);
  const bottom = Math.min(bounds.y + height, viewport.bottom);
  const clip = { x: bounds.x, y: top, width, height: Math.max(0, bottom - top) };
  const onLayout = (next: Bounds) =>
    setBounds((old) =>
      old.x === next.x && old.y === next.y && old.width === next.width && old.height === next.height ? old : next,
    );
  return { viewport, bounds, clip, top, onLayout };
}

export function useReaderModel(props: ReaderProps): ReaderModel {
  const documentId = String(props.document.document_id);
  const id = `reader:scroll:${encodeURIComponent(documentId)}:${props.document.source_hash ?? ''}`;
  const requested = useRef(clampPage(props.initialPage, Number.MAX_SAFE_INTEGER));
  const resources = useReaderResources(props.document, requested);
  const { viewport, bounds, clip, top, onLayout } = useReaderClip(props.width, props.height);
  const [firstDrawn, setFirstDrawn] = useState(0);
  const scale = useReaderScale(
    documentId,
    resources.source,
    props.width,
    props.height,
    !!resources.cache && firstDrawn === resources.cache.identity,
  );
  const layout = useDocumentLayout(resources, scale.zoom, props.width);
  const navigation = useReaderNavigation(
    id,
    resources.source,
    layout,
    requested,
    clip.height,
    top - bounds.y,
    props.onPageChange,
    props.height,
  );
  const nav = useRef(navigation);
  nav.current = navigation;
  useEffect(() => {
    if (resources.source) nav.current.requestCurrentPage();
  }, [scale.zoom, resources.source]);
  const readiness = useReaderReadiness(resources, navigation, scale.zoom, viewport.dpr, setFirstDrawn);
  const { controller, selection } = useSelection(documentId, props.onSelectionChange, navigation, clip);
  const marks = useMemo(() => annotationsByPage(props.annotations, documentId), [props.annotations, documentId]);
  const model: ReaderModel = {
    id,
    documentId,
    resources,
    scale,
    layout,
    navigation,
    readiness,
    controller,
    selection,
    marks,
    onLayout,
    onKeyInput: (event) => readerKey(model, event, clip.height || props.height),
  };
  return model;
}
