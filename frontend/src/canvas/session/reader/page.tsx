import { useLayoutEffect, useRef } from 'react';
import { Raster, Text, View } from '../../ui/primitives';
import { Button } from '../../ui/controls';
import type { CachedPage } from './cache';
import { caretPoint } from './geometry';
import type { PageLayout } from './layout';
import { PageMarks, SelectionPaint } from './marks';
import type { ReaderSelectionController } from './selection';
import type { Annotation, Bounds, ReaderSelection } from './types';

interface PageProps {
  documentId: string;
  layout: PageLayout;
  entry: CachedPage | undefined;
  annotations: Annotation[];
  controller: ReaderSelectionController;
  selection: ReaderSelection | null;
  onAnnotationClick(annotation: Annotation): void;
  onContextMenu?(event: { x: number; y: number; page: number; selection: ReaderSelection | null }): void;
  onDrawn(page: number, image: string): void;
  retry(page: number): void;
}

function TouchHandles({
  entry,
  layout,
  controller,
  selection,
}: {
  entry: CachedPage;
  layout: PageLayout;
  controller: ReaderSelectionController;
  selection: ReaderSelection;
}) {
  if (!entry.geometry || !controller.touch) return null;
  return (['start', 'end'] as const).map((end) => {
    const offset = end === 'start' ? selection.char_start : selection.char_end;
    const point = caretPoint(entry.geometry!, offset);
    if (!point) return null;
    return (
      <View
        key={end}
        id={`reader:handle:${selection.documentId}:${layout.page}:${end}`}
        onPointer={(event) => controller.handle(layout.page, end, event)}
        style={{
          position: 'absolute',
          left: (point.x / entry.geometry!.size.width) * layout.width - 14,
          top: (point.y / entry.geometry!.size.height) * layout.height - 4,
          width: 28,
          height: 32,
          alignItems: 'center',
          justifyContent: 'end',
        }}
      >
        <View style={{ width: 3, height: 12, backgroundColor: '#2563eb' }} />
        <View style={{ width: 22, height: 22, borderRadius: 11, backgroundColor: '#2563eb' }} />
      </View>
    );
  });
}

function PagePixels({
  id,
  entry,
  layout,
  onDrawn,
  retry,
}: {
  id: string;
  entry: CachedPage | undefined;
  layout: PageLayout;
  onDrawn(page: number, image: string): void;
  retry(page: number): void;
}) {
  const image = entry?.image;
  if (image && entry?.geometry)
    return (
      <Raster
        id={id}
        source={image}
        onVisible={() => onDrawn(layout.page, image)}
        style={{ width: layout.width, height: layout.height }}
      />
    );
  return (
    <View
      style={{
        width: layout.width,
        height: layout.height,
        alignItems: 'center',
        justifyContent: 'center',
        gap: 12,
        padding: 24,
      }}
    >
      <Text style={{ color: '#52525b', fontSize: 16 }}>{entry?.error ?? `Loading page ${layout.page}`}</Text>
      {entry?.error ? (
        <Button id={`reader:retry-page:${layout.page}`} onClick={() => retry(layout.page)}>
          Try again
        </Button>
      ) : null}
    </View>
  );
}

export function ReaderPage(props: PageProps) {
  const { layout, entry, controller } = props;
  const id = `reader:page-node:${encodeURIComponent(props.documentId)}:${layout.page}`;
  const box = useRef<Bounds | null>(null);
  const geometry = entry?.geometry;
  useLayoutEffect(() => {
    if (geometry && box.current) controller.updatePage(layout.page, geometry, box.current, id);
    return () => {
      controller.pages.delete(layout.page);
    };
  }, [controller, geometry, id, layout.page]);
  const selected = props.selection?.page === layout.page ? props.selection : null;
  return (
    <View
      id={id}
      label={`Page ${layout.page}`}
      onLayout={(bounds) => {
        box.current = bounds;
        if (geometry) controller.updatePage(layout.page, geometry, bounds, id);
      }}
      onPointer={(event) => controller.pointer(layout.page, event)}
      onContextMenu={(event) =>
        props.onContextMenu?.({ x: event.x, y: event.y, page: layout.page, selection: controller.selection })
      }
      style={{
        position: 'absolute',
        left: layout.left,
        top: layout.top,
        width: layout.width,
        height: layout.height,
        backgroundColor: '#ffffff',
      }}
    >
      <PagePixels id={`${id}:pixels`} entry={entry} layout={layout} onDrawn={props.onDrawn} retry={props.retry} />
      <PageMarks
        annotations={props.annotations}
        width={layout.width}
        height={layout.height}
        activate={props.onAnnotationClick}
      />
      <SelectionPaint selection={selected} width={layout.width} height={layout.height} />
      {selected && entry ? (
        <TouchHandles entry={entry} layout={layout} controller={controller} selection={selected} />
      ) : null}
    </View>
  );
}
