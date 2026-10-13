import { forwardRef, useEffect, useImperativeHandle, useRef } from 'react';
import type { Ref } from 'react';
import { ScrollArea } from '../../ui/overlays';
import { colors } from '../../ui/palette';
import { View } from '../../ui/primitives';
import { useReaderModel } from './model';
import type { ReaderModel } from './model';
import { ReaderPage } from './page';
import { sourceFormat, sourceIdentity } from './source';
import { ReaderStatus } from './states';
import type { ReaderHandle, ReaderProps, ReaderState } from './types';

export type {
  Annotation,
  AnnotationRect,
  ReaderHandle,
  ReaderProps,
  ReaderSelection,
  ReaderState,
  SessionDocument,
} from './types';
export { annotationAnchorId } from './types';
export { readerSupportsSource } from './source';

function useReaderReport(props: ReaderProps, model: ReaderModel): void {
  const callback = useRef(props.onStateChange);
  callback.current = props.onStateChange;
  const ready = model.readiness.ready;
  const pageCount = model.resources.source?.pageCount ?? props.document.page_count ?? 0;
  const { zoom, mode } = model.scale;
  const error = model.readiness.error;
  useEffect(() => {
    const state: ReaderState = { ready, pageCount, zoom, mode, error };
    callback.current(state);
  }, [ready, pageCount, zoom, mode, error]);
}

function useReaderCommands(ref: Ref<ReaderHandle>, props: ReaderProps, model: ReaderModel): void {
  useImperativeHandle(
    ref,
    () => ({
      goToPage: (page) => model.navigation.goToPage(page),
      fitWidth: model.scale.fitWidth,
      fitPage: model.scale.fitPage,
      zoomIn: model.scale.zoomIn,
      zoomOut: model.scale.zoomOut,
      clearSelection: () => model.controller.clear(),
      focusAnnotation: (annotation) => {
        if (annotation.document_id !== undefined && String(annotation.document_id) !== model.documentId) return;
        model.navigation.goToPage(annotation.page_number, Math.max(0, (annotation.rects?.[0]?.y ?? 0) - 0.08));
        props.onAnnotationClick(annotation);
      },
    }),
    [model, props.onAnnotationClick],
  );
}

function ReaderContent({ props, model }: { props: ReaderProps; model: ReaderModel }) {
  const { resources, readiness, layout } = model;
  const unsupported = sourceFormat(props.document) === 'unsupported';
  if (unsupported || readiness.error || !resources.source)
    return (
      <ReaderStatus
        document={props.document}
        page={model.navigation.target}
        count={resources.source?.pageCount ?? props.document.page_count ?? 0}
        error={readiness.error}
        unsupported={unsupported}
        retry={resources.retry}
      />
    );
  return (
    <View style={{ width: layout.width, height: layout.height }}>
      {readiness.pages.map((page) => (
        <ReaderPage
          key={page}
          documentId={model.documentId}
          layout={layout.pages[page - 1]!}
          entry={resources.cache?.pages.get(page)}
          annotations={model.marks.get(page) ?? []}
          controller={model.controller}
          selection={model.selection}
          onAnnotationClick={props.onAnnotationClick}
          onContextMenu={props.onContextMenu}
          onDrawn={readiness.onDrawn}
          retry={(number) => resources.cache?.retry(number)}
        />
      ))}
    </View>
  );
}

const ReaderBody = forwardRef<ReaderHandle, ReaderProps>(function ReaderBody(props, ref) {
  const model = useReaderModel(props);
  useReaderReport(props, model);
  useReaderCommands(ref, props, model);
  return (
    <View
      id={`${model.id}:frame`}
      focusable
      label="Document reader"
      onLayout={model.onLayout}
      onKeyInput={model.onKeyInput}
      onContextMenu={(event) =>
        props.onContextMenu?.({
          x: event.x,
          y: event.y,
          page: model.navigation.current,
          selection: model.controller.selection,
        })
      }
      style={{ width: props.width, height: props.height, minWidth: 0, backgroundColor: colors.page }}
    >
      <ScrollArea
        id={model.id}
        height={props.height}
        axis="both"
        onScroll={model.navigation.onScroll}
        onUserScroll={model.navigation.onUserScroll}
        style={{ width: props.width }}
      >
        <ReaderContent props={props} model={model} />
      </ScrollArea>
    </View>
  );
});

/** A source revision is the lifetime boundary, not ordinary tracked-page or metadata updates. */
export const CanvasReader = forwardRef<ReaderHandle, ReaderProps>(function CanvasReader(props, ref) {
  return <ReaderBody key={sourceIdentity(props.document)} {...props} ref={ref} />;
});
