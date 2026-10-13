import { useState } from 'react';
import { AppLink } from '../app/links';
import { useViewport } from '../app/viewport';
import { Button, Notice } from '../ui/controls';
import { Text, View } from '../ui/primitives';
import { CanvasReader, readerSupportsSource } from './reader';
import { ReaderContextMenu } from './contextMenu';
import { SessionDrawer } from './drawer';
import { FinishPopover } from './finish';
import { InsightPanel } from './insight';
import { InlineMark, MarksPanel } from './marks';
import { RecallPanel } from './recall';
import { SelectionEditor } from './selection';
import { styles } from './styles';
import { SessionToolbar } from './toolbar';
import type { Context, SessionQuery } from './types';
import { useWorkspace, type WorkspaceModel } from './useWorkspace';

export function SessionWorkspace({ query, context }: { query: SessionQuery; context: Context }) {
  const model = useWorkspace(query, context);
  const viewport = useViewport();
  const [geometry, setGeometry] = useState({ y: viewport.top + 180, width: viewport.width - 28 });
  const overlay = viewport.width <= 900;
  const height = Math.max(180, viewport.bottom - geometry.y - 16);
  const width = Math.max(180, geometry.width - (!overlay && model.ui.drawer ? 380 : 0));
  const contentReady =
    !model.source || model.ui.state.ready || model.ui.state.error !== null || !readerSupportsSource(model.source);
  const onLayout = (rect: { y: number; width: number }) =>
    setGeometry((old) => (old.y === rect.y && old.width === rect.width ? old : { y: rect.y, width: rect.width }));
  return (
    <View id="session-workspace" ready={contentReady && !model.marks.loading} style={styles.frame}>
      <WorkspaceBanners model={model} />
      <WorkspaceChrome model={model} />
      <View id="session-reader-row" style={styles.readerRow} onLayout={onLayout}>
        <WorkspaceReader model={model} height={height} width={width} />
        <WorkspaceDrawer model={model} height={height} overlay={overlay} />
      </View>
      <WorkspaceOverlays model={model} />
      <FinishPopover
        open={model.finishOpen}
        onClose={() => model.setFinishOpen(false)}
        action={Boolean(context.action)}
        sitting={model.sitting}
        state={model.finish}
        questions={model.summary.questions}
      />
    </View>
  );
}
function PendingAttention({ sitting }: { sitting: WorkspaceModel['sitting'] }) {
  if (!sitting.attentionError) return null;
  return (
    <Notice tone="warning">
      {sitting.attentionError}
      <Button
        id="session-attention-retry"
        onClick={() => {
          void sitting.ledger?.retry().catch(() => undefined);
        }}
      >
        Retry pending time
      </Button>
      <Text style={styles.muted}>
        If this sitting cannot admit its pending records, you may discard all unconfirmed intervals. This loses unsaved
        time; already committed server time remains recorded.
      </Text>
      <Button
        id="session-attention-discard"
        tone="danger"
        onClick={() => {
          void sitting.ledger?.discardPending().catch(() => undefined);
        }}
      >
        Discard all unconfirmed intervals
      </Button>
    </Notice>
  );
}
function WorkspaceBanners({ model }: { model: WorkspaceModel }) {
  return (
    <View style={styles.column}>
      {model.sitting.starting && <Text style={styles.muted}>Starting or resuming this sitting…</Text>}
      {model.context.notice && <Text style={styles.muted}>{model.context.notice}</Text>}
      {model.finish.intent && !model.finish.result && !model.sitting.session?.ended_at && (
        <Notice tone="warning">
          An outcome is awaiting confirmation. Open Finish session to retry the exact saved outcome and reflection.
        </Notice>
      )}
      {model.sitting.error && (
        <Notice tone="danger">
          Session recovery: {model.sitting.error}
          <Button id="session-start-retry" onClick={model.sitting.retry}>
            Retry start
          </Button>
        </Notice>
      )}
      <PendingAttention sitting={model.sitting} />
      {model.marks.error && (
        <Notice tone="danger">
          {model.marks.error}
          <Button id="session-write-dismiss" onClick={model.marks.dismiss}>
            Dismiss
          </Button>
        </Notice>
      )}
      {model.marks.announcement && <Text style={styles.success}>{model.marks.announcement}</Text>}
      {model.source && !model.source.source_hash && (
        <Notice tone="danger">
          This document has no original source identity. Time and marks cannot be safely saved. Reopen it from the plan.
        </Notice>
      )}
    </View>
  );
}
function WorkspaceChrome({ model }: { model: WorkspaceModel }) {
  if (!model.controls)
    return (
      <Button id="session-show-controls" onClick={() => model.setControls(true)}>
        Show controls
      </Button>
    );
  return (
    <SessionToolbar
      context={model.context}
      source={model.source}
      reader={model.ui.reader.current}
      readerState={model.ui.state}
      page={model.page}
      pageCount={model.count}
      seconds={model.finish.result?.session.active_seconds ?? model.sitting.session?.active_seconds ?? 0}
      paused={model.sitting.paused}
      started={Boolean(model.sitting.session)}
      closed={Boolean(model.finish.result || model.sitting.session?.ended_at)}
      marks={model.summary.count}
      bookmarked={model.summary.bookmarkPages.has(model.page)}
      bookmarkBusy={model.marks.busy.size > 0}
      selection={Boolean(model.ui.selection)}
      drawer={model.ui.drawer}
      onDocument={model.onDocument}
      onPage={model.onPage}
      onDrawer={model.ui.setDrawer}
      onBookmark={() => model.bookmark(model.page)}
      onNote={() => model.ui.changeDraft({ note: true })}
      onHide={() => model.setControls(false)}
      onFinish={() => model.setFinishOpen(true)}
    />
  );
}
function WorkspaceReader({ model, height, width }: { model: WorkspaceModel; height: number; width: number }) {
  if (!model.source)
    return (
      <View style={styles.panel}>
        <Text style={styles.heading}>This action has no document</Text>
        <Text style={styles.body}>
          The planned action has no ready source document. You can record its actual outcome.
        </Text>
        <AppLink href="/study" style={styles.body}>
          Back to plan
        </AppLink>
      </View>
    );
  return (
    <View id="session-reader" style={{ ...styles.reader, width, height }}>
      <CanvasReader
        key={model.ui.key}
        ref={model.ui.reader}
        document={model.source}
        initialPage={model.page}
        height={height}
        width={width}
        annotations={model.marks.marks}
        onPageChange={model.trackPage}
        onSelectionChange={model.ui.onSelection}
        onAnnotationClick={model.ui.setInline}
        onStateChange={model.ui.onState}
        onContextMenu={(event) =>
          model.ui.setMenu({
            ...event,
            selection: model.ui.capture(event.selection),
          })
        }
      />
    </View>
  );
}
function WorkspaceDrawer({ model, height, overlay }: { model: WorkspaceModel; height: number; overlay: boolean }) {
  return (
    <SessionDrawer
      tab={model.ui.drawer}
      page={model.page}
      height={height}
      overlay={overlay}
      marks={model.summary.count}
      onTab={model.ui.setDrawer}
      onClose={() => model.ui.setDrawer(null)}
      insight={<InsightPanel course={model.query.courseId} source={model.source} page={model.page} />}
      markList={
        <MarksPanel
          actions={model.marks}
          editor={model.ui.editor}
          onEditor={model.ui.setEditor}
          onPage={model.onPage}
        />
      }
      recall={<RecallPanel course={model.query.courseId} state={model.recall} />}
    />
  );
}
function WorkspaceOverlays({ model }: { model: WorkspaceModel }) {
  const inline = model.marks.marks.find((mark) => String(mark.id) === String(model.ui.inline?.id)) ?? model.ui.inline;
  return (
    <>
      <SelectionEditor
        selection={model.ui.selection}
        note={model.ui.draft.note}
        draft={model.ui.draft.body}
        busy={model.marks.busy.has(`create:${model.ui.key}`)}
        error={model.marks.error}
        onDraft={(body) => model.ui.changeDraft({ body })}
        onNote={(note) => model.ui.changeDraft({ note })}
        onDismiss={model.ui.clearSelection}
        onSave={(kind, color) => {
          if (model.ui.selection) void model.saveSelection(model.ui.selection, kind, color);
        }}
      />
      <InlineMark mark={inline} actions={model.marks} onClose={() => model.ui.setInline(null)} />
      <ReaderContextMenu
        menu={model.ui.menu}
        count={model.count}
        controls={model.controls}
        busy={model.marks.busy.size > 0}
        onClose={() => model.ui.setMenu(null)}
        bookmarked={model.summary.bookmarkPages.has(model.ui.menu?.page ?? 0)}
        onHighlight={(selection) => {
          model.ui.setSelection(selection);
          void model.saveSelection(selection, 'highlight', 'yellow');
        }}
        onQuestion={(selection) => {
          model.ui.setSelection(selection);
          void model.saveSelection(selection, 'question', 'yellow');
        }}
        onNote={model.ui.beginNote}
        onBookmark={model.bookmark}
        onControls={() => model.setControls(!model.controls)}
      />
    </>
  );
}
