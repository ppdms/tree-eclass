import * as stylex from '@stylexjs/stylex';
import { PanelTop } from 'lucide-react';
import { commonStyles } from '@/styles/common';
import { colors, effects, typography } from '@/styles/tokens.stylex';
import type { SessionDocument } from '@/features/session/reader/types';

import ReaderCard from './components/ReaderCard';
import SelectionToolbar from './components/SelectionToolbar';
import ReaderContextMenu from './components/ReaderContextMenu';
import SessionDrawer from './components/SessionDrawer';
import SessionToolbar from './components/SessionToolbar';
import type { PendingSelection } from './components/useSessionActions';
import type { ReaderActionName, ReaderMenu, SessionControls } from './components/useReaderControls';
import type { DrawerTab, SessionState } from './components/useSessionState';

export interface WorkspaceProps extends SessionState, SessionControls {
  courseId: string | number | null;
}

type FrameProps = WorkspaceProps & {
  visibleCount: number;
  selectDocument: (index: number) => void;
};

const styles = stylex.create({
  frame: {
    borderRadius: 0,
    borderWidth: 0,
    overflow: 'hidden',
    backgroundColor: colors.background,
    boxShadow: 'none',
    color: colors.textPrimary,
    display: 'flex',
    flexDirection: 'column',
    height: '100dvh',
    minHeight: 0,
  },
  readerRow: {
    overflow: 'hidden',
    backgroundColor: colors.background,
    display: 'flex',
    flexBasis: '0%',
    flexGrow: 1,
    flexShrink: 1,
    position: 'relative',
    minHeight: 0,
  },
  chromeReveal: {
    borderColor: colors.borderLight,
    borderRadius: '0.5rem',
    borderWidth: '1px',
    gap: '0.375rem',
    paddingInline: '0.625rem',
    placeItems: 'center',
    backgroundColor: colors.surface,
    boxShadow: effects.shadow,
    color: colors.textPrimary,
    display: 'grid',
    gridAutoFlow: 'column',
    position: 'absolute',
    zIndex: 3,
    height: '2rem',
    minHeight: '2.5rem',
    minWidth: '2.5rem',
    right: '0.5rem',
    top: '0.5rem',
    width: 'auto',
  },
  revealIcon: {
    height: '1rem',
    width: '1rem',
  },
  revealText: {
    fontSize: '0.6875rem',
    fontWeight: 650,
  },
  writeError: {
    borderColor: colors.danger,
    gap: '0.75rem',
    paddingBlock: '0.5rem',
    paddingInline: '0.875rem',
    alignItems: 'center',
    backgroundColor: colors.surface,
    color: colors.danger,
    display: 'flex',
    fontSize: typography.sizeSm,
    justifyContent: 'space-between',
    borderBottomWidth: '1px',
  },
  dismissBtn: {
    borderColor: 'currentColor',
    borderRadius: '0.5rem',
    borderWidth: '1px',
    paddingBlock: '0.25rem',
    paddingInline: '0.5rem',
    backgroundColor: 'transparent',
    color: 'inherit',
    minHeight: '2rem',
  },
});

function ReaderCardPane(props: FrameProps) {
  return (
    <ReaderCard
      ref={props.pdfReaderRef}
      readerRef={props.readerRef}
      activeDocument={props.activeDocument}
      courseId={props.courseId || ''}
      annotations={props.annotations}
      onPageChange={props.setPageNumber}
      onScaleChange={(next, mode) => {
        props.setScale(next);
        // SAFETY: PdfViewer only produces 'page', 'width', or 'manual' as the mode string;
        // the cast narrows string to the FitMode union.
        props.setFitMode(mode as 'page' | 'width' | 'manual');
      }}
      onSelect={props.setPendingSelection}
      onContextMenu={props.onContextMenu}
      onDeleteAnnotation={props.deleteAnnotation}
      onUpdateAnnotation={props.updateAnnotation}
    />
  );
}

function ReaderDrawerPane(props: FrameProps) {
  return (
    <SessionDrawer
      open={Boolean(props.drawerTab)}
      onClose={() => props.setDrawerTab(null)}
      tab={props.drawerTab || 'insight'}
      onTabChange={props.setDrawerTab}
      annotations={props.annotations}
      courseId={props.courseId || ''}
      documentId={props.activeDocument?.document_id}
      pageNumber={props.pageNumber}
      onUpdate={props.updateAnnotation}
      onDelete={props.deleteAnnotation}
      onGoToPage={props.goToPage}
      practice={props.practice}
      unitKey={props.context?.unit_key}
      onRecorded={props.setPractice}
    />
  );
}

function ReaderRow(props: FrameProps) {
  return (
    <div {...stylex.props(styles.readerRow)}>
      <ReaderCardPane {...props} />
      <ReaderDrawerPane {...props} />
    </div>
  );
}

function ActiveSelectionToolbar({
  pendingSelection,
  saveAnnotation,
  setPendingSelection,
  command,
  onCommandHandled,
}: {
  pendingSelection: PendingSelection | null;
  saveAnnotation: (mark: PendingSelection) => Promise<void>;
  setPendingSelection: (value: PendingSelection | null) => void;
  command: 'note' | null;
  onCommandHandled: () => void;
}) {
  const selectionKey = pendingSelection
    ? `${pendingSelection.pageNumber}-${pendingSelection.char_start}-${pendingSelection.char_end}`
    : 'none';
  return (
    <SelectionToolbar
      key={selectionKey}
      pending={pendingSelection}
      onSave={saveAnnotation}
      onDismiss={() => setPendingSelection(null)}
      command={command}
      onCommandHandled={onCommandHandled}
    />
  );
}

function WorkspaceRevealButton({ onClick }: { onClick: () => void }) {
  return (
    <button
      type="button"
      {...stylex.props(styles.chromeReveal)}
      title="Show reader controls"
      aria-label="Show reader controls"
      onClick={onClick}
    >
      <PanelTop aria-hidden="true" {...stylex.props(styles.revealIcon)} />
      <span {...stylex.props(styles.revealText)}>Show controls</span>
    </button>
  );
}

function WorkspaceChrome(props: FrameProps) {
  const { chromeVisible, setChromeVisible } = props;
  if (!chromeVisible) return <WorkspaceRevealButton onClick={() => setChromeVisible(true)} />;
  return (
    <SessionToolbar
      context={props.context || { documents: [] }}
      action={props.context?.action || null}
      activeDocument={props.activeDocument}
      documents={props.context?.documents || []}
      documentIndex={props.documentIndex}
      onSelectDocument={props.setDocumentIndex}
      scale={props.scale}
      fitMode={props.fitMode}
      onZoomOut={() => props.pdfReaderRef.current?.zoomOut()}
      onZoomIn={() => props.pdfReaderRef.current?.zoomIn()}
      onFitPage={() => props.pdfReaderRef.current?.fitPage()}
      onFitWidth={() => props.pdfReaderRef.current?.fitToWidth()}
      pageNumber={props.pageNumber}
      pageCount={props.activeDocument?.page_count}
      onGoToPage={props.goToPage}
      measuredSeconds={props.measuredSeconds}
      idle={props.idle}
      annotationCount={props.visibleCount}
      drawerTab={props.drawerTab}
      onToggleDrawer={(tab) =>
        // SAFETY: toolbar only fires tab toggles for valid DrawerTab values;
        // the cast narrows string to DrawerTab.
        props.setDrawerTab((current) => (current === tab ? null : (tab as DrawerTab)))
      }
      onBookmark={() => props.onBookmark(props.pageNumber)}
      pageBookmarked={props.pageBookmarked}
      session={props.session}
      closing={props.closing}
      closed={props.closed}
      questionCount={props.questionCount}
      onFinish={props.finish}
      selectionAvailable={Boolean(props.pendingSelection)}
      onAddNote={() => props.setSelectionCommand('note')}
    />
  );
}

function WorkspaceOverlays({
  pendingSelection,
  saveAnnotation,
  setPendingSelection,
  selectionCommand,
  setSelectionCommand,
  readerMenu,
  pageNumber,
  activeDocument,
  chromeVisible,
  pageBookmarked,
  runReaderAction,
}: {
  pendingSelection: PendingSelection | null;
  saveAnnotation: (mark: PendingSelection) => Promise<void>;
  setPendingSelection: (value: PendingSelection | null) => void;
  selectionCommand: 'note' | null;
  setSelectionCommand: (value: 'note' | null) => void;
  readerMenu: ReaderMenu | null;
  pageNumber: number;
  activeDocument: SessionDocument | null;
  chromeVisible: boolean;
  pageBookmarked: boolean;
  runReaderAction: (actionName: ReaderActionName) => void;
}) {
  return (
    <>
      <ActiveSelectionToolbar
        pendingSelection={pendingSelection}
        saveAnnotation={saveAnnotation}
        setPendingSelection={setPendingSelection}
        command={selectionCommand}
        onCommandHandled={() => setSelectionCommand(null)}
      />
      <ReaderContextMenu
        open={Boolean(readerMenu)}
        x={readerMenu?.x || 0}
        y={readerMenu?.y || 0}
        pageNumber={readerMenu?.pageNumber || pageNumber}
        pageCount={activeDocument?.page_count}
        hasSelection={Boolean(pendingSelection)}
        chromeVisible={chromeVisible}
        bookmarked={pageBookmarked}
        onAction={runReaderAction}
      />
    </>
  );
}

function WorkspaceFrame(props: FrameProps) {
  return (
    <div {...stylex.props(styles.frame)}>
      <WorkspaceChrome {...props} />
      <ReaderRow {...props} />
    </div>
  );
}

function WorkspaceWriteError({ error, onDismiss }: { error: string; onDismiss: () => void }) {
  return (
    <div {...stylex.props(styles.writeError)} role="alert">
      <span>{error}</span>
      <button type="button" {...stylex.props(styles.dismissBtn)} onClick={onDismiss}>
        Dismiss
      </button>
    </div>
  );
}

function workspacePieces(props: WorkspaceProps) {
  return {
    annotations: props.annotations,
    activeDocument: props.activeDocument,
    pendingSelection: props.pendingSelection,
    setPendingSelection: props.setPendingSelection,
    pageNumber: props.pageNumber,
    saveAnnotation: props.saveAnnotation,
    selectionCommand: props.selectionCommand,
    setSelectionCommand: props.setSelectionCommand,
    readerMenu: props.readerMenu,
    chromeVisible: props.chromeVisible,
    pageBookmarked: props.pageBookmarked,
    runReaderAction: props.runReaderAction,
    annotationAnnouncement: props.annotationAnnouncement,
    error: props.error,
    setError: props.setError,
  };
}

function WorkspaceOverlaysPane(props: WorkspaceProps) {
  return (
    <WorkspaceOverlays
      pendingSelection={props.pendingSelection}
      saveAnnotation={props.saveAnnotation}
      setPendingSelection={props.setPendingSelection}
      selectionCommand={props.selectionCommand}
      setSelectionCommand={props.setSelectionCommand}
      readerMenu={props.readerMenu}
      pageNumber={props.pageNumber}
      activeDocument={props.activeDocument}
      chromeVisible={props.chromeVisible}
      pageBookmarked={props.pageBookmarked}
      runReaderAction={props.runReaderAction}
    />
  );
}

function WorkspaceBody(props: WorkspaceProps & { visibleCount?: number; selectDocument?: (index: number) => void }) {
  const { annotations, annotationAnnouncement, error, setError } = workspacePieces(props);

  const selectDocument = (index: number) => {
    props.setDocumentIndex(index);
    props.setPageNumber(1);
  };
  const visibleCount = annotations.filter((item) => item.status !== 'deleted').length;
  return (
    <>
      {error && <WorkspaceWriteError error={error} onDismiss={() => setError?.(null)} />}
      {annotationAnnouncement && (
        <span {...stylex.props(commonStyles.srOnly)} role="status" aria-live="polite">
          {annotationAnnouncement}
        </span>
      )}
      <WorkspaceFrame {...props} visibleCount={visibleCount} selectDocument={selectDocument} />
      <WorkspaceOverlaysPane {...props} />
    </>
  );
}

function Workspace(props: WorkspaceProps) {
  return <WorkspaceBody {...props} />;
}

export default Workspace;
