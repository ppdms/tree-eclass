import * as stylex from '@stylexjs/stylex';
import { ArrowLeft, FileText } from 'lucide-react';
import { Selector } from '@astryxdesign/core/Selector';
import { colors, spacing } from '@/styles/tokens.stylex';
import type { StudyAction } from '@/lib/types';
import type { SessionContext, SessionDocument, SessionInfo } from '@/features/session/reader/types';
import ToolbarActions from './ToolbarActions';
import ToolbarCenter from './SessionToolbarChrome';

const styles = stylex.create({
  toolbar: {
    gap: spacing.sm,
    paddingBlock: '0.375rem',
    paddingInline: '0.875rem',
    alignItems: 'center',
    backgroundColor: colors.surface,
    color: colors.textPrimary,
    display: 'flex',
    flexBasis: 'auto',
    flexGrow: 0,
    flexShrink: 0,
    flexWrap: 'nowrap',
    scrollbarWidth: 'none',
    borderBottomColor: colors.border,
    borderBottomWidth: '1px',
    minHeight: '3.25rem',
    minWidth: 0,
    overflowX: 'auto',
  },
  toolbarIdentity: {
    gap: '0.875rem',
    alignItems: 'center',
    display: 'flex',
    flexBasis: 'min(32vw, 28rem)',
    flexGrow: 0,
    flexShrink: 0,
    minWidth: 0,
  },
  documentIdentity: {
    gap: spacing.sm,
    alignItems: 'center',
    display: 'flex',
    maxWidth: 'min(25rem, 34vw)',
    minWidth: 0,
  },
  backToPlan: {
    textDecoration: 'none',
    alignItems: 'center',
    color: { default: colors.textSecondary, ':hover': colors.textPrimary },
    display: 'inline-flex',
    fontSize: '1.1rem',
    justifyContent: 'center',
    minHeight: '2.5rem',
    minWidth: '2.5rem',
  },
  icon: {
    height: '1rem',
    width: '1rem',
  },
  documentIcon: {
    borderColor: colors.borderLight,
    borderRadius: '0.625rem',
    borderWidth: '1px',
    placeItems: 'center',
    backgroundColor: colors.surface,
    color: colors.info,
    display: 'grid',
    flexBasis: 'auto',
    flexGrow: 0,
    flexShrink: 0,
    height: '1.9rem',
    width: '1.9rem',
  },
  documentCopy: {
    gap: '0.375rem',
    display: 'flex',
    flexDirection: 'column',
    minWidth: 0,
  },
  courseTitle: {
    margin: 0,
    overflow: 'hidden',
    color: colors.textSecondary,
    fontSize: '0.6875rem',
    letterSpacing: '0.02em',
    textOverflow: 'ellipsis',
    whiteSpace: 'nowrap',
  },
  documentTitle: {
    margin: 0,
    overflow: 'hidden',
    color: colors.textPrimary,
    fontSize: '0.8125rem',
    fontWeight: 650,
    letterSpacing: '0.015em',
    textOverflow: 'ellipsis',
    whiteSpace: 'nowrap',
  },
  singleDocLabel: {
    overflow: 'hidden',
    alignItems: 'center',
    color: colors.textSecondary,
    display: 'inline-flex',
    fontSize: '0.75rem',
    textOverflow: 'ellipsis',
    whiteSpace: 'nowrap',
    borderLeftColor: colors.border,
    borderLeftWidth: '1px',
    maxWidth: 'min(15rem, 20vw)',
    minWidth: 0,
    paddingLeft: '0.875rem',
  },
  docLabelName: {
    overflow: 'hidden',
    textOverflow: 'ellipsis',
    whiteSpace: 'nowrap',
  },
  docSelect: {
    alignItems: 'center',
    color: colors.textSecondary,
    display: 'inline-flex',
    fontSize: '0.75rem',
    height: '2.25rem',
    minWidth: 0,
    width: '14rem',
  },
});

function DocumentIdentity({
  context,
  activeDocument,
  showDocument,
}: {
  context: SessionContext;
  activeDocument: SessionDocument | null;
  showDocument: boolean;
}) {
  const courseName = context.course?.short_name || context.course?.name;
  const documentName = activeDocument?.display_name || 'Reading';
  return (
    <div {...stylex.props(styles.documentIdentity)}>
      <a {...stylex.props(styles.backToPlan)} href="/study" aria-label="Back to study plan" title="Back to study plan">
        <ArrowLeft aria-hidden="true" {...stylex.props(styles.icon)} />
      </a>
      <div {...stylex.props(styles.documentIcon)} aria-hidden="true">
        <FileText {...stylex.props(styles.icon)} />
      </div>
      <div {...stylex.props(styles.documentCopy)}>
        <h1 {...stylex.props(styles.courseTitle)} lang="el">
          {courseName || 'Study session'}
        </h1>
        {showDocument && (
          <p {...stylex.props(styles.documentTitle)} title={documentName}>
            {documentName}
          </p>
        )}
      </div>
    </div>
  );
}

function DocumentSwitcher({
  documents,
  documentIndex,
  onSelect,
}: {
  documents: SessionDocument[];
  documentIndex: number;
  onSelect: (index: number) => void;
}) {
  const current = documents[documentIndex];
  if (documents.length < 2) {
    return (
      <div {...stylex.props(styles.singleDocLabel)} title={current?.display_name || 'Reading'}>
        <span {...stylex.props(styles.docLabelName)}>{current?.display_name || 'Reading'}</span>
      </div>
    );
  }
  return (
    <Selector
      label="Choose document"
      isLabelHidden
      options={documents.map((doc, index) => ({
        value: String(index),
        label: doc.display_name,
      }))}
      value={String(documentIndex)}
      onChange={(value) => onSelect(Number(value))}
      variant="input"
      size="sm"
      {...stylex.props(styles.docSelect)}
    />
  );
}

function ToolbarIdentity(props: SessionToolbarProps) {
  return (
    <div {...stylex.props(styles.toolbarIdentity)}>
      <DocumentIdentity
        context={props.context}
        activeDocument={props.activeDocument}
        showDocument={props.documents.length < 2}
      />
      {props.documents.length > 1 ? (
        <DocumentSwitcher
          documents={props.documents}
          documentIndex={props.documentIndex}
          onSelect={props.onSelectDocument}
        />
      ) : null}
    </div>
  );
}

export interface SessionToolbarProps {
  context: SessionContext;
  action: StudyAction | null;
  activeDocument: SessionDocument | null;
  documents: SessionDocument[];
  documentIndex: number;
  onSelectDocument: (index: number) => void;
  scale: number | null;
  onZoomOut: () => void;
  onZoomIn: () => void;
  onFitPage: () => void;
  onFitWidth: () => void;
  fitMode: 'page' | 'width' | 'manual';
  pageNumber: number;
  pageCount?: number;
  onGoToPage: (page: number, behavior?: ScrollBehavior) => void;
  measuredSeconds: number;
  idle: boolean;
  annotationCount: number;
  drawerTab: string | null;
  onToggleDrawer: (tab: string) => void;
  onBookmark: (targetPage?: number) => void | Promise<void>;
  pageBookmarked: boolean;
  session: SessionInfo | null;
  closing: string | null;
  closed: SessionInfo | null;
  questionCount: number;
  onFinish: (outcome: string) => void;
  selectionAvailable: boolean;
  onAddNote: () => void;
}

export default function SessionToolbar(props: SessionToolbarProps) {
  return (
    <div {...stylex.props(styles.toolbar)} aria-label="Study session controls">
      <ToolbarIdentity {...props} />
      <ToolbarCenter
        scale={props.scale}
        onZoomOut={props.onZoomOut}
        onZoomIn={props.onZoomIn}
        onFitPage={props.onFitPage}
        onFitWidth={props.onFitWidth}
        fitMode={props.fitMode}
        pageNumber={props.pageNumber}
        pageCount={props.pageCount}
        onGoToPage={props.onGoToPage}
      />
      <ToolbarActions
        measuredSeconds={props.measuredSeconds}
        idle={props.idle}
        annotationCount={props.annotationCount}
        drawerTab={props.drawerTab}
        onToggleDrawer={props.onToggleDrawer}
        onBookmark={props.onBookmark}
        pageBookmarked={props.pageBookmarked}
        action={props.action}
        session={props.session}
        closing={props.closing}
        closed={props.closed}
        questionCount={props.questionCount}
        onFinish={props.onFinish}
        selectionAvailable={props.selectionAvailable}
        onAddNote={props.onAddNote}
      />
    </div>
  );
}
