import * as stylex from '@stylexjs/stylex';
import type { StyleXStyles } from '@stylexjs/stylex';
import { Bookmark, FilePenLine, Highlighter, MessageCircleQuestion, Trash2, type LucideIcon } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Textarea } from '@/components/ui/textarea';
import { commonStyles } from '@/styles/common';
import { colors, spacing, typography } from '@/styles/tokens.stylex';
import type { Annotation, AnnotationKind } from '@/features/session/reader/types';
import type { AnnotationUpdatePayload } from './api';
import { AnnotationListContent } from './AnnotationListContent';

export interface NotesPanelProps {
  annotations: Annotation[];
  onUpdate: (id: string | number, patch: AnnotationUpdatePayload) => Promise<Annotation | null>;
  onDelete: (id: string | number) => Promise<boolean>;
  onGoToPage?: (page: number) => void;
}

const KIND_LABEL = {
  highlight: 'Highlight',
  note: 'Note',
  question: 'Question',
  bookmark: 'Bookmark',
} satisfies Record<AnnotationKind, string>;

const styles = stylex.create({
  annotationList: {
    flexBasis: '0%',
    flexGrow: 1,
    flexShrink: 1,
    minBlockSize: 0,
    overflowY: 'auto',
  },
  sessionMark: {
    paddingBlock: '1rem',
    borderInlineStartStyle: 'solid',
    borderInlineStartWidth: 1,
    paddingInlineStart: '0.75rem',
  },
  sessionMarkHighlight: { borderInlineStartColor: colors.warning },
  sessionMarkNoteKind: { borderInlineStartColor: colors.info },
  sessionMarkQuestion: { borderInlineStartColor: colors.danger },
  sessionMarkBookmark: { borderInlineStartColor: colors.textSecondary },
  sessionMarkStale: { opacity: 0.68 },
  sessionMarkDeleted: { borderInlineStartStyle: 'dashed', opacity: 0.75 },
  header: {
    gap: '0.55rem',
    alignItems: 'center',
    display: 'flex',
    minHeight: '1.5rem',
  },
  pageBtn: {
    padding: 0,
    color: { default: colors.textSecondary, ':hover': colors.primary },
    fontSize: '0.68rem',
    fontWeight: typography.weightNormal,
    height: 'auto',
  },
  kindBadge: {
    gap: '0.3rem',
    alignItems: 'center',
    color: colors.textSecondary,
    display: 'inline-flex',
    fontSize: typography.size2xs,
    fontWeight: 650,
    letterSpacing: '0.04em',
    textTransform: 'uppercase',
  },
  kindBadgeQuestion: {
    color: colors.danger,
  },
  staleTag: {
    color: colors.warning,
    fontSize: typography.size2xs,
  },
  removeBtn: {
    padding: 0,
    color: { default: colors.textSecondary, ':hover': colors.danger },
    fontSize: '0.68rem',
    fontWeight: typography.weightNormal,
    height: 'auto',
    marginLeft: 'auto',
  },
  noteContent: {
    gap: '0.55rem',
    display: 'grid',
    marginTop: '0.55rem',
  },
  quote: {
    margin: 0,
    borderColor: colors.borderLight,
    borderRadius: '0.625rem',
    borderWidth: '1px',
    paddingBlock: '0.65rem',
    paddingInline: '0.75rem',
    backgroundColor: colors.surfaceRaised,
    color: colors.textPrimarySoft,
    fontSize: '0.8125rem',
    lineHeight: 1.55,
  },
  plainQuote: {
    margin: 0,
    color: colors.textPrimarySoft,
    fontSize: '0.8125rem',
    lineHeight: 1.55,
  },
  noteBody: {
    borderColor: colors.border,
    borderRadius: '0.625rem',
    borderWidth: '1px',
    paddingBlock: '0.65rem',
    paddingInline: '0.75rem',
    backgroundColor: colors.surfaceRaised,
  },
  noteLabel: {
    gap: '0.35rem',
    alignItems: 'center',
    color: colors.info,
    display: 'flex',
    fontSize: typography.size2xs,
    fontWeight: typography.weightBold,
    letterSpacing: '0.06em',
    textTransform: 'uppercase',
    marginBottom: '0.35rem',
  },
  noteBtn: {
    padding: 0,
    color: colors.textPrimarySoft,
    fontSize: typography.sizeXs,
    fontWeight: 400,
    justifyContent: 'flex-start',
    lineHeight: 1.55,
    textAlign: 'left',
    width: '100%',
  },
  editButtons: {
    gap: spacing.unit,
    display: 'flex',
    marginTop: '0.25rem',
  },
  editBtn: {
    paddingInline: '0.5rem',
    fontSize: '0.75rem',
    height: '1.75rem',
  },
});

function cleanQuote(value: string): string {
  return String(value || '')
    .replace(/\s+([μΜ])\s+/g, '$1')
    .replace(/\s+([»”])/g, '$1')
    .replace(/([«“])\s+/g, '$1')
    .replace(/\s{2,}/g, ' ')
    .trim();
}

const KIND_ICON = {
  highlight: Highlighter,
  note: FilePenLine,
  question: MessageCircleQuestion,
  bookmark: Bookmark,
} satisfies Record<AnnotationKind, LucideIcon>;

function AnnotationCardActions({
  item,
  label,
  onDelete,
}: {
  item: Annotation;
  label: string;
  onDelete: NotesPanelProps['onDelete'];
}) {
  return (
    <Button
      type="button"
      variant="ghost"
      size="icon"
      {...stylex.props(styles.removeBtn)}
      onClick={() => onDelete(item.id)}
      aria-label={`Delete ${label.toLowerCase()}`}
      title={`Delete ${label.toLowerCase()}`}
      icon={<Trash2 aria-hidden="true" />}
    />
  );
}

function AnnotationCardHeader({
  item,
  onGoToPage,
  onDelete,
}: {
  item: Annotation;
  onGoToPage: NotesPanelProps['onGoToPage'];
  onDelete: NotesPanelProps['onDelete'];
}) {
  const Icon = KIND_ICON[item.kind] || FilePenLine;
  const label = KIND_LABEL[item.kind] || item.kind;
  return (
    <div {...stylex.props(styles.header)}>
      <Button
        type="button"
        variant="ghost"
        size="sm"
        {...stylex.props(styles.pageBtn)}
        onClick={() => onGoToPage?.(item.page_number)}
      >
        p. {item.page_number}
      </Button>
      <span {...stylex.props(styles.kindBadge, item.kind === 'question' && styles.kindBadgeQuestion)}>
        <Icon aria-hidden="true" />
        {label}
      </span>
      {item.status === 'orphaned' && (
        <span {...stylex.props(styles.staleTag)} title="The document changed after this mark was made.">
          Older version
        </span>
      )}
      <AnnotationCardActions item={item} label={label} onDelete={onDelete} />
    </div>
  );
}

function EditingForm({
  value,
  onChange,
  onSave,
  onCancel,
}: {
  value: string;
  onChange: (value: string) => void;
  onSave: () => void;
  onCancel: () => void;
}) {
  return (
    <div>
      <label {...stylex.props(commonStyles.srOnly)} htmlFor="editing-note">
        Edit note
      </label>
      <Textarea id="editing-note" value={value} onChange={(event) => onChange(event.target.value)} rows={2} />
      <div {...stylex.props(styles.editButtons)}>
        <Button type="button" variant="outline" size="sm" style={styles.editBtn} onClick={onSave}>
          Save
        </Button>
        <Button type="button" variant="outline" size="sm" style={styles.editBtn} onClick={onCancel}>
          Cancel
        </Button>
      </div>
    </div>
  );
}

function BodyTextButton({
  body,
  onEdit,
  style,
}: {
  body: string | null | undefined;
  onEdit: () => void;
  style?: StyleXStyles;
}) {
  return (
    <Button type="button" variant="ghost" size="sm" style={[styles.noteBtn, style]} onClick={onEdit}>
      {body || <span>Add a note</span>}
    </Button>
  );
}

interface AnnotationBodyProps {
  item: Annotation;
  editing: boolean;
  editingBody: string;
  onEditingBodyChange: (value: string) => void;
  onStartEdit: () => void;
  onSaveEdit: () => void;
  onCancelEdit: () => void;
}

function NoteContent({ item, body }: { item: Annotation; body: React.ReactNode }) {
  return (
    <div {...stylex.props(styles.noteContent)}>
      {item.quote ? <blockquote {...stylex.props(styles.quote)}>“{cleanQuote(item.quote)}”</blockquote> : null}
      <div {...stylex.props(styles.noteBody)}>
        <span {...stylex.props(styles.noteLabel)}>
          <FilePenLine aria-hidden="true" /> Your note
        </span>
        {body}
      </div>
    </div>
  );
}

function AnnotationBody(props: AnnotationBodyProps) {
  const { item, editing, editingBody, onEditingBodyChange, onStartEdit, onSaveEdit, onCancelEdit } = props;
  const body = editing ? (
    <EditingForm value={editingBody} onChange={onEditingBodyChange} onSave={onSaveEdit} onCancel={onCancelEdit} />
  ) : (
    <BodyTextButton body={item.body} onEdit={onStartEdit} />
  );

  if (item.kind !== 'note') {
    return (
      <>
        {item.quote ? <p {...stylex.props(styles.plainQuote)}>“{cleanQuote(item.quote)}”</p> : null}
        {body}
      </>
    );
  }

  return <NoteContent item={item} body={body} />;
}

export interface AnnotationCardProps {
  item: Annotation;
  editing: boolean;
  editingBody: string;
  onEditingBodyChange: (value: string) => void;
  onStartEdit: () => void;
  onSaveEdit: () => void;
  onCancelEdit: () => void;
  onDelete: NotesPanelProps['onDelete'];
  onGoToPage: NotesPanelProps['onGoToPage'];
}

export function AnnotationCard(props: AnnotationCardProps) {
  const {
    item,
    editing,
    editingBody,
    onEditingBodyChange,
    onStartEdit,
    onSaveEdit,
    onCancelEdit,
    onDelete,
    onGoToPage,
  } = props;
  return (
    <article
      {...stylex.props(
        styles.sessionMark,
        item.kind === 'highlight' && styles.sessionMarkHighlight,
        item.kind === 'note' && styles.sessionMarkNoteKind,
        item.kind === 'question' && styles.sessionMarkQuestion,
        item.kind === 'bookmark' && styles.sessionMarkBookmark,
        item.status === 'orphaned' && styles.sessionMarkStale,
        item.status === 'deleted' && styles.sessionMarkDeleted,
      )}
    >
      <AnnotationCardHeader item={item} onGoToPage={onGoToPage} onDelete={onDelete} />
      <AnnotationBody
        item={item}
        editing={editing}
        editingBody={editingBody}
        onEditingBodyChange={onEditingBodyChange}
        onStartEdit={onStartEdit}
        onSaveEdit={onSaveEdit}
        onCancelEdit={onCancelEdit}
      />
    </article>
  );
}

export interface AnnotationListProps {
  annotations: Annotation[];
  editing: string | number | null;
  editingBody: string;
  onEditingBodyChange: (value: string) => void;
  onStartEdit: (item: Annotation) => void;
  onSaveEdit: (id: string | number) => void;
  onCancelEdit: () => void;
  onDelete: NotesPanelProps['onDelete'];
  onGoToPage: NotesPanelProps['onGoToPage'];
}

export function AnnotationList(props: AnnotationListProps) {
  return (
    <div {...stylex.props(styles.annotationList)}>
      <AnnotationListContent {...props} />
    </div>
  );
}
