import * as stylex from '@stylexjs/stylex';
import { useEffect, useState, type ReactNode } from 'react';
import { CircleHelp, Highlighter, MessageSquareText, X } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Textarea } from '@/components/ui/textarea';
import { lookup } from '@/lib/display';
import { HIGHLIGHT_COLORS } from '@/features/session/reader/types';
import { commonStyles } from '@/styles/common';
import { layers } from '@/styles/constants.stylex';
import { colors, effects, spacing, typography } from '@/styles/tokens.stylex';
import type { SelectionScreenRect } from '@/features/session/reader/PdfReaderHelpers';
import type { MarkInput, PendingSelection } from './useSessionActions';

const COLORS = Object.keys(HIGHLIGHT_COLORS);

const styles = stylex.create({
  popover: {
    padding: '0.35rem',
    borderColor: colors.borderLight,
    borderRadius: '0.75rem',
    borderWidth: '1px',
    backgroundColor: colors.background,
    boxShadow: effects.shadowLarge,
    position: 'fixed',
    zIndex: layers.popover,
    maxWidth: 'calc(100vw - 1.5rem)',
    minWidth: 0,
  },
  toolbarPosition: (left: string, top: string) => ({ left, top }),
  content: {
    gap: spacing.unit,
    alignItems: 'center',
    display: 'flex',
    flexWrap: 'wrap',
  },
  label: {
    gap: spacing.unit,
    paddingInline: '0.25rem',
    alignItems: 'center',
    color: colors.textSecondary,
    display: 'inline-flex',
    fontSize: '0.7rem',
    fontWeight: typography.weightSemibold,
  },
  labelIcon: {
    color: colors.warning,
    height: '1rem',
    width: '1rem',
  },
  swatch: {
    borderColor: colors.border,
    borderRadius: '999px',
    borderWidth: '1px',
    boxShadow: 'inset 0 0 0 1px rgb(0 0 0 / 0.1)',
    cursor: 'pointer',
    transform: { default: null, ':hover': 'scale(1.12)' },
    transitionDuration: '0.15s',
    transitionProperty: 'transform, scale',
    height: '2rem',
    width: '2rem',
  },
  swatchColor: (background: string) => ({ backgroundColor: background }),
  divider: {
    marginInline: '0.25rem',
    backgroundColor: colors.border,
    height: '1rem',
    width: '1px',
  },
  actionBtn: {
    gap: '0.375rem',
    paddingInline: '0.5rem',
    fontSize: '0.75rem',
    height: '2rem',
  },
  closeBtn: {
    height: '2rem',
    width: '2rem',
  },
  noteForm: {
    gap: '0.5rem',
    display: 'flex',
    flexDirection: 'column',
    width: 'min(17rem, calc(100vw - 2.5rem))',
  },
  noteTextarea: {
    minHeight: '4.5rem',
  },
  noteActions: {
    gap: spacing.unit,
    display: 'flex',
    justifyContent: 'flex-end',
  },
});

function ColorSwatches({ onPick }: { onPick: (color: string) => void }) {
  return COLORS.map((color) => (
    <button
      key={color}
      type="button"
      aria-label={`Highlight ${color}`}
      title={`Highlight ${color}`}
      onClick={() => onPick(color)}
      {...stylex.props(styles.swatch, styles.swatchColor(lookup(HIGHLIGHT_COLORS, color, HIGHLIGHT_COLORS.yellow)))}
    />
  ));
}

function NoteForm({
  draft,
  onDraftChange,
  onSave,
  onCancel,
}: {
  draft: string;
  onDraftChange: (value: string) => void;
  onSave: (body: string | null) => void;
  onCancel: () => void;
}) {
  return (
    <div {...stylex.props(styles.noteForm)}>
      <label {...stylex.props(commonStyles.srOnly)} htmlFor="selection-note">
        Note for selected text
      </label>
      <Textarea
        id="selection-note"
        autoFocus
        value={draft}
        onChange={(event) => onDraftChange(event.target.value)}
        rows={2}
        {...stylex.props(styles.noteTextarea)}
        placeholder="Write a note…"
      />
      <div {...stylex.props(styles.noteActions)}>
        <Button type="button" variant="ghost" size="sm" onClick={onCancel}>
          Cancel
        </Button>
        <Button type="button" size="sm" onClick={() => onSave(draft.trim() || null)}>
          Save note
        </Button>
      </div>
    </div>
  );
}

interface QuickActionsProps {
  onColor: (color: string) => void;
  onNote: () => void;
  onQuestion: () => void;
  onDismiss: () => void;
}

function QuickActions({ onColor, onNote, onQuestion, onDismiss }: QuickActionsProps) {
  return (
    <div {...stylex.props(styles.content)}>
      <SelectionToolbarLabel />
      <ColorSwatches onPick={onColor} />
      <SelectionToolbarActions onNote={onNote} onQuestion={onQuestion} onDismiss={onDismiss} />
    </div>
  );
}

function SelectionToolbarLabel() {
  return (
    <span {...stylex.props(styles.label)}>
      <Highlighter aria-hidden="true" {...stylex.props(styles.labelIcon)} /> Highlight
    </span>
  );
}

function SelectionToolbarActions({
  onNote,
  onQuestion,
  onDismiss,
}: Pick<QuickActionsProps, 'onNote' | 'onQuestion' | 'onDismiss'>) {
  return (
    <>
      <span {...stylex.props(styles.divider)} />
      <Button
        type="button"
        variant="ghost"
        size="sm"
        icon={<MessageSquareText aria-hidden="true" />}
        {...stylex.props(styles.actionBtn)}
        onClick={onNote}
        aria-label="Add note to selection"
        title="Add note to selection"
      >
        Add note
      </Button>
      <Button
        type="button"
        variant="ghost"
        size="sm"
        icon={<CircleHelp aria-hidden="true" />}
        {...stylex.props(styles.actionBtn)}
        onClick={onQuestion}
      >
        Didn’t follow
      </Button>
      <Button
        type="button"
        variant="ghost"
        size="icon"
        icon={<X aria-hidden="true" />}
        {...stylex.props(styles.closeBtn)}
        aria-label="Dismiss annotation toolbar"
        onClick={onDismiss}
      />
    </>
  );
}

interface ToolbarPosition {
  left: number;
  top: number;
}

function toolbarPosition(rect: SelectionScreenRect, noting: boolean): ToolbarPosition {
  const width = noting ? 272 : 360;
  const height = noting ? 148 : 54;
  const left = Math.max(12, Math.min(window.innerWidth - width - 12, rect.left + rect.width / 2 - width / 2));
  const above = rect.top - height - 6;
  const below = rect.bottom + 8;
  const top = above >= 72 ? above : Math.min(window.innerHeight - height - 12, below);
  return { left, top: Math.max(72, top) };
}

function useCommandSync(
  command: 'note' | null,
  onCommandHandled: (() => void) | undefined,
  setNoting: (value: boolean) => void,
): void {
  useEffect(() => {
    if (!command) return;
    setNoting(command === 'note');
    onCommandHandled?.();
  }, [command, onCommandHandled, setNoting]);
}

function useDismissOnEscape(noting: boolean, onDismiss: () => void, setNoting: (value: boolean) => void): void {
  useEffect(() => {
    const closeOnEscape = (event: KeyboardEvent) => {
      if (event.key !== 'Escape') return;
      if (noting) setNoting(false);
      else onDismiss();
    };
    window.addEventListener('keydown', closeOnEscape);
    return () => window.removeEventListener('keydown', closeOnEscape);
  }, [noting, onDismiss, setNoting]);
}

export interface SelectionToolbarProps {
  pending: PendingSelection | null;
  onSave: (mark: MarkInput) => Promise<void>;
  onDismiss: () => void;
  command: 'note' | null;
  onCommandHandled?: () => void;
}

function SelectionToolbarFrame({ content, position }: { content: ReactNode; position: ToolbarPosition }) {
  return (
    <div
      {...stylex.props(styles.popover, styles.toolbarPosition(`${position.left}px`, `${position.top}px`))}
      role="group"
      aria-label="Annotation options"
      onPointerDown={(event) => event.stopPropagation()}
    >
      {content}
    </div>
  );
}

function SelectionToolbarContent({
  noting,
  draft,
  setDraft,
  setNoting,
  save,
  onDismiss,
  position,
}: {
  noting: boolean;
  draft: string;
  setDraft: (value: string) => void;
  setNoting: (value: boolean) => void;
  save: (kind: string, color: string, body: string | null) => void;
  onDismiss: () => void;
  position: ToolbarPosition;
}) {
  const content = noting ? (
    <NoteForm
      draft={draft}
      onDraftChange={setDraft}
      onSave={(body) => {
        save('note', 'yellow', body);
        setDraft('');
      }}
      onCancel={() => setNoting(false)}
    />
  ) : (
    <QuickActions
      onColor={(color) => save('highlight', color, null)}
      onNote={() => setNoting(true)}
      onQuestion={() => save('question', 'yellow', null)}
      onDismiss={onDismiss}
    />
  );
  return <SelectionToolbarFrame content={content} position={position} />;
}

export default function SelectionToolbar({
  pending,
  onSave,
  onDismiss,
  command,
  onCommandHandled,
}: SelectionToolbarProps) {
  const [noting, setNoting] = useState(false);
  const [draft, setDraft] = useState('');

  useCommandSync(command, onCommandHandled, setNoting);
  useDismissOnEscape(noting, onDismiss, setNoting);

  if (!pending) return null;
  const save = (kind: string, color: string, body: string | null) =>
    onSave({ ...pending, kind, color: color || 'yellow', body: body || null });
  if (!pending.screenRect) return null;
  const position = toolbarPosition(pending.screenRect, noting);

  return (
    <SelectionToolbarContent
      noting={noting}
      draft={draft}
      setDraft={setDraft}
      setNoting={setNoting}
      save={save}
      onDismiss={onDismiss}
      position={position}
    />
  );
}
