import * as React from 'react';
import { Dialog } from '@astryxdesign/core/Dialog';
import { Button } from '@/components/ui/button';
import * as stylex from '@stylexjs/stylex';
import { colors } from '@/styles/tokens.stylex';

const styles = stylex.create({
  actions: {
    gap: '1rem',
    display: 'flex',
    justifyContent: 'flex-end',
    marginBlockStart: '1.25rem',
  },
  body: { padding: '1.25rem', width: '100%' },
  description: { margin: 0, color: colors.textSecondary, lineHeight: 1.55 },
  title: { margin: 0, fontSize: '1.125rem', marginBlockEnd: '.45rem' },
});

export interface ConfirmDialogProps {
  open: boolean;
  title: string;
  description: string;
  confirmLabel?: string;
  cancelLabel?: string;
  danger?: boolean;
  onConfirm: () => void;
  onCancel?: () => void;
}

export function ConfirmDialog({
  open,
  title,
  description,
  confirmLabel = 'Confirm',
  cancelLabel = 'Cancel',
  danger = false,
  onConfirm,
  onCancel,
}: ConfirmDialogProps) {
  return (
    <Dialog
      isOpen={open}
      onOpenChange={(nextOpen) => {
        if (!nextOpen) onCancel?.();
      }}
      purpose="info"
      width="min(30rem, calc(100vw - 2rem))"
      aria-label={title}
    >
      <div {...stylex.props(styles.body)}>
        <h2 {...stylex.props(styles.title)}>{title}</h2>
        <p {...stylex.props(styles.description)}>{description}</p>
        <div {...stylex.props(styles.actions)}>
          <Button type="button" variant="outline" onClick={onCancel}>
            {cancelLabel}
          </Button>
          <Button type="button" variant={danger ? 'destructive' : 'default'} onClick={onConfirm}>
            {confirmLabel}
          </Button>
        </div>
      </div>
    </Dialog>
  );
}
