import * as stylex from '@stylexjs/stylex';
import * as React from 'react';
import { createPortal } from 'react-dom';
import { X } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { colors, effects, typography } from '@/styles/tokens.stylex';

const styles = stylex.create({
  studyPlanBackdrop: {
    inset: 0,
    backdropFilter: 'blur(0.2rem)',
    backgroundColor: 'rgb(9 9 11 / 0.7)',
    display: 'flex',
    justifyContent: 'flex-end',
    position: 'fixed',
    zIndex: 1000,
  },
  studyPlanSheet: {
    backgroundColor: colors.background,
    boxShadow: effects.shadowLarge,
    display: 'flex',
    flexDirection: 'column',
    borderLeftColor: colors.borderLight,
    borderLeftStyle: 'solid',
    borderLeftWidth: 1,
    height: '100%',
    maxWidth: '100%',
    width: 'min(52rem, 100%)',
  },
  studyPlanSheetHead: {
    paddingBlock: '1.1rem',
    paddingInline: '1.25rem',
    alignItems: 'center',
    display: 'flex',
    justifyContent: 'space-between',
    borderBottomColor: colors.border,
    borderBottomStyle: 'solid',
    borderBottomWidth: 1,
  },
  studyPlanKicker: {
    color: colors.textSecondary,
    fontSize: typography.sizeXs,
    fontWeight: 650,
    letterSpacing: '0.1em',
    textTransform: 'uppercase',
  },
  studyPlanTitle: {
    marginInline: 0,
    color: colors.textPrimary,
    fontSize: '1.35rem',
    letterSpacing: '-0.03em',
    marginBlockEnd: 0,
    marginBlockStart: '0.15rem',
  },
});

interface PlannerSheetProps {
  open: boolean;
  onClose: () => void;
  children: React.ReactNode;
}

function useSheet(open: boolean, onClose: () => void) {
  const closeRef = React.useRef<HTMLButtonElement>(null);
  React.useEffect(() => {
    if (!open) return undefined;
    const previousOverflow = document.body.style.overflow;
    document.body.style.overflow = 'hidden';
    closeRef.current?.focus();
    const closeOnEscape = (event: KeyboardEvent) => {
      if (event.key === 'Escape') onClose();
    };
    window.addEventListener('keydown', closeOnEscape);
    return () => {
      document.body.style.overflow = previousOverflow;
      window.removeEventListener('keydown', closeOnEscape);
    };
  }, [open, onClose]);
  return closeRef;
}

function SheetPanel({
  children,
  onClose,
  closeRef,
}: {
  children: React.ReactNode;
  onClose: () => void;
  closeRef: React.RefObject<HTMLButtonElement | null>;
}) {
  return (
    <section {...stylex.props(styles.studyPlanSheet)} role="dialog" aria-modal="true" aria-labelledby="plan-title">
      <header {...stylex.props(styles.studyPlanSheetHead)}>
        <div>
          <span {...stylex.props(styles.studyPlanKicker)}>Settings</span>
          <h2 id="plan-title" {...stylex.props(styles.studyPlanTitle)}>
            Study plan
          </h2>
        </div>
        <Button
          ref={closeRef}
          type="button"
          variant="ghost"
          size="icon"
          onClick={onClose}
          aria-label="Close study plan"
          icon={<X aria-hidden="true" />}
        />
      </header>
      {children}
    </section>
  );
}

export function PlannerSheet({ open, onClose, children }: PlannerSheetProps) {
  const closeRef = useSheet(open, onClose);
  if (!open) return null;
  return createPortal(
    <div
      {...stylex.props(styles.studyPlanBackdrop)}
      role="presentation"
      onMouseDown={(event) => {
        if (event.target === event.currentTarget) onClose();
      }}
    >
      <SheetPanel onClose={onClose} closeRef={closeRef}>
        {children}
      </SheetPanel>
    </div>,
    document.body,
  );
}
