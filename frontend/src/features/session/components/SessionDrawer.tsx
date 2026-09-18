import * as stylex from '@stylexjs/stylex';
import { useEffect, useRef } from 'react';
import { Lightbulb, NotebookText, Brain, X, type LucideIcon } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { layers } from '@/styles/constants.stylex';
import { media } from '@/styles/constants.stylex';
import { colors, effects, typography } from '@/styles/tokens.stylex';
import type { DrawerTab } from './useSessionState';
import ContextPanel, { type ContextPanelProps } from './ContextPanel';

const TAB_TITLES = {
  insight: ['Page insight', Lightbulb],
  notes: ['Marks & notes', NotebookText],
  recall: ['Recall', Brain],
} satisfies Record<DrawerTab, [string, LucideIcon]>;

const styles = stylex.create({
  drawer: {
    overflow: 'hidden',
    backgroundColor: colors.surface,
    boxShadow: effects.shadowLarge,
    color: colors.textPrimary,
    display: 'flex',
    flexBasis: 'auto',
    flexDirection: 'column',
    flexGrow: 0,
    flexShrink: 0,
    position: {
      default: 'relative',
      [media.tablet]: 'absolute',
    },
    zIndex: {
      default: 'auto',
      [media.tablet]: 1005,
    },
    borderLeftColor: colors.border,
    borderLeftWidth: '1px',
    bottom: {
      default: 'auto',
      [media.tablet]: 0,
    },
    height: '100%',
    minWidth: 0,
    right: {
      default: 'auto',
      [media.tablet]: 0,
    },
    top: {
      default: 'auto',
      [media.tablet]: 0,
    },
    width: {
      default: 'min(26rem, 30vw)',
      [media.tablet]: 'min(100%, 25rem)',
    },
  },
  scrim: {
    inset: 0,
    borderWidth: 0,
    backgroundColor: 'rgb(0 0 0 / 0.5)',
    display: {
      default: 'none',
      [media.tablet]: 'block',
    },
    position: 'absolute',
    zIndex: layers.overlay,
  },
  header: {
    paddingBlock: '0.75rem',
    paddingInline: '1rem',
    alignItems: 'center',
    display: 'flex',
    flexBasis: 'auto',
    flexGrow: 0,
    flexShrink: 0,
    justifyContent: 'space-between',
    borderBottomColor: colors.border,
    borderBottomWidth: '1px',
    minHeight: '4.25rem',
  },
  headerInfo: {
    gap: '0.5rem',
    alignItems: 'center',
    display: 'flex',
    minWidth: 0,
  },
  headerText: {
    minWidth: 0,
  },
  title: {
    overflow: 'hidden',
    fontSize: typography.sizeSm,
    fontWeight: typography.weightSemibold,
    textOverflow: 'ellipsis',
    whiteSpace: 'nowrap',
  },
  subtitle: {
    overflow: 'hidden',
    color: colors.textSecondary,
    fontSize: '0.68rem',
    textOverflow: 'ellipsis',
    whiteSpace: 'nowrap',
  },
  closeBtn: {
    padding: 0,
    borderRadius: '0.375rem',
    color: colors.textSecondary,
    height: '2.75rem',
    width: '2.75rem',
  },
});

function useDrawerFocusTrap(open: boolean, onClose: () => void) {
  const closeRef = useRef<HTMLButtonElement>(null);
  const drawerRef = useRef<HTMLElement>(null);
  const returnFocusRef = useRef<HTMLElement | null>(null);
  useEffect(() => {
    if (!open) return undefined;
    // SAFETY: the drawer is open, so the active element is a DOM element
    // inside the page; the cast narrows the generic Element for focus().
    returnFocusRef.current = document.activeElement as HTMLElement;
    requestAnimationFrame(() => closeRef.current?.focus());
    const trap = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        event.preventDefault();
        onClose();
        return;
      }
      if (event.key !== 'Tab') return;
      const nodes = [
        ...(drawerRef.current?.querySelectorAll<HTMLElement>(
          'button, a, input, select, textarea, [tabindex]:not([tabindex="-1"])',
        ) || []),
      ].filter(
        (node) =>
          !(
            // SAFETY: the query selector above only matches form controls
            // and tabbable elements, all of which carry a disabled property.
            'disabled' in node &&
            (node as HTMLButtonElement | HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement).disabled
          ),
      );
      if (!nodes.length) return;
      const first = nodes[0]!;
      const last = nodes[nodes.length - 1]!;
      if (event.shiftKey && document.activeElement === first) {
        event.preventDefault();
        last.focus();
      } else if (!event.shiftKey && document.activeElement === last) {
        event.preventDefault();
        first.focus();
      }
    };
    window.addEventListener('keydown', trap);
    return () => {
      window.removeEventListener('keydown', trap);
      returnFocusRef.current?.focus?.();
    };
  }, [open, onClose]);
  return { closeRef, drawerRef };
}

interface SessionDrawerHeaderProps {
  onClose: () => void;
  closeRef: React.RefObject<HTMLButtonElement | null>;
  title: string;
  Icon: LucideIcon;
  pageNumber: number;
  markCount: number;
}

function SessionDrawerHeader({ onClose, closeRef, title, Icon, pageNumber, markCount }: SessionDrawerHeaderProps) {
  return (
    <div {...stylex.props(styles.header)}>
      <div {...stylex.props(styles.headerInfo)}>
        <Icon aria-hidden="true" />
        <div {...stylex.props(styles.headerText)}>
          <h2 id="session-drawer-title" {...stylex.props(styles.title)}>
            {title}
          </h2>
          <p {...stylex.props(styles.subtitle)}>
            Page {pageNumber} · {markCount} marks saved
          </p>
        </div>
      </div>
      <Button
        ref={closeRef}
        type="button"
        variant="ghost"
        size="icon"
        {...stylex.props(styles.closeBtn)}
        aria-label="Close panel"
        title="Close panel"
        onClick={onClose}
        icon={<X aria-hidden="true" />}
      />
    </div>
  );
}

export interface SessionDrawerProps extends ContextPanelProps {
  open: boolean;
  onClose: () => void;
}

export default function SessionDrawer({ open, onClose, ...contextPanelProps }: SessionDrawerProps) {
  const { closeRef, drawerRef } = useDrawerFocusTrap(open, onClose);
  if (!open) return null;
  const tabTitle = TAB_TITLES[contextPanelProps.tab] ?? TAB_TITLES.insight;
  const [title, Icon] = tabTitle;
  const markCount = contextPanelProps.annotations?.filter((item) => item.status !== 'deleted').length || 0;
  return (
    <>
      <button type="button" {...stylex.props(styles.scrim)} aria-label="Close side panel" onClick={onClose} />
      <aside ref={drawerRef} {...stylex.props(styles.drawer)} role="dialog" aria-labelledby="session-drawer-title">
        <SessionDrawerHeader
          onClose={onClose}
          closeRef={closeRef}
          title={title}
          Icon={Icon}
          pageNumber={contextPanelProps.pageNumber}
          markCount={markCount}
        />
        <ContextPanel {...contextPanelProps} />
      </aside>
    </>
  );
}
