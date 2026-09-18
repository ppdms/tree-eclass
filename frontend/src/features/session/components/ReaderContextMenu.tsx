import * as stylex from '@stylexjs/stylex';
import { useLayoutEffect, useRef, useState } from 'react';
import { Bookmark, Copy, Highlighter, MessageSquareText, PanelTop, type LucideIcon } from 'lucide-react';
import { colors, effects, spacing, typography } from '@/styles/tokens.stylex';
import type { ReaderActionName } from './useReaderControls';

const MENU_WIDTH = 248;
const MENU_MARGIN = 12;

interface MenuPosition {
  left: number;
  top: number;
}

function clampPosition(x: number, y: number, height: number): MenuPosition {
  const left = Math.max(MENU_MARGIN, Math.min(x, window.innerWidth - MENU_WIDTH - MENU_MARGIN));
  const top = Math.max(MENU_MARGIN, Math.min(y, window.innerHeight - height - MENU_MARGIN));
  return { left, top };
}

const styles = stylex.create({
  menu: {
    padding: '0.35rem',
    borderColor: colors.borderLight,
    borderRadius: '0.75rem',
    borderWidth: '1px',
    backgroundColor: colors.background,
    boxShadow: effects.shadowLarge,
    position: 'fixed',
    zIndex: 70,
    maxHeight: 'calc(100vh - 1.5rem)',
    overflowY: 'auto',
    width: 'min(15.5rem, calc(100vw - 1.5rem))',
  },
  menuPosition: (left: string, top: string) => ({ left, top }),
  menuHeading: {
    gap: '0.75rem',
    paddingInline: '0.55rem',
    alignItems: 'baseline',
    display: 'flex',
    justifyContent: 'space-between',
    paddingBlockEnd: '0.55rem',
    paddingBlockStart: '0.45rem',
  },
  headingRow: {
    gap: spacing.unit,
    alignItems: 'baseline',
    display: 'flex',
  },
  pageTitle: {
    color: colors.textPrimary,
    fontSize: '0.75rem',
  },
  pageCount: {
    color: colors.textSecondary,
    fontSize: '0.625rem',
  },
  menuGroup: {
    borderTopColor: colors.border,
    borderTopWidth: '1px',
    paddingTop: '0.3rem',
  },
  selectionSection: {
    borderTopColor: colors.border,
    borderTopWidth: '1px',
    marginTop: '0.3rem',
    paddingTop: '0.3rem',
  },
  selectionLabel: {
    marginInline: '0.55rem',
    color: colors.textSecondary,
    fontSize: '0.625rem',
    fontWeight: typography.weightBold,
    letterSpacing: '0.08em',
    marginBlockEnd: '0.2rem',
    marginBlockStart: '0.35rem',
    textTransform: 'uppercase',
  },
  menuItem: {
    borderRadius: '0.375rem',
    borderWidth: 0,
    gap: spacing.sm,
    outline: { default: 'none', ':focus-visible': `2px solid ${colors.focusRing}` },
    paddingBlock: '0.375rem',
    paddingInline: '0.625rem',
    alignItems: 'center',
    backgroundColor: { default: 'transparent', ':hover': colors.surfaceHover },
    color: { default: colors.textPrimarySoft, ':hover': colors.textPrimary },
    display: 'flex',
    fontSize: '0.75rem',
    outlineOffset: { default: 0, ':focus-visible': '2px' },
    textAlign: 'left',
    minHeight: '2rem',
    width: '100%',
  },
  menuItemIcon: {
    color: colors.textSecondary,
    flexShrink: 0,
    height: '1rem',
    width: '1rem',
  },
});

function MenuGroup({
  actions,
  onAction,
}: {
  actions: [ReaderActionName, string, LucideIcon][];
  onAction: (action: ReaderActionName) => void;
}) {
  return actions.map(([value, label, Icon]) => (
    <button
      key={value}
      type="button"
      role="menuitem"
      {...stylex.props(styles.menuItem)}
      onClick={() => onAction(value)}
    >
      <Icon aria-hidden="true" {...stylex.props(styles.menuItemIcon)} />
      <span>{label}</span>
    </button>
  ));
}

function SelectionActions({ onAction }: { onAction: (action: ReaderActionName) => void }) {
  return (
    <div {...stylex.props(styles.selectionSection)}>
      <p {...stylex.props(styles.selectionLabel)}>Selection</p>
      <MenuGroup
        actions={[
          ['highlight', 'Highlight selection', Highlighter],
          ['note', 'Add note to selection', MessageSquareText],
          ['question', 'Mark as didn’t follow', MessageSquareText],
          ['copy', 'Copy selection', Copy],
        ]}
        onAction={onAction}
      />
    </div>
  );
}

function InterfaceActions({
  chromeVisible,
  onAction,
}: {
  chromeVisible: boolean;
  onAction: (action: ReaderActionName) => void;
}) {
  return (
    <div {...stylex.props(styles.menuGroup)}>
      <MenuGroup
        actions={[['toggleChrome', chromeVisible ? 'Hide controls' : 'Show controls', PanelTop]]}
        onAction={onAction}
      />
    </div>
  );
}

function useMenuKeyboard(menuRef: React.RefObject<HTMLDivElement | null>, open: boolean): void {
  useLayoutEffect(() => {
    if (!open) return undefined;
    const menu = menuRef.current;
    const items = () => [...(menu?.querySelectorAll<HTMLElement>('[role="menuitem"]') || [])];
    const onKeyDown = (event: KeyboardEvent) => {
      const controls = items();
      // SAFETY: the menu is open, so the active element is a DOM element
      // inside the page; the cast narrows the generic Element for indexOf.
      const current = controls.indexOf(document.activeElement as HTMLElement);
      if (event.key === 'ArrowDown' || event.key === 'ArrowUp') {
        event.preventDefault();
        const direction = event.key === 'ArrowDown' ? 1 : -1;
        controls[(current + direction + controls.length) % controls.length]?.focus();
      }
      if (event.key === 'Home' || event.key === 'End') {
        event.preventDefault();
        controls[event.key === 'Home' ? 0 : controls.length - 1]?.focus();
      }
    };
    menu?.addEventListener('keydown', onKeyDown);
    return () => menu?.removeEventListener('keydown', onKeyDown);
  }, [menuRef, open]);
}

export interface ReaderContextMenuProps {
  open: boolean;
  x: number;
  y: number;
  pageNumber: number;
  pageCount?: number;
  hasSelection: boolean;
  chromeVisible: boolean;
  bookmarked: boolean;
  onAction: (action: ReaderActionName) => void;
}

function ReaderContextMenuBody({
  pageNumber,
  pageCount,
  hasSelection,
  pageActions,
  chromeVisible,
  onAction,
}: {
  pageNumber: number;
  pageCount?: number;
  hasSelection: boolean;
  pageActions: [ReaderActionName, string, LucideIcon][];
  chromeVisible: boolean;
  onAction: (action: ReaderActionName) => void;
}) {
  return (
    <>
      <div {...stylex.props(styles.menuHeading)}>
        <div {...stylex.props(styles.headingRow)}>
          <strong {...stylex.props(styles.pageTitle)}>Page {pageNumber}</strong>
          <span {...stylex.props(styles.pageCount)}>of {pageCount || '—'}</span>
        </div>
      </div>
      {hasSelection ? <SelectionActions onAction={onAction} /> : null}
      <div {...stylex.props(styles.menuGroup)}>
        <MenuGroup actions={pageActions} onAction={onAction} />
      </div>
      <InterfaceActions chromeVisible={chromeVisible} onAction={onAction} />
    </>
  );
}

function ReaderContextMenuFrame({
  menuRef,
  position,
  children,
}: {
  menuRef: React.RefObject<HTMLDivElement | null>;
  position: { left: number; top: number };
  children: React.ReactNode;
}) {
  return (
    <div
      ref={menuRef}
      {...stylex.props(styles.menu, styles.menuPosition(`${position.left}px`, `${position.top}px`))}
      role="menu"
      aria-label="Reader options"
      onPointerDown={(event) => event.stopPropagation()}
    >
      {children}
    </div>
  );
}

function ReaderContextMenuSurface({
  menuRef,
  pageNumber,
  pageCount,
  hasSelection,
  pageActions,
  chromeVisible,
  onAction,
  position,
}: {
  menuRef: React.RefObject<HTMLDivElement | null>;
  pageNumber: number;
  pageCount?: number;
  hasSelection: boolean;
  pageActions: [ReaderActionName, string, LucideIcon][];
  chromeVisible: boolean;
  onAction: (action: ReaderActionName) => void;
  position: { left: number; top: number };
}) {
  return (
    <ReaderContextMenuFrame menuRef={menuRef} position={position}>
      <ReaderContextMenuBody
        pageNumber={pageNumber}
        pageCount={pageCount}
        hasSelection={hasSelection}
        pageActions={pageActions}
        chromeVisible={chromeVisible}
        onAction={onAction}
      />
    </ReaderContextMenuFrame>
  );
}

export default function ReaderContextMenu({
  open,
  x,
  y,
  pageNumber,
  pageCount,
  hasSelection,
  chromeVisible,
  bookmarked,
  onAction,
}: ReaderContextMenuProps) {
  const menuRef = useRef<HTMLDivElement>(null);
  const [position, setPosition] = useState({ left: 0, top: 0 });
  useMenuKeyboard(menuRef, open);
  useLayoutEffect(() => {
    if (!open) return undefined;
    setPosition(clampPosition(x, y, 0));
    const menu = menuRef.current;
    if (menu) setPosition(clampPosition(x, y, menu.getBoundingClientRect().height));
    requestAnimationFrame(() => menu?.querySelector<HTMLElement>('[role="menuitem"]')?.focus());
  }, [open, x, y]);
  if (!open) return null;
  const pageActions: [ReaderActionName, string, LucideIcon][] = [
    ['bookmark', bookmarked ? 'Delete bookmark' : 'Bookmark this page', Bookmark],
  ];
  return (
    <ReaderContextMenuSurface
      menuRef={menuRef}
      pageNumber={pageNumber}
      pageCount={pageCount}
      hasSelection={hasSelection}
      pageActions={pageActions}
      chromeVisible={chromeVisible}
      onAction={onAction}
      position={position}
    />
  );
}
