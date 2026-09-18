import * as stylex from '@stylexjs/stylex';
import { Bookmark, Check, LoaderCircle, MessageSquareText, type LucideIcon } from 'lucide-react';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Popover } from '@astryxdesign/core/Popover';
import { Tooltip } from '@astryxdesign/core/Tooltip';
import { colors, typography } from '@/styles/tokens.stylex';
import type { StudyAction } from '@/lib/types';
import type { SessionInfo } from '@/features/session/reader/types';
import { formatMinutes } from './formatMinutes';

type OutcomeVariant = 'default' | 'secondary' | 'destructive' | 'ghost' | 'outline';

const OUTCOMES: [string, string, OutcomeVariant][] = [
  ['completed', 'Done', 'default'],
  ['partial', 'Partial', 'secondary'],
  ['stuck', 'Stuck', 'destructive'],
  ['deferred', 'Defer', 'ghost'],
];
const FREE_READING_OUTCOME: [string, string, OutcomeVariant][] = [['abandoned', 'Finish reading', 'outline']];

const styles = stylex.create({
  actionToggle: {
    borderColor: { default: 'transparent', ':hover': 'transparent' },
    borderRadius: '0.5rem',
    borderWidth: '1px',
    gap: '0.5rem',
    paddingInline: '0.625rem',
    backgroundColor: { default: 'transparent', ':hover': colors.surfaceHover },
    color: { default: colors.textSecondary, ':hover': colors.textPrimary },
    flexShrink: 0,
    height: '2.5rem',
    minWidth: '2.5rem',
    width: '2.5rem',
  },
  actionTogglePressed: {
    borderColor: colors.borderLight,
    backgroundColor: colors.surfaceHover,
    color: colors.textPrimary,
  },
  drawerBtn: {
    width: '2.5rem',
  },
  drawerToggleWrap: {
    display: 'inline-flex',
    position: 'relative',
  },
  drawerBadge: {
    borderColor: colors.borderLight,
    borderWidth: '1px',
    paddingInline: '0.25rem',
    backgroundColor: colors.primary,
    color: colors.textPrimary,
    fontSize: typography.size2xs,
    justifyContent: 'center',
    position: 'absolute',
    height: '1rem',
    left: 'auto',
    minWidth: '1rem',
    right: '0.25rem',
    top: '0.25rem',
  },
  finishButton: {
    borderRadius: '0.5rem',
    borderWidth: 0,
    paddingInline: '0.625rem',
    backgroundColor: colors.textPrimary,
    boxShadow: 'none',
    color: colors.background,
    transitionProperty: 'background-color, color',
    height: '2.5rem',
    minHeight: '2.5rem',
  },
  finishContent: {
    width: '12rem',
  },
  closedContainer: {
    padding: '0.25rem',
    color: colors.textSecondary,
    fontSize: '0.75rem',
  },
  closedMt: {
    marginTop: '0.25rem',
  },
  planLink: {
    color: colors.primary,
    display: 'inline-block',
    textUnderlineOffset: '4px',
    marginTop: '0.25rem',
  },
  finishMenuGrid: {
    gap: '0.25rem',
    display: 'grid',
    gridTemplateColumns: 'repeat(2, minmax(0, 1fr))',
  },
  spinner: {
    height: '1rem',
    width: '1rem',
  },
  disabledToggle: {
    backgroundColor: 'transparent',
    color: colors.textSecondary,
    cursor: 'not-allowed',
    opacity: 0.5,
  },
});

export function DrawerToggle({
  icon: Icon,
  label,
  active,
  badge,
  onClick,
}: {
  icon: LucideIcon;
  label: string;
  active: boolean;
  badge: number | null;
  onClick: () => void;
}) {
  return (
    <Tooltip content={label} placement="below">
      <span {...stylex.props(styles.drawerToggleWrap)}>
        <Button
          type="button"
          variant={active ? 'secondary' : 'ghost'}
          size="sm"
          {...stylex.props(styles.actionToggle, styles.drawerBtn, active && styles.actionTogglePressed)}
          aria-label={label}
          title={label}
          aria-pressed={active}
          onClick={onClick}
          isIconOnly
          icon={<Icon aria-hidden="true" />}
        />
        {badge ? <Badge {...stylex.props(styles.drawerBadge)}>{badge}</Badge> : null}
      </span>
    </Tooltip>
  );
}

function FinishMenuClosed({ closed, questionCount }: { closed: SessionInfo; questionCount: number }) {
  return (
    <div {...stylex.props(styles.closedContainer)}>
      <p>{formatMinutes(closed.active_seconds)} recorded.</p>
      {questionCount ? (
        <p {...stylex.props(styles.closedMt)}>
          {questionCount} thing{questionCount === 1 ? '' : 's'} you flagged as unclear stayed with the pages.
        </p>
      ) : null}
      <a href="/study" {...stylex.props(styles.planLink)}>
        Back to the plan
      </a>
    </div>
  );
}

function FinishMenuOutcomes({
  outcomes,
  closing,
  onFinish,
  grid,
}: {
  outcomes: [string, string, OutcomeVariant][];
  closing: string | null;
  onFinish: (outcome: string) => void;
  grid: boolean;
}) {
  return (
    <div {...stylex.props(grid && styles.finishMenuGrid)}>
      {outcomes.map(([value, label, variant]) => (
        <Button
          key={value}
          type="button"
          variant={variant}
          size="sm"
          icon={closing === value ? <LoaderCircle {...stylex.props(styles.spinner)} aria-hidden="true" /> : undefined}
          disabled={Boolean(closing)}
          aria-label={closing === value ? `Recording ${label}` : label}
          onClick={() => onFinish(value)}
        >
          {label}
        </Button>
      ))}
    </div>
  );
}

function FinishMenuContent({
  action,
  closing,
  closed,
  questionCount,
  onFinish,
}: Pick<FinishMenuProps, 'action' | 'closing' | 'closed' | 'questionCount' | 'onFinish'>) {
  return (
    <div {...stylex.props(styles.finishContent)}>
      {closed ? (
        <FinishMenuClosed closed={closed} questionCount={questionCount} />
      ) : (
        <FinishMenuOutcomes
          outcomes={action ? OUTCOMES : FREE_READING_OUTCOME}
          closing={closing}
          onFinish={onFinish}
          grid={Boolean(action)}
        />
      )}
    </div>
  );
}

interface FinishMenuProps {
  action: StudyAction | null;
  session: SessionInfo | null;
  closing: string | null;
  closed: SessionInfo | null;
  questionCount: number;
  onFinish: (outcome: string) => void;
}

export function FinishMenu({ action, session, closing, closed, questionCount, onFinish }: FinishMenuProps) {
  return (
    <Popover
      placement="below"
      alignment="end"
      content={
        <FinishMenuContent
          action={action}
          closing={closing}
          closed={closed}
          questionCount={questionCount}
          onFinish={onFinish}
        />
      }
    >
      <Button
        type="button"
        size="sm"
        style={styles.finishButton}
        variant="outline"
        disabled={!session}
        icon={closed ? <Check aria-hidden="true" /> : undefined}
      >
        {closed ? 'Recorded' : 'Finish session'}
      </Button>
    </Popover>
  );
}

export function SelectionNoteAction({ available, onClick }: { available: boolean; onClick: () => void }) {
  const label = available ? 'Add note to selection' : 'Select text to add a note';
  return (
    <Tooltip content={label} placement="below">
      <Button
        type="button"
        variant="ghost"
        size="sm"
        {...stylex.props(styles.actionToggle, styles.drawerBtn, !available && styles.disabledToggle)}
        aria-label={label}
        aria-disabled={!available}
        disabled={!available}
        title={label}
        onClick={onClick}
        isIconOnly
        icon={<MessageSquareText aria-hidden="true" />}
      />
    </Tooltip>
  );
}

export function BookmarkAction({ bookmarked, onClick }: { bookmarked: boolean; onClick: () => void }) {
  const label = bookmarked ? 'Delete bookmark' : 'Bookmark page';
  const tooltip = bookmarked ? 'Delete bookmark from this page' : 'Bookmark this page';
  return (
    <Tooltip content={tooltip} placement="below">
      <Button
        type="button"
        variant="ghost"
        size="sm"
        {...stylex.props(styles.actionToggle, bookmarked && styles.actionTogglePressed)}
        onClick={onClick}
        aria-pressed={bookmarked}
        aria-label={label}
        title={tooltip}
        isIconOnly
        icon={<Bookmark aria-hidden="true" fill={bookmarked ? 'currentColor' : 'none'} />}
      />
    </Tooltip>
  );
}
