import * as stylex from '@stylexjs/stylex';
import { useEffect, useState } from 'react';
import { Maximize2, Minus, Plus, ScanLine, type LucideIcon } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Tooltip } from '@astryxdesign/core/Tooltip';
import { commonStyles } from '@/styles/common';
import { colors, spacing } from '@/styles/tokens.stylex';

const styles = stylex.create({
  center: {
    gap: spacing.sm,
    marginInline: 'auto',
    alignItems: 'center',
    display: 'flex',
    flexShrink: 0,
    justifyContent: 'center',
    minWidth: 0,
  },
  zoomGroup: {
    padding: '0.1rem',
    borderColor: colors.border,
    borderRadius: '0.5rem',
    borderWidth: '1px',
    alignItems: 'center',
    backgroundColor: colors.surface,
    display: 'flex',
    flexShrink: 0,
    height: '2.5rem',
  },
  zoomBtn: {
    padding: 0,
    borderRadius: '0.5rem',
    flexShrink: 0,
    height: '2.5rem',
    width: '2.5rem',
  },
  zoomValue: {
    color: colors.textSecondary,
    fontSize: '0.75rem',
    fontVariantNumeric: 'tabular-nums',
    textAlign: 'center',
    width: '3rem',
  },
  fitBtn: {
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
  },
  fitBtnActive: {
    borderColor: colors.borderLight,
    backgroundColor: colors.surfaceHover,
    color: colors.textPrimary,
  },
  pageNav: {
    borderColor: colors.border,
    borderRadius: '0.5rem',
    borderWidth: '1px',
    gap: '0.35rem',
    paddingBlock: '0.2rem',
    alignItems: 'center',
    backgroundColor: colors.surface,
    display: 'inline-flex',
    flexShrink: 0,
    paddingInlineEnd: '0.45rem',
    paddingInlineStart: '0.65rem',
    minWidth: 0,
  },
  pageLabel: {
    color: colors.textSecondary,
    fontSize: '0.75rem',
  },
  pageInput: {
    borderColor: 'transparent',
    paddingBlock: 0,
    paddingInline: '0.375rem',
    appearance: 'none',
    backgroundColor: colors.background,
    boxShadow: 'none',
    fontSize: '0.75rem',
    fontVariantNumeric: 'tabular-nums',
    textAlign: 'center',
    height: '2rem',
    width: '2.5rem',
  },
  pageTotal: {
    color: colors.textSecondary,
    fontSize: '0.75rem',
    fontVariantNumeric: 'tabular-nums',
    minWidth: '2rem',
  },
  pageRange: {
    paddingBlock: '0.85rem',
    accentColor: colors.info,
    appearance: 'none',
    backgroundColor: 'transparent',
    flexShrink: 0,
    minHeight: '2.5rem',
    width: 'clamp(4.5rem, 8vw, 8rem)',
  },
});

function ZoomControls({
  scale,
  onZoomOut,
  onZoomIn,
}: {
  scale: number | null;
  onZoomOut: () => void;
  onZoomIn: () => void;
}) {
  return (
    <span {...stylex.props(styles.zoomGroup)} aria-label="Zoom controls">
      <Button
        type="button"
        variant="ghost"
        size="icon"
        {...stylex.props(styles.zoomBtn)}
        onClick={onZoomOut}
        aria-label="Zoom out"
        title="Zoom out"
        icon={<Minus aria-hidden="true" />}
      />
      <span {...stylex.props(styles.zoomValue)} aria-live="polite">
        {scale ? `${Math.round(scale * 100)}%` : '—'}
      </span>
      <Button
        type="button"
        variant="ghost"
        size="icon"
        {...stylex.props(styles.zoomBtn)}
        onClick={onZoomIn}
        aria-label="Zoom in"
        title="Zoom in"
        icon={<Plus aria-hidden="true" />}
      />
    </span>
  );
}

function FitButton({
  icon: Icon,
  active,
  label,
  title,
  onClick,
}: {
  icon: LucideIcon;
  active: boolean;
  label: string;
  title: string;
  onClick: () => void;
}) {
  return (
    <Tooltip content={title} placement="below">
      <Button
        type="button"
        variant="ghost"
        size="sm"
        isIconOnly
        {...stylex.props(styles.fitBtn, active && styles.fitBtnActive)}
        onClick={onClick}
        aria-label={label}
        aria-pressed={active}
        title={active ? `${title} (active)` : title}
        icon={<Icon aria-hidden="true" />}
      />
    </Tooltip>
  );
}

function FitControls({
  fitMode,
  onFitPage,
  onFitWidth,
}: {
  fitMode: 'page' | 'width' | 'manual';
  onFitPage: () => void;
  onFitWidth: () => void;
}) {
  return (
    <>
      <FitButton
        icon={Maximize2}
        active={fitMode === 'page'}
        label="Fit one page in view"
        title="Fit one page in view"
        onClick={onFitPage}
      />
      <FitButton
        icon={ScanLine}
        active={fitMode === 'width'}
        label="Fit page width"
        title="Fit page width"
        onClick={onFitWidth}
      />
    </>
  );
}

interface PageNavProps {
  pageNumber: number;
  pageCount?: number;
  onGoToPage: (page: number, behavior?: ScrollBehavior) => void;
}

function PageNav({ pageNumber, pageCount, onGoToPage }: PageNavProps) {
  const total = Number(pageCount) || 1;
  const [draft, setDraft] = useState(String(pageNumber));
  useEffect(() => setDraft(String(pageNumber)), [pageNumber]);
  const commit = () => {
    const next = Math.max(1, Math.min(total, Number(draft) || pageNumber));
    setDraft(String(next));
    onGoToPage(next);
  };
  return (
    <div {...stylex.props(styles.pageNav)} aria-label="Page navigation">
      <span {...stylex.props(styles.pageLabel)}>Page</span>
      <label {...stylex.props(commonStyles.srOnly)} htmlFor="session-go-to-page">
        Go to page
      </label>
      <Input
        id="session-go-to-page"
        type="number"
        min={1}
        max={pageCount || undefined}
        {...stylex.props(styles.pageInput)}
        onKeyDown={(event) => {
          if (event.key === 'Enter') commit();
        }}
        value={draft}
        onChange={(event) => setDraft(event.target.value)}
        onBlur={commit}
        aria-label="Go to page"
      />
      <span {...stylex.props(styles.pageTotal)}>/ {pageCount || '—'}</span>
      <input
        {...stylex.props(styles.pageRange)}
        type="range"
        min="1"
        max={total}
        value={Math.min(pageNumber, total)}
        onChange={(event) => onGoToPage(Number(event.target.value))}
        aria-label={`Page ${pageNumber} of ${pageCount || 'unknown'}`}
      />
      <span {...stylex.props(commonStyles.srOnly)} aria-live="polite">
        Page {pageNumber} of {pageCount || 'unknown'}
      </span>
    </div>
  );
}

export interface ToolbarCenterProps {
  scale: number | null;
  onZoomOut: () => void;
  onZoomIn: () => void;
  fitMode: 'page' | 'width' | 'manual';
  onFitPage: () => void;
  onFitWidth: () => void;
  pageNumber: number;
  pageCount?: number;
  onGoToPage: (page: number, behavior?: ScrollBehavior) => void;
}

export default function ToolbarCenter({
  scale,
  onZoomOut,
  onZoomIn,
  fitMode,
  onFitPage,
  onFitWidth,
  pageNumber,
  pageCount,
  onGoToPage,
}: ToolbarCenterProps) {
  return (
    <div {...stylex.props(styles.center)}>
      <ZoomControls scale={scale} onZoomOut={onZoomOut} onZoomIn={onZoomIn} />
      <FitControls fitMode={fitMode} onFitPage={onFitPage} onFitWidth={onFitWidth} />
      <PageNav pageNumber={pageNumber} pageCount={pageCount} onGoToPage={onGoToPage} />
    </div>
  );
}
