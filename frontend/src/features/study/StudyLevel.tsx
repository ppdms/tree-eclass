import * as stylex from '@stylexjs/stylex';
import { Icon } from '@/components/Icon';
import * as React from 'react';
import { colors, layout, spacing } from '@/styles/tokens.stylex';
import { useStudyLevel } from './useStudyLevel';

const styles = stylex.create({
  studyWrap: {
    display: 'inline-block',
    position: 'relative',
    verticalAlign: 'middle',
  },
  studyPicker: {
    borderColor: colors.border,
    borderRadius: layout.radius,
    borderStyle: 'solid',
    borderWidth: 1,
    gap: spacing.xs,
    paddingBlock: spacing.xs,
    paddingInline: spacing.sm,
    alignItems: 'center',
    backgroundColor: colors.surfaceRaised,
    boxShadow: '0 0.5rem 1.5rem rgb(0 0 0 / 0.3)',
    display: 'flex',
    position: 'absolute',
    transform: 'translateY(-50%)',
    whiteSpace: 'nowrap',
    zIndex: 200,
    left: 'calc(100% + 0.125rem)',
    top: '50%',
  },
  pickerButton: {
    padding: 0,
    borderColor: { default: 'transparent', ':hover': colors.borderLight },
    borderRadius: layout.radius,
    borderStyle: 'solid',
    borderWidth: 1,
    alignItems: 'center',
    backgroundColor: { default: 'transparent', ':hover': colors.surfaceHover },
    color: colors.textPrimary,
    cursor: 'pointer',
    display: 'inline-flex',
    justifyContent: 'center',
    height: '2.5rem',
    minHeight: '2.5rem',
    minWidth: '2.5rem',
    width: '2.5rem',
  },
  studyLevelButton: {
    borderColor: { default: colors.border, ':hover': colors.borderLight },
    borderRadius: layout.radiusPill,
    borderStyle: 'solid',
    borderWidth: 1,
    gap: '0.35rem',
    paddingBlock: '0.3rem',
    paddingInline: '0.65rem',
    alignItems: 'center',
    backgroundColor: { default: colors.surfaceRaised, ':hover': colors.surfaceHover },
    color: colors.textPrimary,
    cursor: 'pointer',
    display: 'inline-flex',
    fontSize: '0.75rem',
  },
  level0: { color: colors.textSecondary },
  level1: { color: colors.level1 },
  level2: { color: colors.level2 },
  level3: { color: colors.level3 },
  level4: { color: colors.level4 },
  studyLevelIgnored: {
    opacity: 0.5,
  },
  studyLevelText: {
    color: colors.textPrimarySoft,
    fontSize: '0.75rem',
  },
});

const LEVELS = ['not studied', 'glanced', 'familiar', 'studied', 'mastered', 'ignored'];
const LEVEL_MARKS = ['circle', 'circle-half', 'circle-half', 'circle-fill', 'record-circle-fill', 'dash'];

export interface StudyLevelProps {
  courseId: string | number;
  filePath?: string;
  initial?: number;
  onChanged?: (level: number) => void;
}

function useDismiss(open: boolean, ref: React.RefObject<HTMLSpanElement | null>, onClose: () => void) {
  React.useEffect(() => {
    if (!open) return;
    const outside = (event: Event) => {
      // SAFETY: mousedown/focusin events target DOM elements; the cast
      // narrows the generic EventTarget for contains().
      if (!ref.current?.contains(event.target as Node)) onClose();
    };
    const onKey = (event: KeyboardEvent) => {
      if (event.key === 'Escape') onClose();
    };
    document.addEventListener('mousedown', outside);
    document.addEventListener('focusin', outside);
    document.addEventListener('keydown', onKey);
    return () => {
      document.removeEventListener('mousedown', outside);
      document.removeEventListener('focusin', outside);
      document.removeEventListener('keydown', onKey);
    };
  }, [open, ref, onClose]);
}

function StudyLevelPicker({ level, busy, onPick }: { level: number; busy: boolean; onPick: (index: number) => void }) {
  return (
    <span {...stylex.props(styles.studyPicker)} role="group" aria-label="Set study level">
      {LEVELS.map((name, index) => (
        <button
          type="button"
          key={name}
          {...stylex.props(styles.pickerButton)}
          aria-pressed={index === level}
          aria-label={`Set study level to ${name}`}
          title={`Set study level to ${name}`}
          onClick={() => onPick(index)}
          disabled={busy}
        >
          <Icon name={LEVEL_MARKS[index] ?? 'circle'} aria-hidden="true" />
        </button>
      ))}
    </span>
  );
}

export default function StudyLevel({ courseId, filePath, initial, onChanged }: StudyLevelProps) {
  const { level, busy, update } = useStudyLevel(courseId, filePath, initial, onChanged);
  const [open, setOpen] = React.useState(false);
  const wrapRef = React.useRef<HTMLSpanElement>(null);
  const close = React.useCallback(() => setOpen(false), []);
  useDismiss(open, wrapRef, close);
  const pick = (index: number) => {
    close();
    update(index);
  };
  const levelStyle =
    level === 0
      ? styles.level0
      : level === 1
        ? styles.level1
        : level === 2
          ? styles.level2
          : level === 3
            ? styles.level3
            : level === 4
              ? styles.level4
              : null;
  return (
    <span {...stylex.props(styles.studyWrap)} ref={wrapRef}>
      <button
        type="button"
        {...stylex.props(styles.studyLevelButton, levelStyle, level === 5 && styles.studyLevelIgnored)}
        data-level={level}
        aria-haspopup="true"
        aria-expanded={open}
        aria-label={`Study level: ${LEVELS[level]}. Activate to change.`}
        title={`Study level: ${LEVELS[level]}`}
        onClick={() => setOpen((value) => !value)}
        disabled={busy}
      >
        <Icon name={LEVEL_MARKS[level] ?? 'circle'} aria-hidden="true" />
        <span {...stylex.props(styles.studyLevelText)}>{LEVELS[level]}</span>
      </button>
      {open && <StudyLevelPicker level={level} busy={busy} onPick={pick} />}
    </span>
  );
}
