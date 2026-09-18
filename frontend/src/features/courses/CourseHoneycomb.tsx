import * as stylex from '@stylexjs/stylex';
import type { CourseSummary } from '@/lib/types';
import { media } from '@/styles/constants.stylex';
import { colors, typography } from '@/styles/tokens.stylex';
import { honeycombRows } from './honeycombLayout';

const styles = stylex.create({
  courseCardStudy: {
    gap: {
      default: '0.5rem',
      [media.tablet]: '0.625rem',
    },
    alignItems: 'flex-start',
    display: 'flex',
    flexDirection: 'column',
    marginTop: 'auto',
    maxWidth: '100%',
    paddingTop: {
      default: 0,
      [media.tablet]: '2rem',
    },
    width: 'fit-content',
  },
  courseCardStudyMeta: {
    alignItems: 'center',
    display: 'flex',
  },
  courseCardStudyLabel: {
    color: colors.textSecondary,
    fontSize: typography.sizeXs,
    fontWeight: 650,
    letterSpacing: '.08em',
    lineHeight: 1,
    textTransform: 'uppercase',
  },
  courseHoneycomb: {
    gap: 0,
    alignItems: 'flex-start',
    display: 'inline-flex',
    flexDirection: 'column',
    maxWidth: '100%',
    width: 'fit-content',
  },
  courseHoneycombRow: {
    gap: 0,
    display: 'flex',
    width: 'max-content',
  },
  courseHoneycombRowNext: {
    marginTop: {
      default: 'calc(-1 * 0.2552rem)',
      [media.tablet]: 'calc(-1 * 0.328125rem)',
      [media.mobile]: 'calc(-1 * 0.291675rem)',
    },
  },
  courseHoneycombRowIndent: (offset: number) => ({
    paddingInlineStart: `${offset * 0.4375}rem`,
  }),
  courseHoneycombCell: {
    fill: colors.surfaceHover,
    fillOpacity: 1,
    stroke: colors.borderLight,
    strokeLinejoin: 'round',
    strokeWidth: '1.15px',
    overflow: 'visible',
    display: 'block',
    height: {
      default: '1.0208rem',
      [media.tablet]: '1.3125rem',
      [media.mobile]: '1.1667rem',
    },
    width: {
      default: '0.875rem',
      [media.tablet]: '1.125rem',
      [media.mobile]: '1rem',
    },
  },
  courseHoneycombCellLevel1: { fill: colors.level1 },
  courseHoneycombCellLevel2: { fill: colors.level2 },
  courseHoneycombCellLevel3: { fill: colors.level3 },
  courseHoneycombCellLevel4: { fill: colors.level4 },
});

const STUDY_LEVELS = [0, 1, 2, 3, 4] as const;

function levelCounts(course: CourseSummary) {
  const counts = STUDY_LEVELS.map((level) => Math.max(0, Math.floor(Number(course.study_distribution?.[level] || 0))));
  const trackedFiles = counts.reduce((total, count) => total + count, 0);
  const totalFiles = Math.max(0, Math.floor(Number(course.total_files || 0)));
  counts[0] = (counts[0] ?? 0) + Math.max(0, totalFiles - trackedFiles);
  return counts;
}

function levelCells(counts: number[]) {
  return counts.flatMap((count, level) => Array.from({ length: count }, () => level));
}

function accessibleSummary(counts: number[]) {
  const detail = counts.map((count, level) => `${count} at ${level * 25}%`).join(', ');
  return `Study status: ${detail}`;
}

export function CourseHoneycomb({ course, rowCount }: { course: CourseSummary; rowCount: number }) {
  const counts = levelCounts(course);
  const rows = honeycombRows(levelCells(counts), rowCount);
  return (
    <span {...stylex.props(styles.courseCardStudy)}>
      <span {...stylex.props(styles.courseCardStudyMeta)}>
        <span {...stylex.props(styles.courseCardStudyLabel)}>Study Status</span>
      </span>
      <span {...stylex.props(styles.courseHoneycomb)} role="img" aria-label={accessibleSummary(counts)}>
        {rows.map((row, rowIndex) => (
          <span
            {...stylex.props(
              styles.courseHoneycombRow,
              rowIndex > 0 && styles.courseHoneycombRowNext,
              styles.courseHoneycombRowIndent(row.offset),
            )}
            aria-hidden="true"
            key={rowIndex}
          >
            {row.cells.map((level, cellIndex) => (
              <svg
                {...stylex.props(
                  styles.courseHoneycombCell,
                  level === 1
                    ? styles.courseHoneycombCellLevel1
                    : level === 2
                      ? styles.courseHoneycombCellLevel2
                      : level === 3
                        ? styles.courseHoneycombCellLevel3
                        : level === 4
                          ? styles.courseHoneycombCellLevel4
                          : null,
                )}
                data-level={level}
                viewBox="0 0 18 21"
                focusable="false"
                key={`${rowIndex}-${level}-${cellIndex}`}
              >
                <path d="M9 0L18 5.25V15.75L9 21L0 15.75V5.25Z" />
              </svg>
            ))}
          </span>
        ))}
      </span>
    </span>
  );
}
