import * as stylex from '@stylexjs/stylex';
import * as React from 'react';
import { ArrowUpRight, BookOpen, Brain, ListChecks, PenLine } from 'lucide-react';
import { Button } from '@/components/ui/button';
import type { StudyAction } from '@/lib/types';
import { media } from '@/styles/constants.stylex';
import { colors, layout, typography } from '@/styles/tokens.stylex';

const styles = stylex.create({
  studyQueueCard: {
    borderColor: colors.border,
    borderRadius: '1rem',
    borderStyle: 'solid',
    borderWidth: 1,
    overflow: 'hidden',
    backgroundColor: colors.surface,
  },
  studySectionHeading: {
    paddingBlock: '0.85rem',
    paddingInline: '1rem',
    alignItems: 'center',
    display: 'flex',
    justifyContent: 'space-between',
    borderBottomColor: colors.border,
    borderBottomStyle: 'solid',
    borderBottomWidth: 1,
    minHeight: '3.5rem',
  },
  studySectionHeadingTitle: {
    gap: '0.5rem',
    alignItems: 'center',
    color: colors.textPrimary,
    display: 'flex',
    fontSize: typography.sizeBase,
    fontWeight: 650,
  },
  studySectionHeadingSvg: {
    height: '0.95rem',
    width: '0.95rem',
  },
  studySectionHeadingBadge: {
    borderRadius: layout.radiusPill,
    placeItems: 'center',
    alignItems: 'center',
    backgroundColor: colors.surfaceRaised,
    color: colors.textSecondary,
    display: 'grid',
    fontSize: typography.sizeXs,
    fontWeight: 600,
    height: '1.5rem',
    width: '1.5rem',
  },
  studyQueueList: {
    padding: '0.35rem',
    display: 'flex',
    flexDirection: 'column',
  },
  studyQueueRow: {
    padding: '0.65rem',
    gap: '0.75rem',
    alignItems: 'center',
    display: 'grid',
    gridTemplateColumns: {
      default: '2rem 2.25rem minmax(0, 1fr) 2.5rem',
      [media.narrow]: '2rem minmax(0, 1fr) 2.5rem',
    },
    borderBottomColor: colors.border,
    borderBottomStyle: 'solid',
    borderBottomWidth: 1,
    minHeight: '5rem',
  },
  studyQueueRowLast: {
    borderBottomStyle: 'none',
    borderBottomWidth: 0,
  },
  studyQueueIndex: {
    color: colors.textSecondary,
    display: {
      default: 'block',
      [media.narrow]: 'none',
    },
    fontFamily: typography.fontMono,
    fontSize: typography.sizeXs,
    textAlign: 'left',
  },
  studyQueueIcon: {
    borderRadius: layout.radiusMedium,
    placeItems: 'center',
    alignItems: 'center',
    backgroundColor: colors.surfaceRaised,
    color: colors.textPrimarySoft,
    display: 'grid',
    height: '2.25rem',
    width: '2.25rem',
  },
  studyQueueIconSvg: {
    height: '1rem',
    width: '1rem',
  },
  studyQueueCopy: {
    minWidth: 0,
  },
  studyQueueHeading: {
    margin: 0,
    overflow: 'hidden',
    WebkitBoxOrient: 'vertical',
    WebkitLineClamp: 2,
    color: colors.textPrimary,
    display: '-webkit-box',
    fontSize: '0.8125rem',
    fontWeight: 600,
    lineHeight: 1.4,
  },
  studyQueueDesc: {
    marginInline: 0,
    color: colors.textSecondary,
    fontSize: typography.sizeXs,
    marginBlockEnd: 0,
    marginBlockStart: '0.25rem',
  },
  studyQueueOpen: {
    color: colors.textSecondary,
  },
});

function ActionIcon({ item }: { item: StudyAction }) {
  const kind = item.action_type || item.kind || '';
  if (kind === 'recall') return <PenLine aria-hidden="true" {...stylex.props(styles.studyQueueIconSvg)} />;
  if (kind === 'solve' || kind === 'practice') {
    return <Brain aria-hidden="true" {...stylex.props(styles.studyQueueIconSvg)} />;
  }
  return <BookOpen aria-hidden="true" {...stylex.props(styles.studyQueueIconSvg)} />;
}

function actionHref(item: StudyAction, fallback: boolean): string {
  if (fallback) {
    return item.redirect_url || `/files${encodeURI(item.source_path || item.file_path || '')}`;
  }
  return `/study/session?course_id=${item.course_id}&action_id=${encodeURIComponent(item.action_id || '')}`;
}

export function AlternativeRow({
  item,
  fallback,
  index,
  isLast,
}: {
  item: StudyAction;
  fallback: boolean;
  index: number;
  isLast: boolean;
}) {
  return (
    <article {...stylex.props(styles.studyQueueRow, isLast && styles.studyQueueRowLast)}>
      <span {...stylex.props(styles.studyQueueIndex)}>{String(index + 1).padStart(2, '0')}</span>
      <span {...stylex.props(styles.studyQueueIcon)}>
        <ActionIcon item={item} />
      </span>
      <div {...stylex.props(styles.studyQueueCopy)}>
        <h3 {...stylex.props(styles.studyQueueHeading)}>
          {item.instruction || item.recommended_action || item.file_name}
        </h3>
        <p {...stylex.props(styles.studyQueueDesc)}>
          {item.course_name}
          {item.minutes ? ` · ${item.minutes} minutes` : ''}
        </p>
      </div>
      <Button
        variant="ghost"
        size="icon"
        href={actionHref(item, fallback)}
        target={fallback ? '_blank' : undefined}
        rel={fallback ? 'noopener' : undefined}
        aria-label={`Start ${item.instruction || item.file_name || 'study action'}`}
        {...stylex.props(styles.studyQueueOpen)}
        icon={<ArrowUpRight aria-hidden="true" />}
      />
    </article>
  );
}

export function Alternatives({ actions, fallback }: { actions: StudyAction[]; fallback: boolean }) {
  if (!actions.length) return null;
  const visible = actions.slice(0, 6);
  return (
    <section {...stylex.props(styles.studyQueueCard)}>
      <header {...stylex.props(styles.studySectionHeading)}>
        <span {...stylex.props(styles.studySectionHeadingTitle)}>
          <ListChecks aria-hidden="true" {...stylex.props(styles.studySectionHeadingSvg)} /> Up next
        </span>
        <small {...stylex.props(styles.studySectionHeadingBadge)}>{actions.length}</small>
      </header>
      <div {...stylex.props(styles.studyQueueList)}>
        {visible.map((item, index) => (
          <AlternativeRow
            key={item.action_id || item.file_path || index}
            item={item}
            fallback={fallback}
            index={index}
            isLast={index === visible.length - 1}
          />
        ))}
      </div>
    </section>
  );
}
