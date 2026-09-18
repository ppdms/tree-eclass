import * as stylex from '@stylexjs/stylex';
import * as React from 'react';
import { Route } from 'lucide-react';
import type { CourseSummary, RoadmapBlueprint } from '@/lib/types';
import { media } from '@/styles/constants.stylex';
import { colors, typography } from '@/styles/tokens.stylex';
import { StrategySection, SupportGrid } from './sections';
import { Units } from './units';
import { RoadmapWaiting } from './waiting';
import { PracticePanel } from './practice';

export { PracticePanel };

const styles = stylex.create({
  courseRoadmap: {
    gap: '.85rem',
    marginInline: 0,
    display: 'grid',
    marginBottom: '1rem',
  },
  courseRoadmapHeader: {
    borderColor: colors.border,
    borderRadius: '.875rem',
    borderStyle: 'solid',
    borderWidth: 1,
    gap: '1.5rem',
    paddingBlock: '1.15rem',
    paddingInline: '1.15rem',
    alignItems: {
      default: 'flex-end',
      [media.mobile]: 'flex-start',
    },
    backgroundColor: colors.surface,
    display: 'flex',
    flexDirection: {
      default: 'row',
      [media.mobile]: 'column',
    },
    justifyContent: 'space-between',
  },
  courseRoadmapTitle: {
    marginInline: 0,
    fontSize: '1.15rem',
    letterSpacing: '-0.025em',
    marginBlockEnd: 0,
    marginBlockStart: '0.2rem',
  },
  courseCardEyebrow: {
    gap: '.4rem',
    alignItems: 'center',
    color: colors.success,
    display: 'flex',
    fontSize: typography.sizeXs,
    fontWeight: 750,
    letterSpacing: '.09em',
    textTransform: 'uppercase',
  },
  courseRoadmapProgress: {
    alignItems: 'baseline',
    columnGap: '.55rem',
    display: 'grid',
    gridTemplateColumns: 'auto auto',
    rowGap: '.2rem',
    minWidth: '12rem',
    width: {
      default: 'auto',
      [media.mobile]: '100%',
    },
  },
  courseRoadmapProgressValue: {
    fontSize: '1.2rem',
    fontWeight: 600,
  },
  courseRoadmapProgressLabel: {
    color: colors.textSecondary,
    fontSize: typography.sizeXs,
  },
  courseRoadmapProgressTrack: {
    borderRadius: '999px',
    overflow: 'hidden',
    backgroundColor: colors.surfaceHover,
    gridColumnEnd: '-1',
    gridColumnStart: '1',
    height: '.3rem',
  },
  progressFill: (width: string) => ({
    borderRadius: 'inherit',
    backgroundColor: colors.success,
    display: 'block',
    height: '100%',
    width,
  }),
  courseRoadmapSecondary: {
    gap: '.85rem',
    display: 'grid',
    gridTemplateColumns: {
      default: 'repeat(2, minmax(0, 1fr))',
      [media.tablet]: '1fr',
    },
  },
  courseRoadmapRefresh: {
    marginBlock: 0,
    marginInline: 0,
    color: colors.textSecondary,
    fontSize: typography.sizeXs,
  },
});

function RoadmapProgress({ progress }: { progress: NonNullable<RoadmapBlueprint['progress']> }) {
  const percent = Math.round(Number(progress.percent || 0));
  return (
    <header {...stylex.props(styles.courseRoadmapHeader)}>
      <div>
        <span {...stylex.props(styles.courseCardEyebrow)}>
          <Route /> Learning path
        </span>
        <h2 id="course-roadmap-title" {...stylex.props(styles.courseRoadmapTitle)}>
          Roadmap
        </h2>
      </div>
      <div {...stylex.props(styles.courseRoadmapProgress)}>
        <strong {...stylex.props(styles.courseRoadmapProgressValue)}>{percent}%</strong>
        <span {...stylex.props(styles.courseRoadmapProgressLabel)}>
          {progress.completed_actions || 0}/{progress.total_actions || 0} actions
        </span>
        <div {...stylex.props(styles.courseRoadmapProgressTrack)} aria-label={`${percent}% complete`}>
          <i {...stylex.props(styles.progressFill(`${Math.min(100, percent)}%`))} />
        </div>
      </div>
    </header>
  );
}

export default function CourseRoadmap({ course, blueprint }: { course: CourseSummary; blueprint?: RoadmapBlueprint }) {
  const roadmap = blueprint?.blueprint;
  if (!roadmap) return <RoadmapWaiting blueprint={blueprint} course={course} />;
  const progress = blueprint?.progress || {};
  const revision = blueprint.revision_id || blueprint.revision_hash;
  return (
    <section {...stylex.props(styles.courseRoadmap)} aria-labelledby="course-roadmap-title">
      <RoadmapProgress progress={progress} />
      <Units roadmap={roadmap} course={course} revision={revision} />
      <div {...stylex.props(styles.courseRoadmapSecondary)}>
        <StrategySection roadmap={roadmap} course={course} revision={revision} />
        <SupportGrid roadmap={roadmap} course={course} revision={revision} />
      </div>
      {blueprint.readiness?.refresh_pending ? (
        <p {...stylex.props(styles.courseRoadmapRefresh)}>A refreshed roadmap is being prepared.</p>
      ) : null}
    </section>
  );
}
