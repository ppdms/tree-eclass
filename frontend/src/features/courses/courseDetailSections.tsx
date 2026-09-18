import * as stylex from '@stylexjs/stylex';
import { ArrowRight, Clock3, Gauge, Play } from 'lucide-react';
import { ActivityList } from '@/features/activity/activityParts';
import { timelineToActivityGroups } from '@/features/activity/activityFeed';
import { Button } from '@/components/ui/button';
import { commonStyles } from '@/styles/common';
import { media } from '@/styles/constants.stylex';
import { colors, typography } from '@/styles/tokens.stylex';
import type { CourseDetailPayload, CourseTimelineItem, RoadmapAction, RoadmapUnit } from '@/lib/types';
import { RoadmapReadinessNote } from './course-roadmap/waiting';

const styles = stylex.create({
  courseOverviewDashboard: {
    gap: '1rem',
    display: 'grid',
  },
  courseOverviewHero: {
    gap: '1rem',
    display: 'grid',
    gridTemplateColumns: {
      default: 'minmax(0, 2.2fr) minmax(15rem, 0.8fr)',
      [media.tablet]: '1fr',
      [media.mobile]: 'minmax(0, 1fr)',
    },
  },
  courseContinueCard: {
    borderColor: colors.border,
    borderRadius: '.875rem',
    borderStyle: 'solid',
    borderWidth: 1,
    overflow: 'hidden',
    backgroundColor: colors.surfaceRaised,
    boxSizing: 'border-box',
    display: 'flex',
    flexDirection: 'column',
    position: 'relative',
    paddingBottom: 'clamp(1.25rem, 3vw, 2rem)',
    paddingLeft: 'clamp(1.25rem, 3vw, 2rem)',
    paddingRight: 'clamp(1.25rem, 3vw, 2rem)',
    paddingTop: 'clamp(1.25rem, 3vw, 2rem)',
    width: '100%',
  },
  courseContinueAccent: {
    backgroundImage: `linear-gradient(90deg, ${colors.success}, transparent 75%)`,
    position: 'absolute',
    height: '0.125rem',
    left: 0,
    top: 0,
    width: '100%',
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
  courseContinueTitle: {
    fontSize: {
      default: 'clamp(1.35rem, 2.6vw, 2rem)',
      [media.mobile]: '1.35rem',
    },
    fontWeight: 650,
    letterSpacing: '-0.045em',
    lineHeight: 1.16,
    marginBlockEnd: '0.75rem',
    marginBlockStart: '1.1rem',
    textWrap: 'balance',
    maxWidth: '36ch',
  },
  courseContinueMeta: {
    alignItems: 'center',
    color: colors.textSecondary,
    columnGap: '1rem',
    display: 'flex',
    flexWrap: 'wrap',
    fontSize: '.75rem',
    rowGap: '.5rem',
    marginBottom: '1.35rem',
    minHeight: '1.5rem',
  },
  courseProgressCard: {
    borderColor: colors.border,
    borderRadius: '.875rem',
    borderStyle: 'solid',
    borderWidth: 1,
    paddingBlock: '1.4rem',
    paddingInline: '1.4rem',
    backgroundColor: colors.surface,
    display: 'flex',
    flexDirection: 'column',
  },
  courseProgressValue: {
    marginBlock: {
      default: 'auto 0.75rem',
      [media.tablet]: '2rem 0.75rem',
    },
    fontSize: 'clamp(2.4rem, 5vw, 3.6rem)',
    fontWeight: 600,
    letterSpacing: '-0.07em',
    lineHeight: 1,
  },
  courseProgressTrack: {
    borderRadius: '999px',
    overflow: 'hidden',
    backgroundColor: colors.surfaceHover,
    height: '.3rem',
    width: '100%',
  },
  courseProgressSummary: {
    color: colors.textSecondary,
    fontSize: typography.sizeXs,
    marginBlockEnd: 0,
    marginBlockStart: '0.65rem',
  },
  progressFill: (width: string) => ({
    borderRadius: 'inherit',
    backgroundColor: colors.success,
    display: 'block',
    height: '100%',
    width,
  }),
  courseOverviewSection: {
    borderColor: colors.border,
    borderRadius: '.875rem',
    borderStyle: 'solid',
    borderWidth: 1,
    overflow: 'hidden',
    backgroundColor: colors.surface,
  },
  roadmapPreviewHeader: {
    gap: '1rem',
    paddingBlock: '1rem',
    paddingInline: '1.15rem',
    alignItems: 'center',
    display: 'flex',
    justifyContent: 'space-between',
  },
  roadmapPreviewHeading: {
    gap: '0.1rem',
    display: 'grid',
  },
  roadmapPreviewCategory: {
    color: colors.textSecondary,
    fontSize: typography.sizeXs,
    fontWeight: 650,
    letterSpacing: '0.07em',
    textTransform: 'uppercase',
  },
  roadmapPreviewTitle: {
    fontSize: '1rem',
    fontWeight: 600,
  },
  courseUnitPreviewList: {
    margin: 0,
    listStyle: 'none',
    paddingInline: '1.15rem',
    display: 'grid',
    gridTemplateColumns: {
      default: 'repeat(2, minmax(0, 1fr))',
      [media.mobile]: '1fr',
    },
    paddingBlockEnd: '1rem',
    paddingBlockStart: 0,
  },
  courseUnitPreviewItem: {
    padding: '0.65rem',
    gap: '0.65rem',
    alignItems: 'center',
    display: 'grid',
    gridTemplateColumns: '1.7rem minmax(0, 1fr) auto',
    borderTopColor: colors.border,
    borderTopStyle: 'solid',
    borderTopWidth: 1,
    minHeight: '3.7rem',
  },
  courseUnitPreviewItemOdd: {
    borderRightColor: {
      default: colors.border,
      [media.mobile]: 'transparent',
    },
    borderRightStyle: 'solid',
    borderRightWidth: {
      default: 1,
      [media.mobile]: 0,
    },
  },
  courseUnitIndex: {
    borderColor: colors.borderLight,
    borderRadius: '999px',
    borderStyle: 'solid',
    borderWidth: 1,
    placeItems: 'center',
    color: colors.textSecondary,
    display: 'grid',
    fontSize: typography.sizeXs,
    fontVariantNumeric: 'tabular-nums',
    height: '1.6rem',
    width: '1.6rem',
  },
  courseUnitPreviewMain: {
    gap: '.45rem',
    display: 'grid',
    minWidth: 0,
  },
  courseUnitPreviewTitle: {
    overflow: 'hidden',
    fontSize: '0.78rem',
    fontWeight: 550,
    textOverflow: 'ellipsis',
    whiteSpace: 'nowrap',
  },
  courseUnitPreviewTrack: {
    borderRadius: '999px',
    overflow: 'hidden',
    backgroundColor: colors.surfaceHover,
    height: '.3rem',
  },
  courseUnitPreviewMeta: {
    color: colors.textSecondary,
    fontSize: typography.sizeXs,
    fontVariantNumeric: 'tabular-nums',
  },
  coursePanelEmpty: {
    padding: '2rem',
    marginBlock: '2rem',
    marginInline: 0,
    color: colors.textSecondary,
    textAlign: 'center',
  },
  courseActivityTab: {},
});

function shortInstruction(value: string | undefined): string {
  const instruction = String(value || 'Choose your next study action');
  const lead = instruction.split(/\s+covering\s+/i)[0] || instruction;
  return lead.replace(/[.:]$/, '');
}

function ActionUnit({ units, action }: { units: RoadmapUnit[]; action?: RoadmapAction }) {
  if (!action) return null;
  const actionId = action.action_id || action.id;
  const unit = units.find((item) =>
    item.actions?.some((candidate) => (candidate.action_id || candidate.id) === actionId),
  );
  const title = action.unit_title || unit?.title;
  return title ? <span>{title}</span> : null;
}

function ContinueCard({ data }: { data: CourseDetailPayload }) {
  const progress = data.course_blueprint?.progress || {};
  const action = progress.next_action;
  const units = data.course_blueprint?.blueprint?.units || [];
  const minutes = action?.remaining_minutes || action?.estimated_minutes || 15;
  return (
    <article {...stylex.props(styles.courseContinueCard)}>
      <div {...stylex.props(styles.courseContinueAccent)} />
      <div {...stylex.props(styles.courseCardEyebrow)}>
        <Play size={12} fill="currentColor" /> Up next
      </div>
      <h2 {...stylex.props(styles.courseContinueTitle)}>{shortInstruction(action?.instruction)}</h2>
      <div {...stylex.props(styles.courseContinueMeta)}>
        <ActionUnit units={units} action={action} />
        <span>
          <Clock3 size={14} /> {minutes} min
        </span>
      </div>
      <Button size="lg" href={`/study?course_id=${data.course.id}`} endContent={<ArrowRight size={16} />}>
        Start session
      </Button>
    </article>
  );
}

function ProgressCard({ data }: { data: CourseDetailPayload }) {
  const progress = data.course_blueprint?.progress || {};
  const percent = Math.round(Number(progress.percent || 0));
  return (
    <aside {...stylex.props(styles.courseProgressCard)}>
      <div {...stylex.props(styles.courseCardEyebrow)}>
        <Gauge size={14} /> Course progress
      </div>
      <strong {...stylex.props(styles.courseProgressValue)}>{percent}%</strong>
      <div {...stylex.props(styles.courseProgressTrack)} aria-label={`${percent}% complete`}>
        <span {...stylex.props(styles.progressFill(`${Math.min(100, percent)}%`))} />
      </div>
      <p {...stylex.props(styles.courseProgressSummary)}>
        {progress.completed_actions || 0} of {progress.total_actions || 0} actions complete
      </p>
    </aside>
  );
}

function UnitPreview({ unit, index }: { unit: RoadmapUnit; index: number }) {
  const completed = Number(unit.completed_actions || 0);
  const total = Number(unit.total_actions || 0);
  const percent = total ? Math.round((completed / total) * 100) : 0;
  return (
    <li {...stylex.props(styles.courseUnitPreviewItem, index % 2 === 0 && styles.courseUnitPreviewItemOdd)}>
      <span {...stylex.props(styles.courseUnitIndex)}>{index + 1}</span>
      <div {...stylex.props(styles.courseUnitPreviewMain)}>
        <strong {...stylex.props(styles.courseUnitPreviewTitle)}>{unit.title}</strong>
        <div {...stylex.props(styles.courseUnitPreviewTrack)}>
          <span {...stylex.props(styles.progressFill(`${percent}%`))} />
        </div>
      </div>
      <span {...stylex.props(styles.courseUnitPreviewMeta)}>
        {completed}/{total}
      </span>
    </li>
  );
}

function RoadmapPreview({ data, onOpen }: { data: CourseDetailPayload; onOpen: () => void }) {
  const units = data.course_blueprint?.blueprint?.units || [];
  return (
    <section {...stylex.props(styles.courseOverviewSection)}>
      <header {...stylex.props(styles.roadmapPreviewHeader)}>
        <div {...stylex.props(styles.roadmapPreviewHeading)}>
          <span {...stylex.props(styles.roadmapPreviewCategory)}>Roadmap</span>
          <strong {...stylex.props(styles.roadmapPreviewTitle)}>
            {data.course_blueprint?.total_units ?? units.length} topics
          </strong>
        </div>
        <Button variant="ghost" size="sm" onClick={onOpen} endContent={<ArrowRight size={16} />}>
          View all
        </Button>
      </header>
      {units.length ? (
        <ol {...stylex.props(styles.courseUnitPreviewList)}>
          {units.slice(0, 6).map((unit, index) => (
            <UnitPreview unit={unit} index={index} key={unit.key || unit.title} />
          ))}
        </ol>
      ) : (
        <div {...stylex.props(styles.coursePanelEmpty)}>
          <RoadmapReadinessNote blueprint={data.course_blueprint} />
        </div>
      )}
    </section>
  );
}

export function CourseOverview({ data, onOpenRoadmap }: { data: CourseDetailPayload; onOpenRoadmap: () => void }) {
  return (
    <div {...stylex.props(styles.courseOverviewDashboard)}>
      <div {...stylex.props(styles.courseOverviewHero)}>
        <ContinueCard data={data} />
        <ProgressCard data={data} />
      </div>
      <RoadmapPreview data={data} onOpen={onOpenRoadmap} />
    </div>
  );
}

export function RecentChanges({ items = [], courseId }: { items?: CourseTimelineItem[]; courseId: string | number }) {
  const groups = timelineToActivityGroups(items, courseId);
  return (
    <section {...stylex.props(styles.courseActivityTab)} aria-labelledby="course-updates-title">
      <h2 id="course-updates-title" {...stylex.props(commonStyles.srOnly)}>
        Course updates
      </h2>
      <ActivityList groups={groups} filter="all" />
    </section>
  );
}
