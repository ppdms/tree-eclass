import { useDeferredResource } from '@/lib/useDeferredResource';
import { Button } from '@/components/ui/button';
import * as stylex from '@stylexjs/stylex';
import * as React from 'react';
import { ChevronDown, Clock3 } from 'lucide-react';
import type { RoadmapBlueprint, RoadmapUnit } from '@/lib/types';
import { media } from '@/styles/constants.stylex';
import { colors, layout, typography } from '@/styles/tokens.stylex';
import { Evidence } from './evidence';
import { ActionCard } from './actionCard';

const styles = stylex.create({
  courseRoadmapUnits: {
    borderColor: colors.border,
    borderRadius: layout.radiusLarge,
    borderStyle: 'solid',
    borderWidth: 1,
    overflow: 'hidden',
    backgroundColor: colors.surface,
  },
  courseRoadmapUnit: {
    borderTopColor: colors.border,
    borderTopStyle: 'solid',
    borderTopWidth: 1,
  },
  courseRoadmapUnitOpen: {
    backgroundColor: `color-mix(in srgb, ${colors.surfaceRaised} 45%, ${colors.surface})`,
  },
  courseRoadmapUnitSummary: {
    gap: '.8rem',
    listStyle: 'none',
    paddingBlock: '.75rem',
    paddingInline: '1rem',
    alignItems: 'center',
    cursor: 'pointer',
    display: 'grid',
    gridTemplateColumns: {
      default: '2rem minmax(0, 1fr) auto auto 1rem',
      [media.tablet]: '2rem minmax(0, 1fr) 1rem',
    },
    minHeight: '4.2rem',
  },
  courseRoadmapStep: {
    borderColor: colors.borderLight,
    borderRadius: '999px',
    borderStyle: 'solid',
    borderWidth: 1,
    placeItems: 'center',
    color: colors.textSecondary,
    display: 'grid',
    fontSize: typography.sizeXs,
    fontVariantNumeric: 'tabular-nums',
    height: '1.8rem',
    width: '1.8rem',
  },
  courseRoadmapUnitTitle: {
    gap: '.5rem',
    display: 'grid',
    minWidth: 0,
  },
  courseRoadmapUnitTitleText: {
    overflow: 'hidden',
    fontSize: '0.85rem',
    fontWeight: 600,
    textOverflow: 'ellipsis',
    whiteSpace: 'nowrap',
  },
  progressTrack: {
    borderRadius: '999px',
    overflow: 'hidden',
    backgroundColor: colors.surfaceHover,
    height: '.3rem',
    width: '100%',
  },
  progressFill: (width: string) => ({
    borderRadius: 'inherit',
    backgroundColor: colors.success,
    display: 'block',
    height: '100%',
    width,
  }),
  coursePriority: {
    borderColor: colors.border,
    borderRadius: '999px',
    borderStyle: 'solid',
    borderWidth: 1,
    paddingBlock: '.2rem',
    paddingInline: '.45rem',
    color: colors.textSecondary,
    display: {
      default: 'inline-block',
      [media.tablet]: 'none',
    },
    fontSize: typography.sizeXs,
    textTransform: 'capitalize',
  },
  coursePriorityCritical: {
    borderColor: `color-mix(in srgb, ${colors.danger} 35%, ${colors.border})`,
    color: colors.danger,
    display: 'inline-block',
  },
  coursePriorityHigh: {
    color: colors.warning,
    display: 'inline-block',
  },
  coursePriorityNormal: {
    display: 'inline-block',
  },
  courseRoadmapUnitMeta: {
    gap: '.35rem',
    gridColumn: {
      default: 'auto',
      [media.tablet]: '2',
    },
    gridRow: {
      default: 'auto',
      [media.tablet]: '2',
    },
    alignItems: 'center',
    color: colors.textSecondary,
    display: 'flex',
    fontSize: typography.sizeXs,
    fontVariantNumeric: 'tabular-nums',
    whiteSpace: 'nowrap',
  },
  courseRoadmapChevron: {
    gridColumn: {
      default: 'auto',
      [media.tablet]: '3',
    },
    gridRow: {
      default: 'auto',
      [media.tablet]: '1 / 3',
    },
    color: colors.textSecondary,
    transitionDuration: '.15s',
    transitionProperty: 'transform',
    width: '.9rem',
  },
  courseRoadmapChevronOpen: {
    transform: 'rotate(180deg)',
  },
  courseRoadmapUnitBody: {
    paddingBlockEnd: '1rem',
    paddingBlockStart: 0,
    paddingInlineEnd: '1rem',
    paddingInlineStart: {
      default: '3.8rem',
      [media.tablet]: '1rem',
    },
  },
  courseRoadmapObjective: {
    marginInline: 0,
    color: colors.textSecondary,
    fontSize: '.75rem',
    marginBlockEnd: '.8rem',
    marginBlockStart: 0,
    maxWidth: '65rem',
  },
  courseRoadmapActions: {
    borderColor: colors.border,
    borderRadius: '.7rem',
    borderStyle: 'solid',
    borderWidth: 1,
    overflow: 'hidden',
    backgroundColor: colors.surface,
    marginTop: '.65rem',
  },
});

function priorityStyle(priority: string | undefined) {
  if (priority === 'critical') return styles.coursePriorityCritical;
  if (priority === 'high') return styles.coursePriorityHigh;
  return styles.coursePriorityNormal;
}

function UnitSummary({ unit, index, open }: { unit: RoadmapUnit; index: number; open: boolean }) {
  const completed = Number(unit.completed_actions || 0);
  const total = Number(unit.total_actions || 0);
  const percent = total ? Math.round((completed / total) * 100) : 0;
  return (
    <summary {...stylex.props(styles.courseRoadmapUnitSummary)}>
      <span {...stylex.props(styles.courseRoadmapStep)}>{completed === total && total ? '✓' : index + 1}</span>
      <div {...stylex.props(styles.courseRoadmapUnitTitle)}>
        <strong {...stylex.props(styles.courseRoadmapUnitTitleText)}>{unit.title}</strong>
        <div {...stylex.props(styles.progressTrack)}>
          <i {...stylex.props(styles.progressFill(`${percent}%`))} />
        </div>
      </div>
      <span {...stylex.props(styles.coursePriority, priorityStyle(unit.priority ?? ''))}>{unit.priority}</span>
      <span {...stylex.props(styles.courseRoadmapUnitMeta)}>
        {completed}/{total} <i>·</i> <Clock3 size={13} /> {unit.estimated_minutes}m
      </span>
      <ChevronDown {...stylex.props(styles.courseRoadmapChevron, open && styles.courseRoadmapChevronOpen)} />
    </summary>
  );
}

function UnitBody({ unit, courseId, revision }: { unit: RoadmapUnit; courseId: string | number; revision?: string }) {
  return (
    <div {...stylex.props(styles.courseRoadmapUnitBody)}>
      {unit.objective && <p {...stylex.props(styles.courseRoadmapObjective)}>{unit.objective}</p>}
      <Evidence links={unit.evidence_links} courseId={courseId} />
      <div {...stylex.props(styles.courseRoadmapActions)}>
        {(unit.actions || []).map((action) => (
          <ActionCard key={action.action_id || action.id} action={action} courseId={courseId} revision={revision} />
        ))}
      </div>
    </div>
  );
}

function DeferredUnit({
  unit,
  courseId,
  revision,
  open,
}: {
  unit: RoadmapUnit;
  courseId: string | number;
  revision?: string;
  open: boolean;
}) {
  const base = `/api/v1/courses/${courseId}/roadmap/units/${encodeURIComponent(unit.key || '')}`;
  const url = `${base}?revision=${encodeURIComponent(revision || '')}`;
  const { data, error, retry } = useDeferredResource<{ unit: RoadmapUnit }>(url, open && !unit.actions);
  React.useEffect(() => {
    window.addEventListener('tree:course-progress', retry);
    return () => window.removeEventListener('tree:course-progress', retry);
  }, [retry]);
  if (!open) return null;
  if (error)
    return (
      <p role="alert">
        Topic could not load. <Button onClick={retry}>Try again</Button>
      </p>
    );
  if (!unit.actions && !data) return <p role="status">Loading topic…</p>;
  return <UnitBody unit={data?.unit || unit} courseId={courseId} revision={revision} />;
}

function RoadmapUnitItem({
  unit,
  index,
  initialOpen,
  courseId,
  revision,
}: {
  unit: RoadmapUnit;
  index: number;
  initialOpen: boolean;
  courseId: string | number;
  revision?: string;
}) {
  const [open, setOpen] = React.useState(initialOpen);
  return (
    <details
      {...stylex.props(styles.courseRoadmapUnit, open && styles.courseRoadmapUnitOpen)}
      open={open}
      onToggle={(event) => setOpen(event.currentTarget.open)}
    >
      <UnitSummary unit={unit} index={index} open={open} />
      <DeferredUnit unit={unit} courseId={courseId} revision={revision} open={open} />
    </details>
  );
}

export function Units({
  roadmap,
  course,
  revision,
}: {
  roadmap: NonNullable<RoadmapBlueprint['blueprint']>;
  course: { id: string | number };
  revision?: string;
}) {
  const units = roadmap?.units || [];
  const current = units.findIndex((unit) => Number(unit.completed_actions || 0) < Number(unit.total_actions || 0));
  return (
    <div {...stylex.props(styles.courseRoadmapUnits)}>
      {units.map((unit, index) => (
        <RoadmapUnitItem
          key={unit.key || unit.title}
          unit={unit}
          index={index}
          initialOpen={index === Math.max(0, current)}
          courseId={course.id}
          revision={revision}
        />
      ))}
    </div>
  );
}
