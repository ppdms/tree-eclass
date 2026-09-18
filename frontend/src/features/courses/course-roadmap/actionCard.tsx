import * as stylex from '@stylexjs/stylex';
import * as React from 'react';
import { Check, Circle } from 'lucide-react';
import { readableValue } from '@/lib/display';
import type { RoadmapAction } from '@/lib/types';
import { colors, typography } from '@/styles/tokens.stylex';
import { Evidence } from './evidence';
import { EventForm } from './eventForm';

const styles = stylex.create({
  courseRoadmapAction: {
    padding: '.9rem',
    gap: '.55rem',
    display: 'grid',
    gridTemplateColumns: '1.3rem minmax(0, 1fr)',
    borderTopColor: colors.border,
    borderTopStyle: 'solid',
    borderTopWidth: 1,
  },
  courseRoadmapActionCompleted: {
    opacity: 0.55,
  },
  courseActionStatus: {
    color: colors.success,
    display: 'inline-flex',
    paddingTop: '.15rem',
  },
  courseActionMain: {
    minWidth: 0,
  },
  courseActionInstruction: {
    fontSize: '0.8rem',
    lineHeight: 1.5,
    marginBlockEnd: '.7rem',
    marginBlockStart: '.35rem',
    maxWidth: '70rem',
  },
  courseActionMeta: {
    alignItems: 'center',
    color: colors.textSecondary,
    columnGap: '.7rem',
    display: 'flex',
    flexWrap: 'wrap',
    fontSize: typography.sizeXs,
    rowGap: '.3rem',
    textTransform: 'capitalize',
  },
});

export function ActionCard({
  action,
  courseId,
  revision,
}: {
  action: RoadmapAction;
  courseId: string | number;
  revision?: string;
}) {
  const [status, setStatus] = React.useState<string | undefined>(action.status);
  const completed = status === 'completed';
  const minutes = action.remaining_minutes || action.estimated_minutes || 15;
  return (
    <article
      {...stylex.props(styles.courseRoadmapAction, completed && styles.courseRoadmapActionCompleted)}
      data-study-action
    >
      <span {...stylex.props(styles.courseActionStatus)}>{completed ? <Check size={16} /> : <Circle size={16} />}</span>
      <div {...stylex.props(styles.courseActionMain)}>
        <div {...stylex.props(styles.courseActionMeta)}>
          <span>{readableValue(action.kind || action.action_type, 'Study')}</span>
          <span>{minutes} min</span>
          <Evidence
            links={action.evidence_links}
            courseId={courseId}
            actionId={action.action_id || String(action.id || '')}
          />
        </div>
        <p {...stylex.props(styles.courseActionInstruction)}>{readableValue(action.instruction)}</p>
        {!completed && <EventForm action={action} courseId={courseId} revision={revision} onStatus={setStatus} />}
      </div>
    </article>
  );
}
