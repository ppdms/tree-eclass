import * as stylex from '@stylexjs/stylex';
import { Icon } from '@/components/Icon';
import * as React from 'react';
import type { CourseSummary, CoverageGap, QuestionFamily, RoadmapBlueprint, RoadmapConflict } from '@/lib/types';
import { readableValue } from '@/lib/display';
import { media } from '@/styles/constants.stylex';
import { colors, layout, typography } from '@/styles/tokens.stylex';
import { Evidence } from './evidence';
import { DeferredSection } from './DeferredSection';

interface SectionProps {
  roadmap: Roadmap;
  course: CourseSummary;
  revision?: string;
}

type Roadmap = NonNullable<RoadmapBlueprint['blueprint']>;

const styles = stylex.create({
  roadmapCard: {
    margin: 0,
    borderColor: colors.border,
    borderRadius: '.75rem',
    borderStyle: 'solid',
    borderWidth: 1,
    backgroundColor: colors.surface,
  },
  strategySummary: {
    gap: '1rem',
    listStyle: 'none',
    paddingBlock: '0.9rem',
    paddingInline: '1rem',
    alignItems: 'center',
    cursor: 'pointer',
    display: 'flex',
    fontSize: '0.75rem',
    fontWeight: 600,
    justifyContent: 'space-between',
  },
  chevron: {
    color: colors.textSecondary,
    transitionDuration: '.15s',
    transitionProperty: 'transform',
  },
  chevronOpen: {
    transform: 'rotate(180deg)',
  },
  roadmapStrategy: {
    padding: '1rem',
    borderTopColor: colors.border,
    borderTopStyle: 'solid',
    borderTopWidth: 1,
    marginTop: '.5rem',
    paddingTop: 0,
  },
  roadmapStrategyMain: {
    minWidth: 0,
  },
  roadmapStrategyLabel: {
    gap: '1rem',
    alignItems: 'center',
    display: 'flex',
    justifyContent: 'space-between',
    marginBottom: '.5rem',
    marginTop: '.75rem',
  },
  roadmapEyebrow: {
    color: colors.textSecondary,
    fontSize: typography.size2xs,
    fontWeight: typography.weightBold,
    letterSpacing: '.05em',
    textTransform: 'uppercase',
  },
  chip: {
    borderColor: colors.border,
    borderRadius: layout.radiusPill,
    borderStyle: 'solid',
    borderWidth: 1,
    paddingBlock: '.25rem',
    paddingInline: '.55rem',
    backgroundColor: colors.surfaceRaised,
    fontSize: typography.sizeXs,
    fontWeight: 550,
  },
  strategyTitle: {
    fontSize: typography.sizeBase,
    fontWeight: typography.weightSemibold,
    lineHeight: 1.375,
    marginBottom: '.625rem',
  },
  strategyDesc: {
    color: colors.textSecondary,
    fontSize: typography.sizeSm,
    lineHeight: typography.leadingRelaxed,
    marginTop: '.5rem',
  },
  roadmapMeta: {
    color: colors.textSecondary,
    fontSize: typography.sizeXs,
    marginTop: '.5rem',
  },
  roadmapEvidenceDetails: {
    borderTopColor: colors.border,
    borderTopStyle: 'solid',
    borderTopWidth: 1,
    marginTop: '.75rem',
    paddingTop: '.5rem',
  },
  evidenceSummary: {
    gap: '1rem',
    listStyle: 'none',
    alignItems: 'center',
    color: colors.textSecondary,
    cursor: 'pointer',
    display: 'flex',
    fontSize: typography.sizeXs,
    fontWeight: typography.weightSemibold,
    justifyContent: 'space-between',
  },
  roadmapSupportCard: {
    padding: '.875rem',
    borderColor: colors.border,
    borderRadius: '.75rem',
    borderStyle: 'solid',
    borderWidth: 1,
    backgroundColor: colors.surface,
  },
  roadmapSupportCardTitle: {
    marginInline: 0,
    fontSize: '0.85rem',
    fontWeight: 600,
    marginBlockEnd: '0.375rem',
    marginBlockStart: 0,
  },
  roadmapSupportCardText: {
    marginInline: 0,
    fontSize: '0.78rem',
    lineHeight: 1.45,
    marginBlockEnd: '0.4rem',
    marginBlockStart: 0,
  },
  roadmapSupportGrid: {
    gap: '.875rem',
    display: 'grid',
    gridTemplateColumns: {
      default: 'repeat(auto-fit, minmax(15rem, 1fr))',
      [media.mobile]: '1fr',
    },
  },
  supportSummary: {
    gap: '1rem',
    listStyle: 'none',
    paddingBlock: '0.9rem',
    paddingInline: '1rem',
    alignItems: 'center',
    cursor: 'pointer',
    display: 'flex',
    fontSize: '0.75rem',
    fontWeight: 600,
    justifyContent: 'space-between',
  },
  supportGridPadding: {
    paddingInline: '1.25rem',
    paddingBottom: '1.25rem',
  },
});

export function StrategySection({ roadmap, course, revision }: SectionProps) {
  const strategy = roadmap?.exam_strategy;
  const [open, setOpen] = React.useState(false);
  if (!strategy && !roadmap.deferred_sections?.strategy) return null;
  return (
    <details {...stylex.props(styles.roadmapCard)} onToggle={(e) => setOpen(e.currentTarget.open)}>
      <summary {...stylex.props(styles.strategySummary)}>
        Exam strategy and evidence{' '}
        <span {...stylex.props(styles.chevron, open && styles.chevronOpen)}>
          <Icon name="chevron-down" aria-hidden="true" />
        </span>
      </summary>
      {roadmap.deferred_sections ? (
        <DeferredSection courseId={course.id} revision={revision} section="strategy" open={open}>
          {(value) => <StrategyContent strategy={value.exam_strategy || {}} course={course} revision={revision} />}
        </DeferredSection>
      ) : strategy && open ? (
        <StrategyContent strategy={strategy} course={course} />
      ) : null}
    </details>
  );
}

function StrategyContent({
  strategy,
  course,
  revision,
}: {
  strategy: NonNullable<Roadmap['exam_strategy']>;
  course: CourseSummary;
  revision?: string;
}) {
  return (
    <div {...stylex.props(styles.roadmapStrategy)}>
      <StrategyLabel confidence={strategy.confidence} />
      <div {...stylex.props(styles.roadmapStrategyMain)}>
        <StrategyText strategy={strategy} course={course} revision={revision} />
      </div>
    </div>
  );
}

function StrategyLabel({ confidence }: { confidence: unknown }) {
  return (
    <div {...stylex.props(styles.roadmapStrategyLabel)}>
      <span {...stylex.props(styles.roadmapEyebrow)}>Exam strategy</span>
      <span {...stylex.props(styles.chip)}>{readableValue(String(confidence ?? 'Unknown'))} confidence</span>
    </div>
  );
}

function StrategyText({
  strategy,
  course,
  revision,
}: {
  strategy: NonNullable<Roadmap['exam_strategy']>;
  course: CourseSummary;
  revision?: string;
}) {
  return (
    <>
      <h3 {...stylex.props(styles.strategyTitle)}>{readableValue(strategy.objective)}</h3>
      <p {...stylex.props(styles.strategyDesc)}>{readableValue(strategy.approach)}</p>
      <p {...stylex.props(styles.roadmapMeta)}>
        Target reflected by this revision:{' '}
        {strategy.target_score_percent != null ? `${strategy.target_score_percent}%` : 'not specified'}
      </p>
      <StrategyEvidence links={strategy.evidence_links} courseId={course.id} revision={revision} />
    </>
  );
}

function StrategyEvidence({
  links,
  courseId,
  revision,
}: {
  links: NonNullable<Roadmap['exam_strategy']>['evidence_links'];
  courseId: CourseSummary['id'];
  revision?: string;
}) {
  const [open, setOpen] = React.useState(false);
  return (
    <details {...stylex.props(styles.roadmapEvidenceDetails)} onToggle={(e) => setOpen(e.currentTarget.open)}>
      <summary {...stylex.props(styles.evidenceSummary)}>
        <span>Show the evidence behind this strategy</span>
        <span {...stylex.props(styles.chevron, open && styles.chevronOpen)}>
          <Icon name="chevron-down" aria-hidden="true" />
        </span>
      </summary>
      {revision ? (
        <DeferredSection courseId={courseId} revision={revision} section="strategy-evidence" open={open}>
          {(value) => <Evidence links={value.exam_strategy?.evidence_links} courseId={courseId} />}
        </DeferredSection>
      ) : open ? (
        <Evidence links={links} courseId={courseId} />
      ) : null}
    </details>
  );
}

function QuestionFamilyCard({ family, course }: { family: QuestionFamily; course: CourseSummary }) {
  return (
    <article {...stylex.props(styles.roadmapSupportCard)}>
      <h3 {...stylex.props(styles.roadmapSupportCardTitle)}>{readableValue(family.name, 'Question family')}</h3>
      <p {...stylex.props(styles.roadmapSupportCardText)}>
        {readableValue(family.priority)} · {String(family.response_mode || '').replaceAll('_', ' ')} ·{' '}
        {readableValue(family.confidence)} confidence
      </p>
      <p {...stylex.props(styles.roadmapSupportCardText)}>
        Observed {String(family.observed_count ?? 0)}/{String(family.observed_out_of ?? 0)} papers
        {family.estimated_marks_percent ? ` · ~${String(family.estimated_marks_percent)}% of marks` : ''}
      </p>
      <Evidence links={family.evidence_links} courseId={course.id} />
    </article>
  );
}

function ConflictCard({ conflict, course }: { conflict: RoadmapConflict; course: CourseSummary }) {
  return (
    <article {...stylex.props(styles.roadmapSupportCard)}>
      <h3 {...stylex.props(styles.roadmapSupportCardTitle)}>Conflict</h3>
      <p {...stylex.props(styles.roadmapSupportCardText)}>{readableValue(conflict.summary)}</p>
      {Boolean(conflict.status) && (
        <p {...stylex.props(styles.roadmapMeta)}>Status: {readableValue(conflict.status)}</p>
      )}
      {Boolean(conflict.resolution) && (
        <p {...stylex.props(styles.roadmapMeta)}>Resolution: {readableValue(conflict.resolution)}</p>
      )}
      <Evidence links={conflict.evidence_links} courseId={course.id} />
    </article>
  );
}

function GapCard({ gap, course }: { gap: CoverageGap; course: CourseSummary }) {
  return (
    <article {...stylex.props(styles.roadmapSupportCard)}>
      <h3 {...stylex.props(styles.roadmapSupportCardTitle)}>Coverage gap</h3>
      <p {...stylex.props(styles.roadmapSupportCardText)}>{readableValue(gap.summary)}</p>
      {Boolean(gap.recommended_action) && (
        <p {...stylex.props(styles.roadmapMeta)}>Next step: {readableValue(gap.recommended_action)}</p>
      )}
      <Evidence links={gap.evidence_links} courseId={course.id} />
    </article>
  );
}

export function SupportGrid({ roadmap, course, revision }: SectionProps) {
  const families = roadmap?.question_families || [];
  const conflicts = roadmap?.conflicts || [];
  const gaps = roadmap?.coverage_gaps || [];
  const [open, setOpen] = React.useState(false);
  if (!families.length && !conflicts.length && !gaps.length && !roadmap.deferred_sections?.support) return null;
  return (
    <details {...stylex.props(styles.roadmapCard)} onToggle={(e) => setOpen(e.currentTarget.open)}>
      <summary {...stylex.props(styles.supportSummary)}>
        <span>Exam patterns, conflicts, and evidence gaps</span>
        <span {...stylex.props(styles.chevron, open && styles.chevronOpen)}>
          <Icon name="chevron-down" aria-hidden="true" />
        </span>
      </summary>
      {roadmap.deferred_sections ? (
        <DeferredSection courseId={course.id} revision={revision} section="support" open={open}>
          {(value) => <SupportContent roadmap={value} course={course} />}
        </DeferredSection>
      ) : open ? (
        <SupportContent roadmap={roadmap} course={course} />
      ) : null}
    </details>
  );
}

function SupportContent({ roadmap, course }: SectionProps) {
  const families = roadmap.question_families || [];
  const conflicts = roadmap.conflicts || [];
  const gaps = roadmap.coverage_gaps || [];
  return (
    <div {...stylex.props(styles.roadmapSupportGrid, styles.supportGridPadding)}>
      {families.map((family, index) => (
        <QuestionFamilyCard family={family} course={course} key={index} />
      ))}
      {conflicts.map((conflict, index) => (
        <ConflictCard conflict={conflict} course={course} key={`conflict-${index}`} />
      ))}
      {gaps.map((gap, index) => (
        <GapCard gap={gap} course={course} key={`gap-${index}`} />
      ))}
    </div>
  );
}
