import * as stylex from '@stylexjs/stylex';
import * as React from 'react';
import { BookOpen, ChevronDown, Clock3, FileText, Gauge, GraduationCap, Sparkles, Target } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { useFileGuide } from './useFileGuide';
import { GuideStatus } from './guideStatus';
import { readableValue } from '@/lib/display';
import type { FileInsight, FileInsightAi } from '@/lib/types';
import { media } from '@/styles/constants.stylex';
import { colors, layout, typography } from '@/styles/tokens.stylex';

const styles = stylex.create({
  fileInsight: {
    gap: '.5rem',
    paddingBlock: '.25rem',
    display: 'flex',
    flexDirection: 'column',
    marginInlineStart: '1.75rem',
  },
  fileInsightTopline: {
    gap: '.75rem',
    alignItems: 'center',
    display: 'flex',
    justifyContent: 'space-between',
  },
  fileInsightMeta: {
    gap: '.75rem',
    alignItems: 'center',
    color: colors.textSecondary,
    display: 'flex',
    fontSize: typography.sizeXs,
  },
  fileInsightActions: {
    gap: '.5rem',
    alignItems: 'center',
    display: 'flex',
  },
  fileReadLink: {
    borderColor: colors.border,
    borderRadius: layout.radius,
    borderStyle: 'solid',
    borderWidth: 1,
    gap: '.35rem',
    paddingBlock: '.25rem',
    paddingInline: '.55rem',
    textDecoration: 'none',
    alignItems: 'center',
    backgroundColor: { default: colors.surfaceRaised, ':hover': colors.surfaceHover },
    color: colors.textPrimary,
    display: 'inline-flex',
    fontSize: typography.sizeXs,
  },
  fileGuideToggle: {
    gap: '.35rem',
    fontSize: typography.sizeXs,
  },
  fileGuideChevron: {
    transitionDuration: '150ms',
    transitionProperty: 'transform',
    height: '.85rem',
    width: '.85rem',
  },
  fileGuideChevronOpen: {
    transform: 'rotate(180deg)',
  },
  fileGuidePanel: {
    padding: '.875rem',
    borderColor: colors.border,
    borderRadius: layout.radius,
    borderStyle: 'solid',
    borderWidth: 1,
    gap: '.75rem',
    backgroundColor: colors.surface,
    display: 'grid',
  },
  fileGuideSummary: {
    margin: 0,
    color: colors.textPrimary,
    fontSize: typography.sizeSm,
    lineHeight: 1.5,
  },
  fileGuideSignals: {
    gap: '.5rem',
    display: 'flex',
    flexWrap: 'wrap',
  },
  fileGuideAction: {
    padding: '.65rem',
    borderRadius: layout.radius,
    gap: '.5rem',
    alignItems: 'flex-start',
    backgroundColor: colors.surfaceRaised,
    display: 'flex',
  },
  fileGuideCore: {
    gap: '.75rem',
    display: 'grid',
    gridTemplateColumns: {
      default: 'repeat(2, minmax(0, 1fr))',
      [media.mobile]: '1fr',
    },
  },
  fileGuideList: {
    margin: 0,
  },
  fileGuideLens: {
    margin: 0,
  },
  fileGuideTopics: {
    gap: '.35rem',
    display: 'flex',
    flexWrap: 'wrap',
  },
  fileGuideRelated: {
    gap: '.35rem',
    display: 'grid',
    marginTop: '.5rem',
  },
  fileGuideMore: {
    marginTop: '.5rem',
  },
});

function InsightMeta({ insight }: { insight: FileInsight }) {
  return (
    <div {...stylex.props(styles.fileInsightMeta)}>
      {insight.page_count ? (
        <span>
          <FileText /> {insight.page_count} {insight.unit_name || 'pages'}
        </span>
      ) : null}
      {insight.reading_minutes ? (
        <span>
          <Clock3 /> ~{insight.reading_minutes} min
        </span>
      ) : null}
      {insight.complexity_label ? (
        <span title="Text density">
          <Gauge /> {insight.complexity_label}
        </span>
      ) : null}
    </div>
  );
}

function GuideSignals({ ai }: { ai: FileInsightAi }) {
  const signals = [
    ['Priority', ai.importance],
    ['Difficulty', ai.difficulty],
    ['Assessment', ai.assessment_relevance],
    ['Material', ai.material_type],
  ].filter((signal): signal is [string, string] => Boolean(signal[1]));
  if (!signals.length) return null;
  return (
    <div {...stylex.props(styles.fileGuideSignals)} aria-label="Study signals">
      {signals.map(([label, value]) => (
        <span key={label}>
          <small>{label}</small> {value.replaceAll('_', ' ')}
        </span>
      ))}
    </div>
  );
}

function GuideAction({ action }: { action?: string }) {
  if (!action) return null;
  return (
    <div {...stylex.props(styles.fileGuideAction)}>
      <Target aria-hidden="true" />
      <div>
        <span>Do this next</span>
        <p>{readableValue(action)}</p>
      </div>
    </div>
  );
}

function GuideList({ title, values = [] }: { title: string; values?: string[] }) {
  if (!values.length) return null;
  return (
    <section {...stylex.props(styles.fileGuideList)}>
      <h4>{title}</h4>
      <ul>
        {values.map((value, index) => (
          <li key={`${title}-${index}`}>{readableValue(value)}</li>
        ))}
      </ul>
    </section>
  );
}

function GuideCore({ ai }: { ai: FileInsightAi }) {
  const examLens = ai.assessment_reason || ai.importance_reason || ai.course_role;
  return (
    <div {...stylex.props(styles.fileGuideCore)}>
      <GuideList title="Learn" values={ai.learning_objectives?.slice(0, 6)} />
      {examLens ? (
        <section {...stylex.props(styles.fileGuideLens)}>
          <h4>
            <GraduationCap /> Exam lens
          </h4>
          <p>{readableValue(examLens)}</p>
        </section>
      ) : null}
    </div>
  );
}

function GuideTopics({ topics = [] }: { topics?: string[] }) {
  if (!topics.length) return null;
  return (
    <div {...stylex.props(styles.fileGuideTopics)} aria-label="Topics">
      {topics.slice(0, 10).map((topic) => (
        <span key={topic}>{readableValue(topic)}</span>
      ))}
    </div>
  );
}

function RelatedMaterials({ items = [] }: { items?: FileInsightAi['related_materials'] }) {
  if (!items.length) return null;
  return (
    <section {...stylex.props(styles.fileGuideRelated)}>
      <h4>Related material</h4>
      {items.map((item, index) => (
        <a
          href={`/files${encodeURI(item.path || '')}`}
          target="_blank"
          rel="noopener noreferrer"
          key={item.path || `related-${index}`}
        >
          <FileText /> {readableValue(item.name)}
        </a>
      ))}
    </section>
  );
}

function GuideMore({ ai }: { ai: FileInsightAi }) {
  const hasMore =
    Boolean(ai.prerequisites?.length) ||
    Boolean(ai.visual_content?.length) ||
    Boolean(ai.notable_items?.length) ||
    Boolean(ai.related_materials?.length) ||
    Boolean(ai.difficulty_reason || ai.overlap);
  if (!hasMore) return null;
  return (
    <details {...stylex.props(styles.fileGuideMore)}>
      <summary>More context</summary>
      <div>
        <GuideList title="Prerequisites" values={ai.prerequisites} />
        <GuideList title="Visuals worth reviewing" values={ai.visual_content} />
        <GuideList title="Key details" values={ai.notable_items} />
        {ai.difficulty_reason ? <p>{readableValue(ai.difficulty_reason)}</p> : null}
        {ai.overlap ? <p>{readableValue(ai.overlap)}</p> : null}
        <RelatedMaterials items={ai.related_materials} />
      </div>
    </details>
  );
}

function FileGuide({ ai, panelId }: { ai: FileInsightAi; panelId: string }) {
  return (
    <div {...stylex.props(styles.fileGuidePanel)} id={panelId}>
      {ai.summary ? <p {...stylex.props(styles.fileGuideSummary)}>{readableValue(ai.summary)}</p> : null}
      <GuideSignals ai={ai} />
      <GuideAction action={ai.recommended_action} />
      <GuideCore ai={ai} />
      <GuideTopics topics={ai.topics} />
      <GuideMore ai={ai} />
    </div>
  );
}

function InsightActions({
  insight,
  open,
  onToggle,
  panelId,
}: {
  insight: FileInsight;
  open: boolean;
  onToggle: () => void;
  panelId: string;
}) {
  const canRead = insight.id && ['pdf', 'image'].includes(insight.document_kind || '');
  return (
    <div {...stylex.props(styles.fileInsightActions)}>
      <GuideStatus insight={insight} />
      {canRead ? (
        <a
          {...stylex.props(styles.fileReadLink)}
          href={`/study/session?course_id=${insight.course_id}&document_id=${encodeURIComponent(insight.id || '')}`}
        >
          <BookOpen /> Read
        </a>
      ) : null}
      {insight.guide_available || insight.ai?.summary ? (
        <Button
          type="button"
          variant="outline"
          size="sm"
          {...stylex.props(styles.fileGuideToggle)}
          aria-expanded={open}
          aria-controls={panelId}
          onClick={onToggle}
        >
          <Sparkles /> Guide{' '}
          <ChevronDown {...stylex.props(styles.fileGuideChevron, open && styles.fileGuideChevronOpen)} />
        </Button>
      ) : null}
    </div>
  );
}

function LazyFileGuide({ insight, open, panelId }: { insight: FileInsight; open: boolean; panelId: string }) {
  const { ai, loaded, error, retry } = useFileGuide(insight, open);
  if (!open) return null;
  if (error)
    return (
      <p role="alert">
        Guide could not load. <Button onClick={retry}>Try again</Button>
      </p>
    );
  if (!loaded) return <p role="status">Loading guide…</p>;
  if (!ai) return <p role="status">No current study guide is available for this file.</p>;
  return <FileGuide ai={ai} panelId={panelId} />;
}

export function Insight({ insight }: { insight: FileInsight | undefined }) {
  const [open, setOpen] = React.useState(false);
  const generatedId = React.useId();
  if (!insight) return null;
  const panelId = `file-guide-${generatedId.replaceAll(':', '')}`;
  return (
    <div {...stylex.props(styles.fileInsight)}>
      <div {...stylex.props(styles.fileInsightTopline)}>
        <InsightMeta insight={insight} />
        <InsightActions insight={insight} open={open} onToggle={() => setOpen((value) => !value)} panelId={panelId} />
      </div>
      <LazyFileGuide key={insight.id} insight={insight} open={open} panelId={panelId} />
    </div>
  );
}
