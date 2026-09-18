import * as stylex from '@stylexjs/stylex';
import { Icon } from '@/components/Icon';
import * as React from 'react';
import type { CourseSummary, RoadmapBlueprint } from '@/lib/types';
import { readableReason } from '@/lib/display';
import { colors, effects, layout, typography } from '@/styles/tokens.stylex';

const styles = stylex.create({
  courseRoadmap: {
    gap: '.85rem',
    display: 'grid',
    marginBottom: '1.25rem',
  },
  roadmapHead: {
    padding: '1.25rem',
    borderColor: colors.border,
    borderRadius: layout.radiusLarge,
    borderStyle: 'solid',
    borderWidth: 1,
    gap: '1.25rem',
    alignItems: 'flex-start',
    backgroundColor: colors.surface,
    boxShadow: effects.shadow,
    display: 'flex',
    justifyContent: 'space-between',
  },
  roadmapTitle: {
    margin: 0,
  },
  roadmapSubtitle: {
    margin: 0,
    color: colors.textSecondary,
    fontSize: '0.8125rem',
    marginTop: '.375rem',
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
  roadmapWaiting: {
    padding: '1.25rem',
    borderColor: colors.border,
    borderRadius: layout.radiusLarge,
    borderStyle: 'solid',
    borderWidth: 1,
    backgroundColor: colors.surface,
    boxShadow: effects.shadow,
    marginBottom: '.875rem',
  },
  roadmapMeta: {
    color: colors.textSecondary,
    fontSize: '0.8125rem',
  },
  roadmapReadinessNote: {
    padding: '.7rem',
    borderColor: colors.border,
    borderRadius: layout.radius,
    borderStyle: 'solid',
    borderWidth: 1,
    backgroundColor: colors.surfaceRaised,
    marginTop: '.75rem',
  },
});

interface RoadmapWaitingProps {
  blueprint?: RoadmapBlueprint;
  course: CourseSummary;
}

export function RoadmapWaiting({ blueprint }: RoadmapWaitingProps) {
  const readiness = blueprint?.readiness || {};
  return (
    <section {...stylex.props(styles.courseRoadmap)} id="roadmap" aria-labelledby="course-roadmap-title">
      <RoadmapWaitingHeader state={String(readiness.state || 'waiting')} />
      <RoadmapWaitingBody readiness={readiness} />
    </section>
  );
}

function RoadmapWaitingHeader({ state }: { state: string }) {
  return (
    <header {...stylex.props(styles.roadmapHead)}>
      <div>
        <h2 id="course-roadmap-title" {...stylex.props(styles.roadmapTitle)}>
          <Icon name="signpost-split" aria-hidden="true" /> Study roadmap
        </h2>
        <p {...stylex.props(styles.roadmapSubtitle)}>
          An evidence-grounded route through lectures, exercises, community material, and prior exams.
        </p>
      </div>
      <span {...stylex.props(styles.chip)}>{state.replaceAll('_', ' ')}</span>
    </header>
  );
}

function RoadmapWaitingBody({ readiness }: { readiness: NonNullable<RoadmapBlueprint['readiness']> }) {
  return (
    <div {...stylex.props(styles.roadmapWaiting)}>
      <h3>
        {String(readiness.state || 'Not ready')
          .replaceAll('_', ' ')
          .replace(/\b\w/g, (c) => c.toUpperCase())}
      </h3>
      <p>{readableReason(readiness.reason, 'Run a course check and indexing will prepare the roadmap.')}</p>
      {readiness.source_readiness && (
        <p {...stylex.props(styles.roadmapMeta)}>
          {readableReason(readiness.source_readiness, 'Source coverage is still being reconciled.')}
        </p>
      )}
      <p>
        Files remain available in the Files tab. Reopen the Roadmap tab after its evidence is indexed and a validated
        synthesis is ready.
      </p>
    </div>
  );
}

export function RoadmapReadinessNote({ blueprint }: { blueprint?: RoadmapBlueprint }) {
  const readiness = blueprint?.readiness || {};
  const source = readiness.source_readiness;
  const ready = Number(source?.ready_document_insights || 0);
  const documents = Number(source?.ready_documents || 0);
  const activeFiles = Number(source?.active_extraction_jobs || 0) + Number(source?.active_document_enrichments || 0);
  const readyPages = Number(source?.page_enrichments?.ready || 0);
  const activePages = Number(source?.active_page_enrichments || 0);
  const pageTotal = readyPages + activePages;
  const failed =
    Number(source?.failed_documents || 0) +
    Number(source?.failed_document_enrichments || 0) +
    Number(source?.failed_page_enrichments || 0);
  const sourceReason = source ? readableReason(source, '') : '';
  return (
    <div {...stylex.props(styles.roadmapReadinessNote)}>
      <strong>
        {String(readiness.state || 'missing')
          .replaceAll('_', ' ')
          .replace(/\b\w/g, (c) => c.toUpperCase())}
      </strong>
      <p>{readableReason(readiness.reason, 'No roadmap generation has been created yet.')}</p>
      {source && (
        <p {...stylex.props(styles.roadmapMeta)}>
          {documents ? `${ready}/${documents} file guides ready` : 'No ready source files reported'}
          {pageTotal ? ` · ${readyPages}/${pageTotal} pages analyzed` : ''}
          {activeFiles ? ` · ${activeFiles} file operation(s) active` : ''}
          {failed ? ` · ${failed} failure(s)` : ''}
        </p>
      )}
      {sourceReason ? <p {...stylex.props(styles.roadmapMeta)}>{sourceReason}</p> : null}
    </div>
  );
}
