import * as stylex from '@stylexjs/stylex';
import { Sparkles } from 'lucide-react';
import type { FileInsight } from '@/lib/types';
import { colors, layout, typography } from '@/styles/tokens.stylex';

const styles = stylex.create({
  guideStatus: {
    borderRadius: layout.radiusPill,
    gap: '.3rem',
    paddingBlock: '.25rem',
    paddingInline: '.45rem',
    alignItems: 'center',
    color: colors.textSecondary,
    display: 'inline-flex',
    fontSize: typography.sizeXs,
    whiteSpace: 'nowrap',
  },
  guideStatusReady: {
    backgroundColor: colors.surfaceSuccess,
    color: colors.success,
  },
  guideStatusWaiting: {
    backgroundColor: colors.surfaceWarning,
    color: colors.warning,
  },
  guideStatusFailed: {
    backgroundColor: colors.surfaceDanger,
    color: colors.danger,
  },
});

type GuideStatusState = {
  label: string;
  detail: string;
  tone: 'ready' | 'waiting' | 'failed';
};

function sourceStatus(insight: FileInsight): GuideStatusState | null {
  if (insight.source_status === 'unsupported') {
    return {
      label: 'File not supported',
      detail: insight.source_diagnostic_reason || 'The source file type is not supported by the extractor.',
      tone: 'failed',
    };
  }
  if (['failed', 'skipped_limit'].includes(insight.source_status || '')) {
    return {
      label: 'Source indexing failed',
      detail: insight.source_error || insight.source_diagnostic_reason || 'The source could not be indexed.',
      tone: 'failed',
    };
  }
  if (['pending', 'running'].includes(insight.source_status || '')) {
    return {
      label: 'Source indexing',
      detail: 'The file is still being extracted before guide generation can start.',
      tone: 'waiting',
    };
  }
  if (insight.source_status === 'external') {
    return {
      label: 'External source',
      detail: 'This auxiliary source is available as course material but is not part of the indexed guide set.',
      tone: 'waiting',
    };
  }
  return null;
}

function generationStatus(insight: FileInsight): GuideStatusState {
  if (!insight.ai_processing_enabled) {
    return {
      label: 'Guides disabled',
      detail: 'AI guide generation is disabled or has no configured provider.',
      tone: 'waiting',
    };
  }
  if (insight.enrichment_status === 'failed') {
    return {
      label: 'Guide failed',
      detail: insight.enrichment_error || 'The last guide generation attempt failed. Retry failed work in Settings.',
      tone: 'failed',
    };
  }
  if (insight.enrichment_status === 'running') {
    return { label: 'Guide processing', detail: 'A worker is generating the guide now.', tone: 'waiting' };
  }
  if (insight.enrichment_status === 'pending') {
    return { label: 'Guide queued', detail: 'The guide is waiting for an available worker.', tone: 'waiting' };
  }
  if (
    insight.page_analysis_enabled &&
    ['pdf', 'image'].includes(insight.document_kind || '') &&
    Number(insight.page_analysis_ready || 0) < Number(insight.page_count || 0)
  ) {
    const ready = Number(insight.page_analysis_ready || 0);
    const total = Number(insight.page_count || 0);
    return {
      label: 'Waiting for page scans',
      detail: `Visual analysis is ${ready}/${total} pages ready.`,
      tone: 'waiting',
    };
  }
  return {
    label: 'Guide not queued',
    detail: 'No current guide generation exists for this indexed file.',
    tone: 'waiting',
  };
}

function guideStatus(insight: FileInsight): GuideStatusState {
  if (insight.guide_available || insight.ai?.summary)
    return { label: 'Guide ready', detail: 'A study guide is available for this file.', tone: 'ready' };
  return sourceStatus(insight) || generationStatus(insight);
}

export function GuideStatus({ insight }: { insight: FileInsight }) {
  const state = guideStatus(insight);
  return (
    <span
      title={state.detail}
      {...stylex.props(
        styles.guideStatus,
        state.tone === 'ready'
          ? styles.guideStatusReady
          : state.tone === 'failed'
            ? styles.guideStatusFailed
            : styles.guideStatusWaiting,
      )}
    >
      <Sparkles size={12} aria-hidden="true" /> {state.label}
    </span>
  );
}
