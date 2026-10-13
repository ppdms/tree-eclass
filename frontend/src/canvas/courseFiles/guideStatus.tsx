import type { FileInsight } from '@/lib/types';
import { Text } from '../ui/primitives';
import { colors } from '../ui/palette';

interface Status {
  label: string;
  detail: string;
  tone: 'ready' | 'waiting' | 'failed';
}

function sourceStatus(insight: FileInsight): Status | null {
  if (insight.source_status === 'unsupported') {
    return {
      label: 'File not supported',
      tone: 'failed',
      detail: insight.source_diagnostic_reason || 'The source file type is not supported by the extractor.',
    };
  }
  if (['failed', 'skipped_limit'].includes(insight.source_status || '')) {
    return {
      label: 'Source indexing failed',
      tone: 'failed',
      detail: insight.source_error || insight.source_diagnostic_reason || 'The source could not be indexed.',
    };
  }
  if (['pending', 'running'].includes(insight.source_status || '')) {
    return {
      label: 'Source indexing',
      tone: 'waiting',
      detail: 'The file is still being extracted before guide generation can start.',
    };
  }
  if (insight.source_status === 'external') {
    return {
      label: 'External source',
      tone: 'waiting',
      detail: 'This auxiliary source is available as course material but is not part of the indexed guide set.',
    };
  }
  return null;
}

function generationStatus(insight: FileInsight): Status {
  if (!insight.ai_processing_enabled) {
    return {
      label: 'Guides disabled',
      tone: 'waiting',
      detail: 'AI guide generation is disabled or has no configured provider.',
    };
  }
  if (insight.enrichment_status === 'failed') {
    return {
      label: 'Guide failed',
      tone: 'failed',
      detail: insight.enrichment_error || 'The last guide generation attempt failed. Retry failed work in Settings.',
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
      tone: 'waiting',
      detail: `Visual analysis is ${ready}/${total} pages ready.`,
    };
  }
  return {
    label: 'Guide not queued',
    tone: 'waiting',
    detail: 'No current guide generation exists for this indexed file.',
  };
}

export function GuideStatus({ insight }: { insight: FileInsight }) {
  const state: Status =
    insight.guide_available || insight.ai?.summary
      ? { label: 'Guide ready', detail: 'A study guide is available for this file.', tone: 'ready' }
      : sourceStatus(insight) || generationStatus(insight);
  const color = state.tone === 'ready' ? colors.success : state.tone === 'failed' ? colors.danger : colors.warning;
  return (
    <Text title={state.detail} label={`${state.label}. ${state.detail}`} style={{ color, fontSize: 12 }}>
      {state.label}
    </Text>
  );
}
