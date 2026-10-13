import type { RoadmapBlueprint } from '@/lib/types';
import { readableReason } from '@/lib/display';
import { Notice, Panel } from '../ui/controls';
import { Text, View } from '../ui/primitives';
import { styles } from './styles';

export function RoadmapReadinessNote({ blueprint }: { blueprint?: RoadmapBlueprint }) {
  const readiness = blueprint?.readiness || {};
  const source = readiness.source_readiness;
  const ready = Number(source?.ready_document_insights || 0);
  const documents = Number(source?.ready_documents || 0);
  const activeFiles = Number(source?.active_extraction_jobs || 0) + Number(source?.active_document_enrichments || 0);
  const readyPages = Number(source?.page_enrichments?.ready || 0);
  const activePages = Number(source?.active_page_enrichments || 0);
  const failed =
    Number(source?.failed_documents || 0) +
    Number(source?.failed_document_enrichments || 0) +
    Number(source?.failed_page_enrichments || 0);
  const counts = [
    documents ? `${ready}/${documents} file guides ready` : 'No ready source files reported',
    readyPages + activePages ? `${readyPages}/${readyPages + activePages} pages analyzed` : '',
    activeFiles ? `${activeFiles} file operation(s) active` : '',
    failed ? `${failed} failure(s)` : '',
  ]
    .filter(Boolean)
    .join(' · ');
  const sourceReason = readableReason(source, '');
  return (
    <View style={styles.stack}>
      <Text style={styles.heading}>
        {(readiness.state || 'missing').replaceAll('_', ' ').replace(/\b\w/g, (character) => character.toUpperCase())}
      </Text>
      <Text style={styles.body}>{readableReason(readiness.reason, 'No roadmap generation has been created yet.')}</Text>
      {source && <Text style={styles.meta}>{counts}</Text>}
      {sourceReason && <Text style={styles.meta}>{sourceReason}</Text>}
    </View>
  );
}

export function RoadmapWaiting({ blueprint }: { blueprint?: RoadmapBlueprint }) {
  const readiness = blueprint?.readiness || {};
  return (
    <Panel style={styles.card}>
      <Text style={styles.title}>Study roadmap</Text>
      <Text style={styles.meta}>
        An evidence-grounded route through lectures, exercises, community material, and prior exams.
      </Text>
      <Text style={styles.heading}>
        {(readiness.state || 'waiting').replaceAll('_', ' ').replace(/\b\w/g, (character) => character.toUpperCase())}
      </Text>
      <Text style={styles.body}>
        {readableReason(readiness.reason, 'Run a course check and indexing will prepare the roadmap.')}
      </Text>
      {readiness.source_readiness && (
        <Text style={styles.meta}>
          {readableReason(readiness.source_readiness, 'Source coverage is still being reconciled.')}
        </Text>
      )}
      <Text style={styles.body}>
        Files remain available in the Files tab. Reopen the Roadmap tab after its evidence is indexed and a validated
        synthesis is ready.
      </Text>
      {readiness.refresh_pending && <Notice>A refreshed roadmap is being prepared.</Notice>}
    </Panel>
  );
}
