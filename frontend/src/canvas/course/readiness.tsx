import { readableReason } from '@/lib/display';
import type { RoadmapBlueprint } from '@/lib/types';
import { Text, View } from '../ui/primitives';
import { styles } from './styles';

export function CourseReadiness({ blueprint }: { blueprint?: RoadmapBlueprint }) {
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
  const state = String(readiness.state || 'missing')
    .replaceAll('_', ' ')
    .replace(/\b\w/g, (c) => c.toUpperCase());
  return (
    <View style={styles.readiness}>
      <Text style={styles.previewTitle}>{state}</Text>
      <Text style={styles.description}>
        {readableReason(readiness.reason, 'No roadmap generation has been created yet.')}
      </Text>
      {source && (
        <Text style={styles.small}>
          {documents ? `${ready}/${documents} file guides ready` : 'No ready source files reported'}
          {pageTotal ? ` · ${readyPages}/${pageTotal} pages analyzed` : ''}
          {activeFiles ? ` · ${activeFiles} file operation(s) active` : ''}
          {failed ? ` · ${failed} failure(s)` : ''}
        </Text>
      )}
      {sourceReason && <Text style={styles.small}>{sourceReason}</Text>}
    </View>
  );
}
