import * as stylex from '@stylexjs/stylex';
import { colors, typography } from '@/styles/tokens.stylex';
import type { PDFDocumentProxy } from 'pdfjs-dist';
import type { SessionDocument } from './types';

const styles = stylex.create({
  loadingState: {
    padding: '2rem',
    borderColor: colors.border,
    borderStyle: 'dashed',
    borderWidth: '1px',
    gap: '0.375rem',
    placeItems: 'center',
    backgroundColor: colors.surfaceRaised,
    color: colors.textSecondary,
    display: 'grid',
    textAlign: 'center',
    minHeight: '14rem',
  },
  strong: {
    color: colors.textPrimary,
    fontSize: typography.sizeSm,
  },
  docName: {
    overflow: 'hidden',
    textOverflow: 'ellipsis',
    whiteSpace: 'nowrap',
    maxWidth: '42ch',
  },
});

export function PdfReaderLoading({ pdf, doc }: { pdf: PDFDocumentProxy | null; doc: SessionDocument | null }) {
  return (
    <div {...stylex.props(styles.loadingState)} role="status" aria-live="polite">
      <strong {...stylex.props(styles.strong)}>Loading page 1{pdf?.numPages ? ` of ${pdf.numPages}` : ''}…</strong>
      <span {...stylex.props(styles.docName)}>{doc?.display_name || doc?.file_name || 'Preparing the document'}</span>
    </div>
  );
}
