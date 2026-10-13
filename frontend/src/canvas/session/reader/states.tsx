import { AppLink } from '../../app/links';
import { Button, Notice } from '../../ui/controls';
import { colors } from '../../ui/palette';
import { Text, View } from '../../ui/primitives';
import type { SessionDocument } from './types';

function SourceActions({ document }: { document: SessionDocument }) {
  const url = document.download_url ?? document.content_url;
  if (!url) return null;
  const id = encodeURIComponent(String(document.document_id));
  return (
    <View style={{ flexDirection: 'row', flexWrap: 'wrap', gap: 16 }}>
      <AppLink
        id={`reader:open:${id}`}
        href={url}
        target="_blank"
        rel="noopener noreferrer"
        style={{ color: colors.link }}
      >
        Open source file
      </AppLink>
      <AppLink
        id={`reader:download:${id}`}
        href={url}
        download={document.file_name ?? document.display_name ?? 'source'}
        style={{ color: colors.link }}
      >
        Download original
      </AppLink>
    </View>
  );
}

interface ReaderStatusProps {
  document: SessionDocument;
  page: number;
  count: number;
  error: string | null;
  unsupported: boolean;
  retry(): void;
}

function SourceMessage({ document, page, count, error, unsupported, retry }: ReaderStatusProps) {
  if (error)
    return (
      <>
        <Notice tone="danger">{error}</Notice>
        <Button id={`reader:retry:${document.document_id}`} onClick={retry}>
          Try again
        </Button>
        <Text style={{ color: colors.muted }}>
          If the source is missing or storage is unavailable, fix it in Settings / Storage, run a course check, and
          return here.
        </Text>
      </>
    );
  if (unsupported)
    return (
      <>
        <Text style={{ color: colors.text }}>{document.display_name ?? document.file_name ?? 'Original source'}</Text>
        <Text style={{ color: colors.muted }}>
          This {document.document_kind ?? 'source'} file cannot be displayed in the reader. It remains available in its
          original format. Time outside the reader is not measured.
        </Text>
      </>
    );
  return (
    <Text style={{ color: colors.muted }}>
      Loading page {page}
      {count ? ` of ${count}` : ''}
    </Text>
  );
}

export function ReaderStatus({ document, page, count, error, unsupported, retry }: ReaderStatusProps) {
  return (
    <View style={{ padding: 24, gap: 18, minHeight: 180 }}>
      <Text style={{ fontSize: 20, fontWeight: 650, color: colors.text }}>
        {error
          ? 'Unable to open this source'
          : unsupported
            ? 'External document'
            : (document.display_name ?? 'Reading')}
      </Text>
      <SourceMessage
        document={document}
        page={page}
        count={count}
        error={error}
        unsupported={unsupported}
        retry={retry}
      />
      {error || unsupported ? (
        <>
          <SourceActions document={document} />
          <AppLink id={`reader:back:${document.document_id}`} href="/study" style={{ color: colors.link }}>
            Back to the plan
          </AppLink>
        </>
      ) : null}
    </View>
  );
}
