import * as stylex from '@stylexjs/stylex';
import type { ReactNode } from 'react';
import { ArrowRight, ExternalLink, FileQuestion, FileWarning, RotateCw, TriangleAlert } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { colors, typography } from '@/styles/tokens.stylex';
import type { SessionDocument } from '@/features/session/reader/types';

const styles = stylex.create({
  shell: {
    padding: '1.5rem',
    borderColor: colors.border,
    borderStyle: 'dashed',
    gap: '0.75rem',
    alignItems: 'center',
    color: colors.textPrimary,
    display: 'flex',
    flexDirection: 'column',
    justifyContent: 'center',
    textAlign: 'center',
    minHeight: '100%',
  },
  shellError: {
    borderColor: colors.danger,
  },
  iconWrapper: {
    height: '1.35rem',
    width: '1.35rem',
  },
  iconDestructive: {
    color: colors.danger,
  },
  kicker: {
    color: colors.textSecondary,
    fontSize: '0.6875rem',
    fontWeight: typography.weightBold,
    letterSpacing: '0.12em',
    textTransform: 'uppercase',
  },
  heading: {
    margin: 0,
    fontSize: 'clamp(1.15rem, 2vw, 1.5rem)',
    letterSpacing: '0.035em',
    maxWidth: '28rem',
  },
  bodyText: {
    margin: 0,
    color: colors.textSecondary,
    fontSize: typography.sizeSm,
    lineHeight: typography.leadingRelaxed,
    maxWidth: '28rem',
  },
  note: {
    margin: 0,
    color: colors.textSecondary,
    fontSize: '0.75rem',
    lineHeight: typography.leadingRelaxed,
    maxWidth: '28rem',
  },
  actions: {
    gap: '0.5rem',
    display: 'flex',
    flexWrap: 'wrap',
    justifyContent: 'center',
    marginTop: '0.25rem',
  },
  retryIcon: {
    height: '0.875rem',
    width: '0.875rem',
  },
  backLink: {
    color: colors.textPrimarySoft,
    fontSize: '0.8125rem',
    textUnderlineOffset: '4px',
    marginTop: '0.25rem',
  },
});

export function EmptyDocument() {
  return (
    <div {...stylex.props(styles.shell)}>
      <div {...stylex.props(styles.iconWrapper)}>
        <FileQuestion aria-hidden="true" />
      </div>
      <span {...stylex.props(styles.kicker)}>Nothing attached yet</span>
      <h2 {...stylex.props(styles.heading)}>This action has no document</h2>
      <p {...stylex.props(styles.bodyText)}>
        The study plan is still available. Choose another source or return to the plan.
      </p>
      <Button variant="outline" size="sm" href="/study">
        Back to the plan
      </Button>
    </div>
  );
}

export function UnrenderableDocument({ doc }: { doc: SessionDocument }) {
  return (
    <ReaderStateShell error={false}>
      <UnrenderableDocumentBody doc={doc} />
    </ReaderStateShell>
  );
}

function ReaderStateShell({ error, children }: { error: boolean; children: ReactNode }) {
  return (
    <div {...stylex.props(styles.shell, error && styles.shellError)} role={error ? 'alert' : undefined}>
      {children}
    </div>
  );
}

function UnrenderableDocumentBody({ doc }: { doc: SessionDocument }) {
  return (
    <>
      <div {...stylex.props(styles.iconWrapper)}>
        <FileWarning aria-hidden="true" />
      </div>
      <span {...stylex.props(styles.kicker)}>External document</span>
      <h2 {...stylex.props(styles.heading)}>{doc.display_name}</h2>
      <p {...stylex.props(styles.bodyText)}>
        This {doc.document_kind} file cannot be displayed in the reader, but it is still available in its original
        format.
      </p>
      <Button
        variant="outline"
        size="sm"
        href={doc.download_url}
        target="_blank"
        rel="noopener"
        icon={<ExternalLink aria-hidden="true" />}
      >
        Open outside the app
      </Button>
      <p {...stylex.props(styles.note)}>
        Time spent there cannot be measured, so record it on the session when you come back.
      </p>
    </>
  );
}

function DocumentErrorActions({ onRetry, downloadUrl }: { onRetry: () => void; downloadUrl?: string }) {
  return (
    <div {...stylex.props(styles.actions)}>
      <Button
        type="button"
        variant="default"
        size="sm"
        onClick={onRetry}
        aria-label="Retry loading the document"
        icon={<RotateCw aria-hidden="true" {...stylex.props(styles.retryIcon)} />}
      >
        Try again
      </Button>
      {downloadUrl ? (
        <Button
          variant="outline"
          size="sm"
          href={downloadUrl}
          target="_blank"
          rel="noopener"
          icon={<ExternalLink aria-hidden="true" />}
        >
          Open source file
        </Button>
      ) : null}
    </div>
  );
}

export function DocumentLoadError({
  message,
  onRetry,
  downloadUrl,
}: {
  message: string;
  onRetry: () => void;
  downloadUrl?: string;
}) {
  return (
    <ReaderStateShell error>
      <DocumentLoadErrorBody message={message} onRetry={onRetry} downloadUrl={downloadUrl} />
    </ReaderStateShell>
  );
}

function DocumentLoadErrorBody({
  message,
  onRetry,
  downloadUrl,
}: {
  message: string;
  onRetry: () => void;
  downloadUrl?: string;
}) {
  return (
    <>
      <div {...stylex.props(styles.iconWrapper, styles.iconDestructive)}>
        <TriangleAlert aria-hidden="true" />
      </div>
      <span {...stylex.props(styles.kicker)}>Document unavailable</span>
      <h2 {...stylex.props(styles.heading)}>Unable to open this source</h2>
      <p {...stylex.props(styles.bodyText)}>{message}</p>
      <DocumentErrorActions onRetry={onRetry} downloadUrl={downloadUrl} />
      <p {...stylex.props(styles.note)}>
        If the source is missing or storage is unavailable, fix it in Settings / Storage, run a course check, and return
        here.
      </p>
      <a {...stylex.props(styles.backLink)} href="/study">
        Back to the plan <ArrowRight aria-hidden="true" />
      </a>
    </>
  );
}
