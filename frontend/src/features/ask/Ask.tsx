import * as stylex from '@stylexjs/stylex';
import * as React from 'react';
import { TriangleAlert } from 'lucide-react';
import { buttonStyles } from '@/components/ui/styles';
import type { Conversation } from '@/features/ask/types';
import { colors, layout, spacing, typography } from '@/styles/tokens.stylex';
import type { ChatStatus, UIMessage } from 'ai';
import { Composer } from './AskComposer';
import { Examples } from './AskExamples';
import { Thread } from './AskThread';
import { useAskChat, useAskFocus, useAskModel } from './useAskChat';

const styles = stylex.create({
  askRoot: {
    display: 'flex',
    flexDirection: 'column',
    height: '100%',
    minHeight: 0,
    width: '100%',
  },
  askHeader: {
    gap: '1rem',
    marginInline: 'auto',
    alignItems: 'center',
    display: 'flex',
    justifyContent: 'space-between',
    marginBottom: spacing.md,
    maxWidth: '48rem',
    width: '100%',
  },
  askHeaderEmpty: {
    display: 'block',
    marginBlockStart: 'clamp(3.5rem, 12vh, 7rem)',
    textAlign: 'center',
    marginBottom: 0,
  },
  askHeaderThreaded: {
    borderBottomColor: colors.border,
    borderBottomStyle: 'solid',
    borderBottomWidth: 1,
    paddingBottom: '1.25rem',
  },
  askHeaderAction: {
    borderColor: { default: colors.border, ':hover': colors.borderLight },
    borderRadius: '0.55rem',
    borderStyle: 'solid',
    borderWidth: 1,
    paddingBlock: '0.45rem',
    paddingInline: '0.7rem',
    textDecoration: 'none',
    backgroundColor: { default: 'transparent', ':hover': colors.surfaceRaised },
    color: { default: colors.textSecondary, ':hover': colors.textPrimary },
    fontSize: typography.sizeSm,
    transitionDuration: '150ms',
    transitionProperty: 'background-color, border-color, color',
  },
  askHeaderTitle: {
    margin: 0,
    color: colors.textPrimary,
    fontSize: 'clamp(1.75rem, 3vw, 2.35rem)',
    fontWeight: typography.weightSemibold,
    letterSpacing: '-0.05em',
    lineHeight: 1.08,
  },
  askHeaderTitleEmpty: {
    fontSize: 'clamp(2rem, 4vw, 3rem)',
  },
  askHeaderDescription: {
    marginInline: 'auto',
    color: colors.textSecondary,
    fontSize: typography.sizeMd,
    lineHeight: 1.55,
    marginBlockStart: '0.65rem',
    maxWidth: '48ch',
  },
  askProviderNotice: {
    borderColor: `color-mix(in srgb, ${colors.warning} 55%, ${colors.border})`,
    borderRadius: '0.75rem',
    borderStyle: 'solid',
    borderWidth: 1,
    gap: '0.75rem',
    marginInline: 'auto',
    paddingBlock: '0.9rem',
    paddingInline: '1rem',
    alignItems: 'flex-start',
    backgroundColor: `color-mix(in srgb, ${colors.warning} 8%, ${colors.surface})`,
    color: colors.textPrimarySoft,
    display: 'flex',
    marginTop: '1.5rem',
    maxWidth: '48rem',
  },
  askProviderIcon: {
    flex: 'none',
    color: colors.warning,
    height: '1rem',
    marginTop: '0.15rem',
    width: '1rem',
  },
  askProviderStrong: {
    color: colors.textPrimary,
    fontSize: typography.sizeSm,
  },
  askProviderText: {
    fontFamily: typography.fontFamily,
    fontSize: typography.sizeSm,
    lineHeight: layout.bodyLeading,
    marginBlockStart: '0.3rem',
  },
  askProviderAction: {
    marginBlockStart: '0.7rem',
    height: '2.5rem',
    minHeight: '2.5rem',
  },
  askProviderDetails: {
    fontSize: typography.sizeXs,
    marginBlockStart: '0.45rem',
  },
  askProviderDetailsSummary: {
    color: colors.textSecondary,
    cursor: 'pointer',
  },
  askProviderDetailsText: {
    fontFamily: typography.fontFamily,
    fontSize: typography.sizeSm,
    lineHeight: layout.bodyLeading,
    marginBlockStart: '0.35rem',
  },
  askStage: {
    marginInline: 'auto',
    display: 'flex',
    flexDirection: 'column',
    flexGrow: 1,
    minHeight: 0,
    width: '100%',
  },
  askStageEmpty: {
    display: 'flex',
    flexGrow: 0,
    maxWidth: '42rem',
    paddingTop: 'clamp(1.75rem, 4vh, 3rem)',
  },
  askStageThreaded: {
    maxWidth: '48rem',
  },
});

function AskHeader({ hasMessages }: { hasMessages: boolean }) {
  return (
    <header {...stylex.props(styles.askHeader, hasMessages ? styles.askHeaderThreaded : styles.askHeaderEmpty)}>
      <div>
        <h1 id="ask-page-title" {...stylex.props(styles.askHeaderTitle, !hasMessages && styles.askHeaderTitleEmpty)}>
          {hasMessages ? 'Ask' : 'What are you working through?'}
        </h1>
        <p {...stylex.props(styles.askHeaderDescription)}>
          {hasMessages
            ? 'Answers are AI-generated from your course material.'
            : 'Ask a focused question and an AI model answers from your course material — ' +
              'trace every claim back to its source.'}
        </p>
      </div>
      {hasMessages && (
        <a {...stylex.props(styles.askHeaderAction)} href="/ask">
          New question
        </a>
      )}
    </header>
  );
}

function AskProviderNotice() {
  return (
    <div {...stylex.props(styles.askProviderNotice)} role="status">
      <TriangleAlert {...stylex.props(styles.askProviderIcon)} aria-hidden="true" />
      <div>
        <strong {...stylex.props(styles.askProviderStrong)}>Ask is offline — no AI provider is connected.</strong>
        <p {...stylex.props(styles.askProviderText)}>Course material stays available to browse and read.</p>
        <a {...stylex.props(buttonStyles.base, buttonStyles.primary, styles.askProviderAction)} href="/settings#ai">
          Configure Ask in Settings
        </a>
        <details {...stylex.props(styles.askProviderDetails)}>
          <summary {...stylex.props(styles.askProviderDetailsSummary)}>Show setup details</summary>
          <p {...stylex.props(styles.askProviderDetailsText)}>
            Ask needs a configured chat provider before it can answer questions.
          </p>
        </details>
      </div>
    </div>
  );
}

interface UseAskPageArgs {
  root: React.RefObject<HTMLElement | null>;
  conversationId: string | number | null | undefined;
  initialMessages: UIMessage[];
  models: string[];
  defaultModel?: string;
  onConversation?: (conversation: Conversation) => void;
}

function useAskPage({ root, conversationId, initialMessages, models, defaultModel, onConversation }: UseAskPageArgs) {
  const [input, setInput] = React.useState('');
  const conversation = React.useRef<string | number | null | undefined>(conversationId);
  const { model, setModel, modelRef } = useAskModel(models, defaultModel);
  const { messages, sendMessage, regenerate, status, error, stop } = useAskChat({
    root,
    initialMessages,
    conversation,
    modelRef,
    onConversation,
  });
  useAskFocus(root, messages.length > 0);

  const busy = status === 'submitted' || status === 'streaming';
  return {
    input,
    setInput,
    messages,
    sendMessage,
    regenerate,
    status,
    error,
    stop,
    model,
    setModel,
    busy,
  };
}

interface AskViewProps {
  busy: boolean;
  providerAvailable: boolean;
  messages: UIMessage[];
  examples: string[];
  sendMessage: (message: { text: string }) => Promise<void>;
  status: ChatStatus;
  error: Error | null | undefined;
  regenerate: () => void;
  input: string;
  setInput: React.Dispatch<React.SetStateAction<string>>;
  stop: (() => void) | undefined;
  models: string[];
  model: string;
  setModel: (value: string) => void;
}

function AskView(props: AskViewProps) {
  const hasMessages = props.messages.length > 0;
  return (
    <div {...stylex.props(styles.askRoot)} aria-busy={props.busy} data-has-messages={hasMessages ? 'true' : 'false'}>
      <AskHeader hasMessages={hasMessages} />
      {!props.providerAvailable && <AskProviderNotice />}
      <div {...stylex.props(styles.askStage, hasMessages ? styles.askStageThreaded : styles.askStageEmpty)}>
        <Thread
          messages={props.messages}
          status={props.status}
          error={props.error}
          onRetry={() => props.regenerate()}
          busy={props.busy}
        />
        <Composer
          input={props.input}
          setInput={props.setInput}
          sendMessage={props.sendMessage}
          busy={props.busy}
          stop={props.stop}
          models={props.providerAvailable ? props.models : []}
          model={props.model}
          setModel={props.setModel}
          available={props.providerAvailable}
        />
        {!hasMessages && props.providerAvailable && props.examples.length > 0 && (
          <Examples examples={props.examples} onPick={(example) => props.sendMessage({ text: example })} />
        )}
      </div>
    </div>
  );
}

export interface AskProps {
  root: React.RefObject<HTMLElement | null>;
  conversationId?: string | number | null;
  initialMessages?: UIMessage[];
  examples?: string[];
  models?: string[];
  defaultModel?: string;
  providerAvailable?: boolean;
  onConversation?: (conversation: Conversation) => void;
}

export default function Ask({
  root,
  conversationId = null,
  initialMessages = [],
  examples = [],
  models = [],
  defaultModel,
  providerAvailable = true,
  onConversation,
}: AskProps) {
  const page = useAskPage({
    root,
    conversationId,
    initialMessages,
    models,
    defaultModel,
    onConversation,
  });
  return <AskView {...page} examples={examples} models={models} providerAvailable={providerAvailable} />;
}
