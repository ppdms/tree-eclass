import { useState } from 'react';
import { AppLink } from '../app/links';
import { Button, Notice } from '../ui/controls';
import { Text, View } from '../ui/primitives';
import type { UIStyle } from '../ui/types';
import { requestNewQuestion } from './navigation';
import { styles } from './styles';

export function AskHeader({ hasMessages }: { hasMessages: boolean }) {
  const headerStyle: UIStyle = { ...styles.header };
  if (!hasMessages) {
    headerStyle.alignItems = 'center';
    headerStyle.paddingTop = 48;
  }
  return (
    <View style={headerStyle}>
      <Text id="ask-page-title" style={styles.title}>
        {hasMessages ? 'Ask' : 'What are you working through?'}
      </Text>
      <Text style={styles.muted}>
        {hasMessages
          ? 'Answers are AI-generated from your course material.'
          : 'Ask a focused question and an AI model answers from your course material — ' +
            'trace every claim back to its source.'}
      </Text>
      {hasMessages && (
        <AppLink id="ask-new-question" href="/ask" style={styles.link} onClick={requestNewQuestion}>
          New question
        </AppLink>
      )}
    </View>
  );
}

export function ProviderNotice() {
  const [details, setDetails] = useState(false);
  return (
    <Notice id="ask-offline" tone="warning">
      <Text style={{ ...styles.text, fontWeight: 600 }}>Ask is offline — no AI provider is connected.</Text>
      <Text style={styles.muted}>Course material stays available to browse and read.</Text>
      <AppLink id="ask-configure" href="/settings#ai" style={styles.link}>
        Configure Ask in Settings
      </AppLink>
      <Button
        id="ask-setup-details-toggle"
        expanded={details}
        controls="ask-setup-details"
        onClick={() => setDetails((open) => !open)}
      >
        {details ? 'Hide setup details' : 'Show setup details'}
      </Button>
      {details && (
        <Text id="ask-setup-details" style={styles.small}>
          Ask needs a configured chat provider before it can answer questions.
        </Text>
      )}
    </Notice>
  );
}

export function AskExamples({
  examples,
  disabled,
  onPick,
}: {
  examples: string[];
  disabled: boolean;
  onPick: (value: string) => void;
}) {
  return (
    <View id="ask-examples" style={styles.column} label="Suggested questions">
      <Text style={styles.muted}>Try asking</Text>
      {examples.map((example) => (
        <Button
          key={example}
          id={`ask-example-${encodeURIComponent(example)}`}
          disabled={disabled}
          onClick={() => onPick(example)}
          style={{ justifyContent: 'start' }}
        >
          {example}
        </Button>
      ))}
    </View>
  );
}
