import { useEffect, useRef, useState } from 'react';
import { Button, Notice, Panel } from '../ui/controls';
import { Field, Select } from '../ui/forms';
import { Text, View } from '../ui/primitives';
import { friendlyError } from './model';
import { styles } from './styles';

export interface AskComposerProps {
  busy: boolean;
  available: boolean;
  models: string[];
  model: string;
  setModel: (value: string) => void;
  send: (question: string) => Promise<boolean>;
  stop: () => void;
}

function useComposerDraft(send: AskComposerProps['send'], busy: boolean, available: boolean, stop: () => void) {
  const [input, setInput] = useState('');
  const [error, setError] = useState<string | null>(null);
  const version = useRef(0);
  const mounted = useRef(true);
  useEffect(() => {
    mounted.current = true;
    return () => {
      mounted.current = false;
    };
  }, []);
  const change = (value: string) => {
    version.current += 1;
    setInput(value);
  };
  const submit = () => {
    if (!available) return;
    if (busy) return stop();
    const question = input.trim();
    if (!question) return;
    const submittedVersion = version.current;
    setError(null);
    void send(question)
      .then((saved) => {
        // Preserve typed-ahead text and failed/aborted drafts; SDK promises also resolve on ordinary stream errors.
        if (mounted.current && saved && version.current === submittedVersion) setInput('');
      })
      .catch((failure) => {
        if (mounted.current) setError(friendlyError(failure));
      });
  };
  return { input, error, change, submit };
}

export function AskComposer({ busy, available, models, model, setModel, send, stop }: AskComposerProps) {
  const { input, error, change, submit } = useComposerDraft(send, busy, available, stop);
  return (
    <Panel id="ask-composer" style={styles.composer}>
      <Field
        id="ask-question"
        label="Question"
        value={input}
        onChange={change}
        multiline
        rows={2}
        maxHeight={192}
        autoResize
        disabled={!available}
        submitOnEnter={available && !busy}
        onSubmit={submit}
        placeholder={available ? 'What does lecture 3 say about …' : 'Asking is unavailable'}
      />
      <View style={{ ...styles.row, justifyContent: 'space-between' }}>
        {available && models.length > 0 && (
          <Select
            id="ask-model"
            label="Answer model"
            value={model}
            options={models.map((value) => ({ value, label: value }))}
            onChange={setModel}
            disabled={busy}
          />
        )}
        <Button
          id={busy ? 'ask-stop-response' : 'ask-send-question'}
          onClick={submit}
          tone="primary"
          disabled={!available || (!busy && !input.trim())}
          title={busy ? 'Stop response' : 'Send question'}
        >
          {busy ? 'Stop response' : 'Send question'}
        </Button>
      </View>
      <Text style={styles.small}>Return sends · Shift+Return starts a new line</Text>
      {error && (
        <Notice id="ask-composer-error" tone="danger">
          <Text style={styles.small}>{error}</Text>
        </Notice>
      )}
    </Panel>
  );
}
