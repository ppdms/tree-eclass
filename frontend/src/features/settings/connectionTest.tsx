import * as stylex from '@stylexjs/stylex';
import * as React from 'react';
import { z } from 'zod/v4';
import { buttonStyles } from '@/components/ui/styles';
import { colors, typography } from '@/styles/tokens.stylex';

const styles = stylex.create({
  connectionTest: {
    gap: '0.75rem',
    alignItems: 'center',
    display: 'flex',
    flexWrap: 'wrap',
    marginTop: '0.75rem',
  },
  connectionTestResult: {
    fontSize: typography.sizeSm,
  },
  connectionTestSuccess: {
    color: colors.success,
  },
  connectionTestError: {
    color: colors.danger,
  },
});

type BusyState = { busy: true };
type ResultState = { ok: boolean; message: string };
type ConnectionTestState = BusyState | ResultState | null;

export interface ConnectionTestProps {
  endpoint: string;
  label: string;
}

const resultSchema = z
  .object({
    ok: z.unknown(),
    message: z.string(),
  })
  .partial();

function isResultState(value: ConnectionTestState): value is ResultState {
  return value !== null && !('busy' in value);
}

interface RunConnectionTestArgs {
  endpoint: string;
  setState: React.Dispatch<React.SetStateAction<ConnectionTestState>>;
}

async function runConnectionTest({ endpoint, setState }: RunConnectionTestArgs): Promise<void> {
  setState({ busy: true });
  try {
    const response = await fetch(endpoint, {
      method: 'POST',
      signal: AbortSignal.timeout(10000),
      headers: { Accept: 'application/json' },
    });
    const parsed = resultSchema.safeParse(await response.json());
    if (parsed.success && parsed.data.message) {
      setState({ ok: Boolean(parsed.data.ok), message: parsed.data.message });
    } else {
      setState({ ok: false, message: 'Could not reach the test endpoint.' });
    }
  } catch {
    setState({ ok: false, message: 'Could not reach the test endpoint.' });
  }
}

export function ConnectionTest({ endpoint, label }: ConnectionTestProps) {
  const [state, setState] = React.useState<ConnectionTestState>(null);
  const test = React.useCallback(() => runConnectionTest({ endpoint, setState }), [endpoint]);
  const isBusy = state !== null && 'busy' in state && state.busy === true;
  return (
    <div {...stylex.props(styles.connectionTest)}>
      <button
        type="button"
        {...stylex.props(buttonStyles.base, buttonStyles.secondary)}
        onClick={test}
        disabled={isBusy}
      >
        {isBusy ? 'Testing…' : label}
      </button>
      {isResultState(state) && (
        <span
          {...stylex.props(
            styles.connectionTestResult,
            state.ok ? styles.connectionTestSuccess : styles.connectionTestError,
          )}
          role={state.ok ? 'status' : 'alert'}
        >
          {state.message}
        </span>
      )}
    </div>
  );
}
