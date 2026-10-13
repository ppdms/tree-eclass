import { useState } from 'react';
import { requestJson } from '../app/api';
import { Button, Notice } from '../ui/controls';
import { Dialog } from '../ui/overlays';
import { Text, View } from '../ui/primitives';
import { styles } from './styles';

function useCourseCheck() {
  const [open, setOpen] = useState(false);
  const [state, setState] = useState<string | null>(null);
  const run = async () => {
    setOpen(false);
    setState('Starting course check…');
    try {
      await requestJson('/api/run-check', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: '{}',
      });
      setState('Course check started. Study guidance will refresh when it finishes.');
    } catch (error) {
      setState(error instanceof Error ? error.message : 'Could not start the course check.');
    }
  };
  return { open, setOpen, state, run };
}

export function RunCheckButton() {
  const { open, setOpen, state, run } = useCourseCheck();
  return (
    <View style={styles.stack}>
      <Button id="study-run-check" onClick={() => setOpen(true)}>
        Run check
      </Button>
      {state && (
        <Notice id="study-check-state" tone={state.startsWith('Could') ? 'danger' : 'info'}>
          <Text>{state}</Text>
        </Notice>
      )}
      <Dialog
        id="study-check-confirm"
        title="Run course check?"
        open={open}
        onClose={() => setOpen(false)}
        actions={
          <>
            <Button id="study-check-cancel" onClick={() => setOpen(false)}>
              Cancel
            </Button>
            <Button
              id="study-check-run"
              tone="primary"
              onClick={() => {
                void run();
              }}
            >
              Run check
            </Button>
          </>
        }
      >
        <Text style={styles.copy}>
          Refresh course files, announcements, and study evidence now. This can take a few minutes.
        </Text>
      </Dialog>
    </View>
  );
}
