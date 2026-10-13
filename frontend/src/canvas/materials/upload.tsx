import { useEffect, useRef, useState } from 'react';
import type { ExternalMaterial } from '@/lib/types';
import { requestJson } from '../app/api';
import { Button, Notice, Panel } from '../ui/controls';
import { FilePicker } from '../ui/forms';
import { Text, View } from '../ui/primitives';
import { materialsPath } from './helpers';
import { styles } from './styles';

interface UploadPayload {
  message: string;
  material: ExternalMaterial;
}

interface UploadPanelProps {
  courseId: string | number;
  onUploaded: (material: ExternalMaterial) => void;
}

function useUploadForm({ courseId, onUploaded }: UploadPanelProps) {
  const [file, setFile] = useState<File | null>(null);
  const [busy, setBusy] = useState(false);
  const [notice, setNotice] = useState('');
  const [resetKey, setResetKey] = useState(0);
  const pending = useRef(false);
  const mounted = useRef(false);
  useEffect(() => {
    mounted.current = true;
    return () => {
      mounted.current = false;
    };
  }, []);
  const submit = async () => {
    if (!file || pending.current) return;
    pending.current = true;
    setBusy(true);
    setNotice('');
    const body = new FormData();
    body.set('file', file);
    let payload: UploadPayload;
    try {
      payload = await requestJson<UploadPayload>(materialsPath(courseId), { method: 'POST', body });
    } catch (error) {
      if (mounted.current) setNotice(error instanceof Error ? error.message : 'The upload failed.');
      return;
    } finally {
      pending.current = false;
      if (mounted.current) setBusy(false);
    }
    if (!mounted.current) return;
    setFile(null);
    setResetKey((value) => value + 1);
    setNotice(payload.message);
    onUploaded(payload.material);
  };
  return { file, setFile, busy, notice, resetKey, submit };
}

export function MaterialUploadPanel(props: UploadPanelProps) {
  const state = useUploadForm(props);
  const id = `materials:${encodeURIComponent(String(props.courseId))}:upload`;
  return (
    <Panel id={id} style={styles.upload}>
      <View style={styles.header}>
        <Text style={styles.groupTitle}>Add course material</Text>
        <Text style={styles.muted}>
          The AI sets the initial category. You can override it after upload; PDFs also become available for highlights,
          notes, questions, and bookmarks.
        </Text>
      </View>
      <View style={styles.uploadFields}>
        <FilePicker
          id={`${id}:file`}
          label="Choose file"
          resetKey={state.resetKey}
          onFiles={(files) => state.setFile(files[0] || null)}
        />
        <Button
          id={`${id}:submit`}
          onClick={() => {
            void state.submit();
          }}
          disabled={!state.file || state.busy}
        >
          <Text>{state.busy ? 'Uploading…' : 'Upload'}</Text>
        </Button>
      </View>
      {state.file && (
        <Text title={state.file.name} style={styles.filename}>
          {state.file.name}
        </Text>
      )}
      {state.notice && (
        <Notice id={`${id}:notice`} tone="info">
          <Text style={styles.muted}>{state.notice}</Text>
        </Notice>
      )}
    </Panel>
  );
}
