import * as stylex from '@stylexjs/stylex';
import * as React from 'react';
import { Upload } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { fetchJson } from '@/lib/api';
import { commonStyles } from '@/styles/common';
import { media } from '@/styles/constants.stylex';
import type { ExternalMaterial } from '@/lib/types';
import { colors, layout, typography } from '@/styles/tokens.stylex';

const styles = stylex.create({
  uploadForm: {
    padding: '1rem',
    borderColor: colors.border,
    borderRadius: layout.radiusLarge,
    borderStyle: 'dashed',
    borderWidth: 1,
    gap: '.75rem',
    backgroundColor: colors.surface,
    display: 'grid',
  },
  uploadTitle: {
    margin: 0,
    fontSize: typography.sizeSm,
    fontWeight: typography.weightSemibold,
  },
  uploadDescription: {
    margin: 0,
    color: colors.textSecondary,
    fontSize: '0.75rem',
    marginTop: '.25rem',
  },
  uploadFields: {
    gap: '.5rem',
    display: 'grid',
    gridTemplateColumns: {
      default: 'minmax(0, 1fr) auto',
      [media.narrow]: '1fr',
    },
  },
  fileInput: {
    borderColor: colors.border,
    borderRadius: layout.radius,
    borderStyle: 'solid',
    borderWidth: 1,
    paddingBlock: '.5rem',
    paddingInline: '.75rem',
    backgroundColor: colors.surface,
    fontSize: typography.sizeSm,
    minWidth: 0,
  },
  notice: {
    margin: 0,
    color: colors.textSecondary,
    fontSize: '0.75rem',
  },
});

interface UploadPayload {
  message: string;
  material: ExternalMaterial;
}

export interface UploadPanelProps {
  courseId: string | number;
  onUploaded: (material: ExternalMaterial) => void;
}

interface UploadFormState {
  file: File | null;
  setFile: React.Dispatch<React.SetStateAction<File | null>>;
  busy: boolean;
  notice: string;
  submit: (event: React.FormEvent<HTMLFormElement>) => Promise<void>;
}

function useUploadForm({ courseId, onUploaded }: UploadPanelProps) {
  const [file, setFile] = React.useState<File | null>(null);
  const [busy, setBusy] = React.useState(false);
  const [notice, setNotice] = React.useState('');
  const submit = async (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (!file) return;
    const form = event.currentTarget;
    setBusy(true);
    setNotice('');
    const body = new FormData();
    body.set('file', file);
    try {
      const payload = await fetchJson<UploadPayload>(`/api/v1/courses/${courseId}/materials`, {
        method: 'POST',
        body,
      });
      onUploaded(payload.material);
      setFile(null);
      form.reset();
      setNotice(payload.message);
    } catch (error) {
      setNotice(error instanceof Error ? error.message : 'The upload failed.');
    } finally {
      setBusy(false);
    }
  };
  const state: UploadFormState = { file, setFile, busy, notice, submit };
  return state;
}

function UploadFields({ state }: { state: UploadFormState }) {
  return (
    <div {...stylex.props(styles.uploadFields)}>
      <label {...stylex.props(commonStyles.srOnly)} htmlFor="course-material-file">
        Choose file
      </label>
      <input
        id="course-material-file"
        name="file"
        type="file"
        required
        onChange={(event) => state.setFile(event.target.files?.[0] || null)}
        {...stylex.props(styles.fileInput)}
      />
      <Button type="submit" disabled={!state.file || state.busy} icon={<Upload aria-hidden="true" />}>
        {state.busy ? 'Uploading…' : 'Upload'}
      </Button>
    </div>
  );
}

export function MaterialUploadPanel(props: UploadPanelProps) {
  const state = useUploadForm(props);
  return (
    <form onSubmit={state.submit} {...stylex.props(styles.uploadForm)}>
      <div>
        <h3 {...stylex.props(styles.uploadTitle)}>Add course material</h3>
        <p {...stylex.props(styles.uploadDescription)}>
          The AI sets the initial category. You can override it after upload; PDFs also become available for highlights,
          notes, questions, and bookmarks.
        </p>
      </div>
      <UploadFields state={state} />
      {state.notice ? (
        <p {...stylex.props(styles.notice)} role="status">
          {state.notice}
        </p>
      ) : null}
    </form>
  );
}
