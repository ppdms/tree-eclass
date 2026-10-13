import { useCallback, useEffect, useRef, useState } from 'react';
import { z } from 'zod/v4';
import { request } from '../app/api';
import { focusField, validateNativeFields } from '../app/browser';
import { Button, Notice } from '../ui/controls';
import { Checkbox, Field, Select } from '../ui/forms';
import type { ComponentProps } from 'react';
import { View } from '../ui/primitives';
import { Hint, message, styles, useSettingsContext } from './shared';
import { publishSettingsSaved } from './data';

export type FormValues = Record<string, string>;
export type Validation = { field?: string; message: string } | null;
interface FormOptions {
  section: string;
  endpoint: string;
  initial: FormValues;
  validate?: (values: FormValues) => Validation;
  serialize?: (values: FormValues) => FormValues;
  nativeFields?: readonly string[] | ((values: FormValues) => readonly string[]);
}
interface FormFeedback {
  busy: boolean;
  saved: boolean;
  error: string | null;
}
export interface SettingsFormModel {
  values: FormValues;
  change(key: string, value: string): void;
  supply(defaults: FormValues): void;
  submit(): Promise<void>;
  state: FormFeedback;
  section: string;
}

const errorBodySchema = z.object({ detail: z.unknown().optional() });
const detailItemSchema = z.object({ msg: z.unknown().optional() });

function dynamicNativeFields(fields: FormOptions['nativeFields']): fields is (values: FormValues) => readonly string[] {
  return typeof fields === 'function';
}

async function settingsError(response: Response): Promise<string> {
  const fallback = `Could not save settings (HTTP ${response.status}).`;
  try {
    const parsed = errorBodySchema.safeParse(await response.json());
    if (!parsed.success) return fallback;
    const detail = parsed.data.detail;
    if (Array.isArray(detail)) {
      return detail
        .map((item) => {
          const parsedItem = detailItemSchema.safeParse(item);
          return String(parsedItem.success ? (parsedItem.data.msg ?? item) : item);
        })
        .join('. ');
    }
    return detail ? String(detail) : fallback;
  } catch {
    // Non-JSON error bodies retain the HTTP diagnostic.
    return fallback;
  }
}

async function postSettings(options: FormOptions, snapshot: FormValues): Promise<Validation> {
  const fields = dynamicNativeFields(options.nativeFields)
    ? options.nativeFields(snapshot)
    : (options.nativeFields ?? []);
  if (fields.length) {
    const validation = await validateNativeFields(fields);
    if (!validation.valid) {
      const invalid = validation.invalid[0];
      return { field: invalid?.id, message: invalid?.message || 'Review the settings fields.' };
    }
  }
  const response = await request(options.endpoint, {
    method: 'POST',
    headers: { Accept: 'application/json', 'Content-Type': 'application/json' },
    body: JSON.stringify(options.serialize?.(snapshot) ?? snapshot),
  });
  if (response.ok) return null;
  throw new Error(await settingsError(response));
}

function useDraft(initial: FormValues, onEdit: () => void) {
  const [values, setValues] = useState(initial);
  const current = useRef(values);
  const revision = useRef(0);
  const dirty = useRef(false);
  useEffect(() => {
    if (dirty.current) return;
    // Refresh pristine server fields without dropping defaults supplied by a lazy catalog.
    for (const key of Object.keys(initial)) {
      if (current.current[key] === initial[key]) continue;
      current.current = { ...current.current, ...initial };
      setValues(current.current);
      return;
    }
  }, [initial]);
  const change = (key: string, value: string) => {
    current.current = { ...current.current, [key]: value };
    revision.current++;
    dirty.current = true;
    setValues(current.current);
    onEdit();
  };
  const supply = useCallback((defaults: FormValues) => {
    const missing = Object.entries(defaults).filter(([key]) => current.current[key] === undefined);
    if (!missing.length) return;
    current.current = { ...Object.fromEntries(missing), ...current.current };
    setValues(current.current);
  }, []);
  return { values, current, revision, dirty, change, supply };
}

export function useSettingsForm(options: FormOptions): SettingsFormModel {
  const context = useSettingsContext();
  const [state, setState] = useState<FormFeedback>({ busy: false, saved: false, error: null });
  const draft = useDraft(options.initial, () => {
    context.setDirty(options.section, true);
    setState((previous) => ({ ...previous, saved: false }));
  });
  const busy = useRef(false);
  const alive = useRef(true);
  useEffect(() => {
    alive.current = true;
    return () => {
      alive.current = false;
    };
  }, []);
  const submit = async () => {
    if (busy.current || !draft.dirty.current) return;
    const invalid = options.validate?.(draft.current.current);
    if (invalid) {
      setState({ busy: false, saved: false, error: invalid.message });
      if (invalid.field) focusField(invalid.field);
      return;
    }
    busy.current = true;
    const submittedRevision = draft.revision.current;
    const snapshot = { ...draft.current.current };
    setState({ busy: true, saved: false, error: null });
    try {
      const invalid = await postSettings(options, snapshot);
      if (invalid) {
        setState({ busy: false, saved: false, error: invalid.message });
        if (invalid.field) focusField(invalid.field);
        return;
      }
      publishSettingsSaved();
      if (alive.current) {
        draft.dirty.current = draft.revision.current !== submittedRevision;
        context.setDirty(options.section, draft.dirty.current);
        setState({ busy: false, saved: true, error: null });
      }
    } catch (error) {
      if (alive.current) setState({ busy: false, saved: false, error: message(error, 'Could not save settings.') });
    } finally {
      busy.current = false;
    }
  };
  const { values, change, supply } = draft;
  return { values, change, supply, submit, state, section: options.section };
}
export function SaveChanges({ form }: { form: SettingsFormModel }) {
  const context = useSettingsContext();
  const dirty = context.dirty.has(form.section);
  return (
    <View style={styles.stack}>
      {form.state.error && <Notice tone="danger">{form.state.error}</Notice>}
      {form.state.saved && (
        <Notice tone="success">{dirty ? 'Submitted changes saved; newer edits are unsaved.' : 'Saved'}</Notice>
      )}
      <Button
        id={`settings-save-${form.section}`}
        disabled={form.state.busy || !dirty}
        onClick={() => void form.submit()}
      >
        {form.state.busy ? 'Saving…' : 'Save changes'}
      </Button>
    </View>
  );
}

type FormFieldProps = Omit<ComponentProps<typeof Field>, 'value' | 'onChange' | 'onSubmit'> & {
  form: SettingsFormModel;
  hint?: string;
};
export function FormField({ form, hint, ...props }: FormFieldProps) {
  return (
    <View style={styles.stack}>
      <Field
        {...props}
        value={form.values[props.id] ?? ''}
        onChange={(value) => form.change(props.id, value)}
        onSubmit={() => void form.submit()}
      />
      {hint && <Hint>{hint}</Hint>}
    </View>
  );
}
export function FormCheck({
  form,
  id,
  label,
  clear = false,
}: {
  form: SettingsFormModel;
  id: string;
  label: string;
  clear?: boolean;
}) {
  return (
    <Checkbox
      id={id}
      label={label}
      checked={['on', 'true'].includes(form.values[id] ?? '')}
      onChange={(checked) => form.change(id, checked ? (clear ? 'true' : 'on') : 'false')}
    />
  );
}
export function FormSelect({
  form,
  id,
  label,
  options,
}: {
  form: SettingsFormModel;
  id: string;
  label: string;
  options: { value: string; label: string; disabled?: boolean }[];
}) {
  return (
    <Select
      id={id}
      label={label}
      value={form.values[id] ?? ''}
      options={options}
      onChange={(value) => form.change(id, value)}
    />
  );
}

const checkboxes = new Set([
  'chat_think',
  'ai_enrichment_enabled',
  'ai_course_synthesis_enabled',
  'ai_practice_questions_enabled',
  'enabled',
  'media',
  'clear_token',
  'clear_webhook',
  'clear_password',
]);
export function serializeCheckboxes(values: FormValues, explicitFalse: readonly string[] = []): FormValues {
  return Object.fromEntries(
    Object.entries(values).filter(
      ([key, value]) =>
        value !== 'false' ||
        explicitFalse.includes(key) ||
        !(checkboxes.has(key) || key.startsWith('provider_enabled_')),
    ),
  );
}
