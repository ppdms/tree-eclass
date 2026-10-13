import { useRef, useState } from 'react';
import { z } from 'zod/v4';
import type { SyncStatusPayload } from '@/features/settings/types';
import { request } from '../app/api';
import { Button, Notice } from '../ui/controls';
import { Text } from '../ui/primitives';
import { Section } from './section';
import { FormCheck, FormField, SaveChanges, serializeCheckboxes, useSettingsForm } from './form';
import { validateWebhook } from './validation';
import type { SettingsPayload } from './shared';

const storageProbeSchema = z.object({ message: z.string(), ok: z.unknown().optional() });

function WebhookForm() {
  const form = useSettingsForm({
    section: 'webhook',
    endpoint: '/api/v1/settings/webhook',
    initial: { webhook_url: '', clear_webhook: 'false' },
    validate: validateWebhook,
    serialize: serializeCheckboxes,
    nativeFields: ['webhook_url'],
  });
  return (
    <>
      <FormField
        form={form}
        id="webhook_url"
        label="Webhook URL"
        type="url"
        sensitive
        autoComplete="off"
        hint="Leave blank to keep it; use Remove to clear it."
      />
      <FormCheck form={form} id="clear_webhook" label="Remove stored webhook" clear />
      <SaveChanges form={form} />
    </>
  );
}
function CredentialsForm({ data }: { data: SettingsPayload }) {
  const form = useSettingsForm({
    section: 'credentials',
    endpoint: '/api/v1/settings/credentials',
    initial: { username: data.credential_username || '', password: '', clear_password: 'false' },
    serialize: serializeCheckboxes,
    nativeFields: ['username', 'password'],
    validate: (values) => {
      const username = values.username?.trim() ?? '';
      if (!username) return { field: 'username', message: 'Username is required.' };
      // Native SaveCredentials checks blank retention against the committed pair in its transaction.
      return null;
    },
  });
  return (
    <>
      <FormField form={form} id="username" label="Username" required autoComplete="username" />
      <FormField
        form={form}
        id="password"
        label="Password"
        type="password"
        autoComplete="current-password"
        hint="Leave blank to keep it; changing username requires the password."
      />
      <FormCheck form={form} id="clear_password" label="Remove stored password" clear />
      <SaveChanges form={form} />
    </>
  );
}
function useStorageProbe() {
  const [busy, setBusy] = useState(false);
  const pending = useRef(false);
  const [result, setResult] = useState<{ ok: boolean; message: string } | null>(null);
  const test = async () => {
    if (pending.current) return;
    pending.current = true;
    setBusy(true);
    setResult(null);
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 10000);
    try {
      const response = await request('/api/settings/test-storage', {
        method: 'POST',
        signal: controller.signal,
        headers: { Accept: 'application/json' },
      });
      const parsed = storageProbeSchema.safeParse(await response.json());
      if (parsed.success && parsed.data.message) {
        setResult({ ok: Boolean(parsed.data.ok), message: parsed.data.message });
      } else setResult({ ok: false, message: 'Could not reach the test endpoint.' });
    } catch {
      setResult({ ok: false, message: 'Could not reach the test endpoint.' });
    } finally {
      clearTimeout(timer);
      pending.current = false;
      setBusy(false);
    }
  };
  return { busy, result, test };
}
function StorageProbe() {
  const { busy, result, test } = useStorageProbe();
  return (
    <>
      <Text>Storage starts and stops with the application; files are kept on this laptop.</Text>
      <Button id="settings-test-storage" disabled={busy} onClick={() => void test()}>
        {busy ? 'Testing…' : 'Check document storage'}
      </Button>
      {result && <Notice tone={result.ok ? 'success' : 'danger'}>{result.message}</Notice>}
    </>
  );
}
export function IntegrationSections({ data, sync }: { data: SettingsPayload; sync: SyncStatusPayload | null }) {
  return (
    <>
      <Section
        id="webhook"
        title="Webhook"
        advanced
        status="Secret"
        description="Send checker notifications to a Discord-compatible webhook."
      >
        <WebhookForm />
      </Section>
      <Section
        id="credentials"
        title="eClass credentials"
        advanced
        status={data.has_credentials ? 'Configured' : 'Not configured'}
        description="Credentials are used only for authenticated course checks."
      >
        <CredentialsForm data={data} />
      </Section>
      <Section
        id="storage"
        title="Document storage"
        advanced
        status={data.storage?.configured ? 'Local storage' : 'Not configured'}
        description="Course files and their previous versions are stored on this laptop."
      >
        <StorageProbe />
        {sync?.check?.last_error && <Notice tone="danger">{`Last check failed: ${sync.check.last_error}`}</Notice>}
      </Section>
    </>
  );
}
