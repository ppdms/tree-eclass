import * as stylex from '@stylexjs/stylex';
import * as React from 'react';
import type { SettingsPayload } from './types';
import { media } from '@/styles/constants.stylex';
import { Section, SettingsForm } from './settingsForm';
import { Field, ClearSecret } from './fields';
import { ConnectionTest } from './connectionTest';
import { CheckFailureSignal } from './syncStatus';

const styles = stylex.create({
  settingsMatrix: {
    gap: '1rem',
    display: 'grid',
    gridTemplateColumns: {
      default: 'repeat(2, minmax(0, 1fr))',
      [media.mobile]: 'minmax(0, 1fr)',
    },
  },
});

export interface WebhookSectionProps {
  data: SettingsPayload;
  dirtySections: Set<string>;
}

export function WebhookSection({ dirtySections }: WebhookSectionProps) {
  return (
    <Section
      id="webhook"
      title="Webhook"
      collapsible
      defaultOpen={false}
      status="Secret"
      dirty={dirtySections.has('webhook')}
      description="Send checker notifications to a Discord-compatible webhook."
    >
      <SettingsForm action="/settings/webhook">
        <Field
          label="Webhook URL"
          name="webhook_url"
          type="url"
          hint="Leave blank to keep it; use Remove to clear it."
        />
        <ClearSecret name="clear_webhook" label="Remove stored webhook" />
      </SettingsForm>
    </Section>
  );
}

export interface CredentialsSectionProps {
  data: SettingsPayload;
  dirtySections: Set<string>;
}

export function CredentialsSection({ data, dirtySections }: CredentialsSectionProps) {
  return (
    <Section
      id="credentials"
      title="eClass credentials"
      collapsible
      defaultOpen={false}
      status={data.has_credentials ? 'Configured' : 'Not configured'}
      dirty={dirtySections.has('credentials')}
      description="Credentials are used only for authenticated course checks."
    >
      <SettingsForm action="/settings/credentials">
        <div {...stylex.props(styles.settingsMatrix)}>
          <Field label="Username" name="username" defaultValue={data.credential_username || ''} required />
          <Field
            label="Password"
            name="password"
            type="password"
            hint="Leave blank to keep it; changing username requires the password."
          />
          <ClearSecret name="clear_password" label="Remove stored password" />
        </div>
      </SettingsForm>
    </Section>
  );
}

export function StorageSection({ data }: { data: SettingsPayload }) {
  return (
    <Section
      id="storage"
      title="Document storage"
      collapsible
      defaultOpen={false}
      status={data.storage?.configured ? 'Local storage' : 'Not configured'}
      description="Course files and their previous versions are stored on this laptop."
    >
      <p>Storage starts and stops with the application; files are kept on this laptop.</p>
      <ConnectionTest endpoint="/api/settings/test-storage" label="Check document storage" />
      <CheckFailureSignal />
    </Section>
  );
}

export interface IntegrationSectionsProps {
  data: SettingsPayload;
  dirtySections: Set<string>;
}

export function IntegrationSections({ data, dirtySections }: IntegrationSectionsProps) {
  return (
    <>
      <WebhookSection data={data} dirtySections={dirtySections} />
      <CredentialsSection data={data} dirtySections={dirtySections} />
      <StorageSection data={data} />
    </>
  );
}
