import * as stylex from '@stylexjs/stylex';
import * as React from 'react';
import { buttonStyles } from '@/components/ui/styles';
import type { SettingsPayload } from './types';
import { media } from '@/styles/constants.stylex';

import { Section, SettingsForm } from './settingsForm';
import { Field } from './fields';

const styles = stylex.create({
  checkRow: {
    gap: '0.5rem',
    alignItems: 'center',
    display: 'flex',
    minHeight: '2.5rem',
  },
  settingsMatrix: {
    gap: '1rem',
    display: 'grid',
    gridTemplateColumns: {
      default: 'repeat(2, minmax(0, 1fr))',
      [media.mobile]: 'minmax(0, 1fr)',
    },
  },
  settingsCheckList: {
    gap: '0.25rem',
    display: 'grid',
  },
});

export interface PreferencesProps {
  data: SettingsPayload;
  dirtySections: Set<string>;
}

const FEED_KEYS = ['dept', 'undergrad', 'rector'] as const;
type FeedKey = (typeof FEED_KEYS)[number];

const FEED_LABELS = {
  dept: 'Show department announcements in Activity',
  undergrad: 'Show undergraduate announcements in Activity',
  rector: 'Show rector announcements in Activity',
} satisfies Record<FeedKey, string>;

function feedEnabled(prefs: NonNullable<SettingsPayload['preferences']>, key: FeedKey): boolean {
  const flag = prefs[`global_feed_${key}_enabled`];
  return Boolean(flag);
}

type FeedRowProps = {
  prefs: NonNullable<SettingsPayload['preferences']>;
  keyName: FeedKey;
};

function FeedRow({ prefs, keyName }: FeedRowProps) {
  const inputName = `global_feed_${keyName}_enabled`;
  const checked = feedEnabled(prefs, keyName);
  return (
    <label {...stylex.props(styles.checkRow)}>
      <input type="checkbox" name={inputName} defaultChecked={checked} /> {FEED_LABELS[keyName]}
    </label>
  );
}

export function Preferences({ data, dirtySections }: PreferencesProps) {
  const prefs: NonNullable<SettingsPayload['preferences']> = data.preferences || {};
  return (
    <Section
      id="preferences"
      title="Application preferences"
      collapsible
      defaultOpen={false}
      dirty={dirtySections.has('preferences')}
      description="Control checker timing, the automatic course mirror, and global announcement feeds."
    >
      <SettingsForm action="/api/v1/settings/preferences">
        <div {...stylex.props(styles.settingsMatrix)}>
          <Field
            label="Check interval (minutes)"
            name="check_interval_minutes"
            type="number"
            min="5"
            max="1440"
            defaultValue={prefs.check_interval_minutes || 60}
          />
          <Field
            label="Local mirror path"
            name="download_base_path"
            defaultValue={prefs.download_base_path || ''}
            placeholder="/University"
            hint="Course checks refresh this folder under your home directory. Leave blank to disable mirroring."
          />
        </div>
        <div {...stylex.props(styles.settingsCheckList)}>
          {FEED_KEYS.map((keyName) => (
            <FeedRow key={keyName} prefs={prefs} keyName={keyName} />
          ))}
        </div>
      </SettingsForm>
    </Section>
  );
}

export function DataExport() {
  return (
    <Section
      id="data-export"
      title="Learner data"
      collapsible
      defaultOpen={false}
      description="Download a portable copy of annotations, notes, the study plan, and study history."
    >
      <a
        {...stylex.props(buttonStyles.base, buttonStyles.secondary)}
        href="/api/settings/export"
        download="tree-eclass-learner-data.json"
      >
        Download learner data
      </a>
    </Section>
  );
}
