import * as stylex from '@stylexjs/stylex';
import * as React from 'react';
import { z } from 'zod/v4';
import { ConfirmDialog } from '@/components/ui/confirm-dialog';
import { buttonStyles } from '@/components/ui/styles';
import { media } from '@/styles/constants.stylex';
import { colors, layout, typography } from '@/styles/tokens.stylex';
import { DIRTY_FORMS } from './state';

const styles = stylex.create({
  settingsFormError: {
    color: colors.danger,
    fontSize: typography.sizeSm,
    marginTop: '0.75rem',
  },
  settingsFormSuccess: {
    color: colors.success,
    fontSize: typography.sizeSm,
    marginTop: '0.75rem',
  },
  settingsDirtySubmit: {
    justifySelf: 'start',
  },
  settingsFormGrid: {
    gap: '1rem',
    display: 'grid',
  },
  section: {
    padding: {
      default: '1.25rem',
      [media.mobile]: '1rem',
    },
    borderColor: colors.border,
    borderRadius: '1rem',
    borderStyle: 'solid',
    borderWidth: 1,
    backgroundColor: colors.surface,
    marginBottom: '1rem',
    minWidth: 0,
    scrollMarginTop: '5.5rem',
  },
  sectionHeader: {
    gap: '1rem',
    alignItems: 'flex-start',
    display: 'flex',
    justifyContent: 'space-between',
    marginBottom: 0,
  },
  sectionSummary: {
    gap: '1rem',
    alignItems: 'flex-start',
    cursor: 'pointer',
    display: 'flex',
    justifyContent: 'space-between',
    marginBottom: 0,
  },
  sectionSummaryOpen: {
    marginBottom: '0.75rem',
  },
  sectionTitle: {
    margin: 0,
    color: colors.textPrimary,
    fontSize: typography.sizeXl,
    fontWeight: typography.weightSemibold,
  },
  sectionStatus: {
    borderColor: colors.border,
    borderRadius: layout.radiusPill,
    borderStyle: 'solid',
    borderWidth: 1,
    paddingBlock: '0.2rem',
    paddingInline: '0.55rem',
    alignItems: 'center',
    backgroundColor: colors.surfaceRaised,
    color: colors.textSecondary,
    display: 'inline-flex',
    fontSize: typography.sizeXs,
    fontWeight: typography.weightMedium,
  },
  sectionDescription: {
    margin: 0,
    color: colors.textSecondary,
    fontSize: typography.sizeSm,
    lineHeight: 1.5,
    marginBottom: '0.75rem',
    maxWidth: '52rem',
  },
});

const errorBodySchema = z
  .object({
    detail: z.unknown(),
  })
  .partial();

const detailItemSchema = z
  .object({
    msg: z.string(),
  })
  .partial();

interface FormState {
  busy: boolean;
  error: string | null;
  saved: boolean;
}

interface UseSettingsFormResult {
  state: FormState;
  dirty: boolean;
  markDirty: () => void;
  submit: (event: React.FormEvent<HTMLFormElement>) => void;
  save: (formData: FormData | null) => Promise<void>;
  pendingData: FormData | null;
  confirmOpen: boolean;
  setConfirmOpen: React.Dispatch<React.SetStateAction<boolean>>;
}

async function saveSettings(action: string, formData: FormData): Promise<void> {
  const response = await fetch(action, {
    method: 'POST',
    body: formData,
    headers: { Accept: 'application/json' },
  });
  if (!response.ok) {
    let message = `Could not save settings (HTTP ${response.status}).`;
    try {
      const parsed = errorBodySchema.safeParse(await response.json());
      if (parsed.success) {
        const detail = parsed.data.detail;
        message = Array.isArray(detail)
          ? detail
              .map((item) => {
                const parsedItem = detailItemSchema.safeParse(item);
                return parsedItem.success && parsedItem.data.msg ? parsedItem.data.msg : String(item);
              })
              .join('. ')
          : String(detail || message);
      }
    } catch {
      /* safe HTML error surface */
    }
    throw new Error(message);
  }
}

function useSettingsForm(action: string, confirm?: string): UseSettingsFormResult {
  const [state, setState] = React.useState<FormState>({ busy: false, error: null, saved: false });
  const [dirty, setDirty] = React.useState(false);
  const [pendingData, setPendingData] = React.useState<FormData | null>(null);
  const [confirmOpen, setConfirmOpen] = React.useState(false);
  const markDirty = () => {
    if (!dirty) {
      setDirty(true);
      DIRTY_FORMS.add(action);
      window.dispatchEvent(new Event('settings-dirty-change'));
    }
    setState((current) => (current.saved ? { ...current, saved: false } : current));
  };
  const clearDirty = () => {
    if (dirty) {
      setDirty(false);
      DIRTY_FORMS.delete(action);
      window.dispatchEvent(new Event('settings-dirty-change'));
    }
  };
  const save = async (formData: FormData | null) => {
    setConfirmOpen(false);
    setState({ busy: true, error: null, saved: false });
    try {
      if (!formData) throw new Error('Missing form data.');
      await saveSettings(action, formData);
      setState({ busy: false, error: null, saved: true });
      clearDirty();
      window.dispatchEvent(new Event('settings-saved'));
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Could not save settings.';
      setState({ busy: false, error: message, saved: false });
    }
  };
  const submit = (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const formData = new FormData(event.currentTarget);
    if (confirm && !confirmOpen) {
      setPendingData(formData);
      setConfirmOpen(true);
      return;
    }
    save(formData);
  };
  return { state, dirty, markDirty, submit, save, pendingData, confirmOpen, setConfirmOpen };
}

export interface SettingsFormProps {
  action: string;
  children: React.ReactNode;
  confirm?: string;
}

function SettingsFormFeedback({ state }: { state: FormState }) {
  return (
    <>
      {state.error && (
        <p {...stylex.props(styles.settingsFormError)} role="alert">
          {state.error}
        </p>
      )}
      {state.saved && (
        <p {...stylex.props(styles.settingsFormSuccess)} role="status">
          Saved
        </p>
      )}
    </>
  );
}

function SettingsFormSubmit({ state, dirty }: { state: FormState; dirty: boolean }) {
  return (
    <button
      {...stylex.props(buttonStyles.base, buttonStyles.secondary, styles.settingsDirtySubmit)}
      type="submit"
      disabled={state.busy || !dirty}
    >
      {state.busy ? 'Saving…' : 'Save changes'}
    </button>
  );
}

export function SettingsForm({ action, children, confirm }: SettingsFormProps) {
  const { state, dirty, markDirty, submit, save, pendingData, confirmOpen, setConfirmOpen } = useSettingsForm(
    action,
    confirm,
  );
  return (
    <>
      <form
        method="POST"
        action={action}
        onSubmit={submit}
        onChange={markDirty}
        {...stylex.props(styles.settingsFormGrid)}
      >
        {children}
        <SettingsFormFeedback state={state} />
        <SettingsFormSubmit state={state} dirty={dirty} />
      </form>
      <ConfirmDialog
        open={confirmOpen}
        title="Apply this settings change?"
        description={confirm || ''}
        confirmLabel="Continue"
        onCancel={() => setConfirmOpen(false)}
        onConfirm={() => save(pendingData)}
      />
    </>
  );
}

export interface SectionProps {
  onOpenChange?: (open: boolean) => void;
  id: string;
  title: string;
  status?: string;
  dirty?: boolean;
  children: React.ReactNode;
  description?: string;
  collapsible?: boolean;
  defaultOpen?: boolean;
}

function SectionHeader({ title, status }: Pick<SectionProps, 'title' | 'status'>) {
  return (
    <div {...stylex.props(styles.sectionHeader)}>
      <h3 {...stylex.props(styles.sectionTitle)}>{title}</h3>
      {status && <span {...stylex.props(styles.sectionStatus)}>{status}</span>}
    </div>
  );
}

function SectionContent({ description, children }: Pick<SectionProps, 'description' | 'children'>) {
  return (
    <>
      {description && <p {...stylex.props(styles.sectionDescription)}>{description}</p>}
      {children}
    </>
  );
}

export function Section({
  id,
  title,
  status,
  dirty = false,
  children,
  description,
  collapsible = false,
  defaultOpen = true,
  onOpenChange,
}: SectionProps) {
  const [open, setOpen] = React.useState(defaultOpen);
  const [visited, setVisited] = React.useState(defaultOpen);
  if (collapsible) {
    return (
      <details
        {...stylex.props(styles.section)}
        id={id}
        data-dirty={dirty || undefined}
        open={open}
        onToggle={(event) => {
          setOpen(event.currentTarget.open);
          if (event.currentTarget.open) setVisited(true);
          onOpenChange?.(event.currentTarget.open);
        }}
      >
        <summary {...stylex.props(styles.sectionSummary, open && styles.sectionSummaryOpen)}>
          <SectionHeader title={title} status={status} />
        </summary>
        {visited && <SectionContent description={description}>{children}</SectionContent>}
      </details>
    );
  }
  return (
    <section {...stylex.props(styles.section)} id={id} data-dirty={dirty || undefined}>
      <SectionHeader title={title} status={status} />
      <SectionContent description={description}>{children}</SectionContent>
    </section>
  );
}
