import * as stylex from '@stylexjs/stylex';
import * as React from 'react';
import { ArrowUp, Square } from 'lucide-react';
import { Button as ActionButton } from '@/components/ui/button';
import { Selector } from '@astryxdesign/core/Selector';
import { Textarea } from '@/components/ui/textarea';
import { commonStyles } from '@/styles/common';
import { media } from '@/styles/constants.stylex';
import { colors, layout, typography } from '@/styles/tokens.stylex';

const styles = stylex.create({
  askModelPicker: {
    gap: 0,
    alignItems: 'center',
    color: colors.textSecondary,
    display: 'inline-flex',
    flexBasis: 'auto',
    flexGrow: 0,
    flexShrink: 0,
    fontSize: typography.sizeXs,
  },
  askModelTrigger: {
    borderColor: { default: 'transparent', ':hover': colors.border },
    borderRadius: layout.radius,
    borderStyle: 'solid',
    borderWidth: 1,
    paddingBlock: '0.25rem',
    paddingInline: '0.5rem',
    backgroundColor: { default: 'transparent', ':hover': colors.surfaceRaised },
    boxShadow: 'none',
    color: colors.textPrimarySoft,
    fontSize: typography.sizeXs,
    height: '2rem',
    minWidth: '7rem',
    width: 'auto',
  },
  askQuestionInput: {
    borderColor: 'transparent',
    borderRadius: 0,
    borderStyle: 'none',
    borderWidth: 0,
    outline: 'none',
    paddingBlock: 0,
    paddingInline: 0,
    backgroundColor: 'transparent',
    boxShadow: 'none',
    color: colors.textPrimary,
    display: 'block',
    fontSize: typography.sizeMd,
    lineHeight: 1.55,
    resize: 'none',
    maxHeight: '12rem',
    minHeight: '4.5rem',
    width: '100%',
    '::placeholder': {
      color: colors.textSecondary,
    },
  },
  askComposerField: {
    paddingInline: { default: '1.15rem', [media.narrow]: '0.85rem' },
    paddingBottom: { default: '0.25rem', [media.narrow]: '0.45rem' },
    paddingTop: { default: '1.05rem', [media.narrow]: '0.85rem' },
  },
  askSendButton: {
    padding: 0,
    borderColor: colors.primary,
    borderRadius: '0.7rem',
    borderStyle: 'solid',
    borderWidth: 1,
    outline: {
      default: 'none',
      ':focus-visible': `0.125rem solid ${colors.focusRing}`,
    },
    placeItems: 'center',
    alignItems: 'center',
    backgroundColor: { default: colors.primary, ':not(:disabled):hover': colors.primaryDim },
    color: colors.background,
    cursor: { default: 'pointer', ':disabled': 'not-allowed' },
    display: 'grid',
    flexShrink: 0,
    opacity: { default: 1, ':disabled': 0.38 },
    outlineOffset: {
      default: 0,
      ':focus-visible': '0.125rem',
    },
    transform: {
      default: 'none',
      ':hover:not(:disabled)': 'translateY(-1px)',
    },
    transitionDuration: '160ms',
    transitionProperty: 'background-color, color, transform',
    height: '2.75rem',
    width: '2.75rem',
  },
  fillCurrent: {
    fill: 'currentColor',
  },
  askComposerToolbar: {
    gap: '0.75rem',
    alignItems: 'center',
    display: 'flex',
    justifyContent: 'flex-end',
    paddingBottom: '0.55rem',
    paddingLeft: { default: '0.95rem', [media.narrow]: '0.7rem' },
    paddingRight: '0.6rem',
    paddingTop: '0.25rem',
  },
  askComposer: {
    display: 'flex',
    flexDirection: 'column',
    marginTop: '1.25rem',
    width: '100%',
  },
  askComposerSurface: {
    borderColor: { default: colors.borderLight, ':focus-within': colors.textSecondary },
    borderRadius: '1.25rem',
    borderStyle: 'solid',
    borderWidth: 1,
    overflow: 'hidden',
    backgroundColor: colors.surface,
    boxShadow: {
      default: '0 12px 30px rgba(0, 0, 0, 0.14)',
      ':focus-within': '0 12px 30px rgba(0, 0, 0, 0.2)',
    },
    transitionDuration: '150ms',
    transitionProperty: 'border-color, box-shadow',
  },
  askComposerHint: {
    color: colors.textSecondary,
    fontFamily: typography.fontFamily,
    fontSize: typography.sizeXs,
    lineHeight: layout.bodyLeading,
    marginBlockEnd: 0,
    marginBlockStart: '0.55rem',
    textAlign: 'center',
  },
});

interface ModelPickerProps {
  models: string[];
  model: string;
  setModel: (value: string) => void;
  busy: boolean;
}

/** The focused composer: question textarea, quiet model picker, send button. */
function ModelPicker({ models, model, setModel, busy }: ModelPickerProps) {
  if (!models.length) return null;
  return (
    <label {...stylex.props(styles.askModelPicker)}>
      <span {...stylex.props(commonStyles.srOnly)}>Answer with</span>
      <Selector
        label="Answer model"
        isLabelHidden
        options={models}
        value={model}
        onChange={setModel}
        isDisabled={busy}
        variant="ghost"
        size="sm"
        id="ask-model"
        {...stylex.props(styles.askModelTrigger)}
      />
    </label>
  );
}

interface ComposerFieldProps {
  input: string;
  setInput: React.Dispatch<React.SetStateAction<string>>;
  busy: boolean;
  submit: (event: React.SyntheticEvent) => void;
  available: boolean;
}

function ComposerTextarea({ input, setInput, busy, submit, available }: ComposerFieldProps) {
  return (
    <Textarea
      value={input}
      onChange={(event) => {
        event.target.style.height = 'auto';
        const rootFontSize = Number.parseFloat(getComputedStyle(document.documentElement).fontSize) || 16;
        const maxHeight = 12 * rootFontSize;
        event.target.style.height = `${Math.min(event.target.scrollHeight, maxHeight) / rootFontSize}rem`;
        setInput(event.target.value);
      }}
      onKeyDown={(event) => {
        if (event.key === 'Enter' && !event.shiftKey && !busy && available) submit(event);
      }}
      id="ask-question"
      placeholder={available ? 'What does lecture 3 say about …' : 'Asking is unavailable'}
      rows={2}
      aria-label="Question"
      disabled={!available}
      {...stylex.props(styles.askQuestionInput)}
    />
  );
}

function ComposerField({ input, setInput, busy, submit, available }: ComposerFieldProps) {
  return (
    <div {...stylex.props(styles.askComposerField)}>
      <ComposerTextarea {...{ input, setInput, busy, submit, available }} />
    </div>
  );
}

interface ComposerActionsProps {
  input: string;
  busy: boolean;
  stop: (() => void) | undefined;
  models: string[];
  model: string;
  setModel: (value: string) => void;
  available: boolean;
}

function ComposerSubmitButton({ input, busy, available }: Pick<ComposerActionsProps, 'input' | 'busy' | 'available'>) {
  return (
    <ActionButton
      type="submit"
      {...stylex.props(styles.askSendButton)}
      disabled={!available || (!busy && !input.trim())}
      aria-label={busy ? 'Stop response' : 'Send question'}
      title={busy ? 'Stop response' : 'Send question'}
      isIconOnly
      icon={busy ? <Square aria-hidden="true" {...stylex.props(styles.fillCurrent)} /> : <ArrowUp aria-hidden="true" />}
    ></ActionButton>
  );
}

function ComposerActions({ input, busy, models, model, setModel, available }: ComposerActionsProps) {
  return (
    <div {...stylex.props(styles.askComposerToolbar)}>
      <ModelPicker models={models} model={model} setModel={setModel} busy={busy || !available} />
      <ComposerSubmitButton input={input} busy={busy} available={available} />
    </div>
  );
}

interface ComposerProps {
  input: string;
  setInput: React.Dispatch<React.SetStateAction<string>>;
  sendMessage: (message: { text: string }) => Promise<void>;
  busy: boolean;
  stop: (() => void) | undefined;
  models: string[];
  model: string;
  setModel: (value: string) => void;
  available: boolean;
}

export function Composer({
  input,
  setInput,
  sendMessage,
  busy,
  stop,
  models,
  model,
  setModel,
  available,
}: ComposerProps) {
  const submit = (event: React.SyntheticEvent) => {
    event.preventDefault();
    if (!available) return;
    if (busy) return stop?.();
    const question = input.trim();
    if (!question) return;
    try {
      Promise.resolve(sendMessage({ text: question }))
        .then(() => setInput(''))
        .catch(() => setInput(question));
    } catch {
      setInput(question);
    }
  };
  return <ComposerFrame {...{ input, setInput, submit, busy, stop, models, model, setModel, available }} />;
}

function ComposerFrame(props: Omit<ComposerProps, 'sendMessage'> & { submit: (event: React.SyntheticEvent) => void }) {
  return (
    <form onSubmit={props.submit} {...stylex.props(styles.askComposer)}>
      <div {...stylex.props(styles.askComposerSurface)}>
        <ComposerField
          input={props.input}
          setInput={props.setInput}
          busy={props.busy}
          submit={props.submit}
          available={props.available}
        />
        <ComposerActions
          input={props.input}
          busy={props.busy}
          stop={props.stop}
          models={props.models}
          model={props.model}
          setModel={props.setModel}
          available={props.available}
        />
      </div>
      <p {...stylex.props(styles.askComposerHint)}>Return sends · Shift+Return starts a new line</p>
    </form>
  );
}
