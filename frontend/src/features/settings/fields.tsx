import * as stylex from '@stylexjs/stylex';
import type * as React from 'react';
import { media } from '@/styles/constants.stylex';
import { colors, typography } from '@/styles/tokens.stylex';

const styles = stylex.create({
  formGroup: {
    gap: '0.35rem',
    display: 'flex',
    flexDirection: 'column',
    marginBottom: '1rem',
  },
  label: {
    color: colors.textPrimary,
    fontSize: typography.sizeSm,
    fontWeight: typography.weightMedium,
    lineHeight: 1.4,
  },
  input: {
    borderColor: {
      default: colors.border,
      ':hover:not(:disabled)': colors.borderLight,
      ':focus': colors.focusRing,
    },
    borderRadius: '0.5rem',
    borderStyle: 'solid',
    borderWidth: 1,
    outline: 'none',
    paddingInline: '0.75rem',
    backgroundColor: colors.surface,
    boxShadow: {
      default: 'none',
      ':focus': `0 0 0 0.1875rem color-mix(in srgb, ${colors.focusRing} 28%, transparent)`,
    },
    boxSizing: 'border-box',
    color: colors.textPrimary,
    cursor: { default: 'text', ':disabled': 'not-allowed' },
    fontFamily: 'inherit',
    fontSize: typography.sizeBase,
    opacity: { default: 1, ':disabled': 0.65 },
    transitionDuration: '150ms',
    transitionProperty: 'border-color, box-shadow, background-color',
    height: '2.5rem',
    minWidth: 0,
    width: '100%',
  },

  inputNumber: {
    width: { default: 'min(100%, 13.75rem)', [media.mobile]: '100%' },
  },
  inputInvalid: {
    borderColor: colors.danger,
  },
  hint: {
    color: colors.textSecondary,
    display: 'block',
    fontSize: typography.sizeXs,
    marginTop: '0.15rem',
  },
  fieldError: {
    color: colors.danger,
    display: 'block',
    fontSize: typography.sizeSm,
  },
  checkRow: {
    gap: '0.625rem',
    alignItems: 'center',
    color: colors.textPrimary,
    cursor: 'pointer',
    display: 'flex',
    fontSize: typography.sizeSm,
    minHeight: '2.5rem',
  },
  checkRowDestructive: {
    color: colors.danger,
  },
  destructiveInput: {
    accentColor: colors.danger,
  },
});

export const settingsFieldStyles = styles;

export interface FieldProps {
  label: string;
  name: string;
  type?: React.InputHTMLAttributes<HTMLInputElement>['type'];
  value?: string | number;
  defaultValue?: string | number;
  required?: boolean;
  min?: string | number;
  max?: string | number;
  hint?: string;
  invalid?: boolean;
  error?: string;
  placeholder?: string;
}

function FieldInput({
  name,
  type,
  inputProps,
  required,
  min,
  max,
  invalid,
  describedBy,
  placeholder,
}: {
  name: string;
  type: React.InputHTMLAttributes<HTMLInputElement>['type'];
  inputProps: { value: string | number } | { defaultValue?: string | number };
  required: boolean;
  min?: string | number;
  max?: string | number;
  invalid: boolean;
  describedBy?: string;
  placeholder?: string;
}) {
  return (
    <input
      id={name}
      name={name}
      type={type}
      {...inputProps}
      required={required}
      min={min}
      max={max}
      aria-invalid={invalid || undefined}
      aria-describedby={describedBy}
      placeholder={placeholder}
      {...stylex.props(styles.input, type === 'number' && styles.inputNumber, invalid && styles.inputInvalid)}
    />
  );
}

export function Field({
  label,
  name,
  type = 'text',
  value,
  defaultValue,
  required = false,
  min,
  max,
  hint,
  invalid = false,
  error,
  placeholder,
}: FieldProps) {
  const inputProps = value !== undefined ? { value } : { defaultValue };
  const describedBy = [hint && `${name}-hint`, error && `${name}-error`].filter(Boolean).join(' ') || undefined;
  return (
    <div {...stylex.props(styles.formGroup)}>
      <label htmlFor={name} {...stylex.props(styles.label)}>
        {label}
      </label>
      <FieldInput
        name={name}
        type={type}
        inputProps={inputProps}
        required={required}
        min={min}
        max={max}
        invalid={invalid}
        describedBy={describedBy}
        placeholder={placeholder}
      />
      {hint && (
        <small id={`${name}-hint`} {...stylex.props(styles.hint)}>
          {hint}
        </small>
      )}
      {error && (
        <small id={`${name}-error`} {...stylex.props(styles.fieldError)} role="alert">
          {error}
        </small>
      )}
    </div>
  );
}

export interface ClearSecretProps {
  name: string;
  label: string;
}

export function ClearSecret({ name, label }: ClearSecretProps) {
  return (
    <label {...stylex.props(styles.checkRow, styles.checkRowDestructive)}>
      <input type="checkbox" name={name} value="true" {...stylex.props(styles.destructiveInput)} /> <span>{label}</span>
    </label>
  );
}
