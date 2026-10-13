import { useCallback, useState } from 'react';
import { colors } from './palette';
import { Text, TextField, View } from './primitives';
import type { FieldOptions, UIStyle } from './types';

export interface FieldProps extends Omit<FieldOptions, 'placeholder' | 'onMetrics'> {
  id: string;
  label?: string;
  placeholder?: string;
  disabled?: boolean;
  error?: string;
  style?: UIStyle;
}

interface FieldMetrics {
  scrollHeight: number;
  clientHeight: number;
}

function fieldHeight(rows: number | undefined, maxHeight: number | undefined, metrics: FieldMetrics): number {
  const minimum = Math.max(1, Number.isFinite(rows) ? Math.floor(rows!) : 3) * 21.6 + 24;
  const limit = maxHeight !== undefined && Number.isFinite(maxHeight) && maxHeight > 0 ? maxHeight : Infinity;
  // Native scrollHeight includes padding but excludes the one-pixel borders.
  return Math.min(limit, Math.max(minimum, metrics.scrollHeight + 2));
}

function useFieldMetrics() {
  const [metrics, setMetrics] = useState<FieldMetrics>({ scrollHeight: 0, clientHeight: 0 });
  const onMetrics = useCallback((next: FieldMetrics) => {
    if (!Number.isFinite(next.scrollHeight) || !Number.isFinite(next.clientHeight)) return;
    setMetrics((previous) =>
      previous.scrollHeight === next.scrollHeight && previous.clientHeight === next.clientHeight ? previous : next,
    );
  }, []);
  return { metrics, onMetrics };
}

function fieldStyle(
  error: string | undefined,
  readOnly: boolean | undefined,
  disabled: boolean,
  height: number | undefined,
): UIStyle {
  const style: UIStyle = {
    borderColor: error ? colors.danger : colors.border,
    backgroundColor: readOnly ? colors.raised : colors.surface,
  };
  if (height !== undefined) style.height = height;
  if (disabled) style.opacity = 0.5;
  return style;
}

function enabledAction(action: (() => void) | undefined, disabled: boolean) {
  return action
    ? () => {
        if (!disabled) action();
      }
    : undefined;
}

type FieldInputProps = Omit<FieldProps, 'label' | 'style'>;

function FieldInput({
  id,
  onChange,
  disabled = false,
  error,
  multiline = false,
  rows,
  autoResize = false,
  maxHeight,
  onBlur,
  onSubmit,
  submitOnEnter,
  readOnly,
  ...constraints
}: FieldInputProps) {
  const { metrics, onMetrics } = useFieldMetrics();
  const height = multiline && autoResize ? fieldHeight(rows, maxHeight, metrics) : undefined;
  const style = fieldStyle(error, readOnly, disabled, height);
  return (
    <TextField
      id={id}
      {...constraints}
      disabled={disabled}
      multiline={multiline}
      rows={rows}
      readOnly={readOnly}
      autoResize={autoResize}
      maxHeight={maxHeight}
      onMetrics={multiline && autoResize ? onMetrics : undefined}
      submitOnEnter={submitOnEnter ?? !multiline}
      onChange={(next) => {
        if (!disabled && !readOnly) onChange(next);
      }}
      onBlur={enabledAction(onBlur, disabled)}
      onSubmit={enabledAction(onSubmit, disabled)}
      style={style}
    />
  );
}

export function Field({ id, label, disabled = false, error, style, ...input }: FieldProps) {
  return (
    <View id={`${id}-group`} style={{ minWidth: 0, gap: 6, ...style }}>
      {label && (
        <Text
          id={`${id}-label`}
          disabled={disabled}
          nativeAction={disabled ? undefined : { kind: 'focus-field', fieldId: id }}
          style={{ fontSize: 13, fontWeight: 600, color: disabled ? colors.muted : colors.text }}
        >
          {label}
        </Text>
      )}
      <FieldInput {...input} id={id} disabled={disabled} error={error} />
      {error && (
        <Text id={`${id}-error`} style={{ fontSize: 13, color: colors.danger }}>
          {error}
        </Text>
      )}
    </View>
  );
}
