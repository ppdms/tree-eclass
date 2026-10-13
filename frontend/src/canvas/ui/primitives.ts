import { createElement } from 'react';
import type { FieldOptions, HostProps, UIStyle } from './types';
import { colors } from './palette';

// No global JSX augmentation: the DOM application keeps its own intrinsic element types.
export function View(props: HostProps) {
  return createElement('view', props);
}

export function Text(props: HostProps) {
  return createElement('text', props);
}

export function Icon(props: HostProps) {
  return createElement('icon', props);
}

export function Vector(props: HostProps) {
  return createElement('vector', props);
}

export function Raster(props: HostProps & { source: string }) {
  return createElement('raster', props);
}

interface TextFieldProps extends Omit<FieldOptions, 'placeholder'> {
  id: string;
  placeholder?: string;
  disabled?: boolean;
  style?: UIStyle;
}

export function TextField({ id, style, disabled, placeholder = '', ...field }: TextFieldProps) {
  const masked = field.sensitive || field.type === 'password';
  const value = masked ? '•'.repeat(field.value.length) : field.value;
  return createElement('text', {
    id,
    disabled,
    focusable: !disabled,
    field: { ...field, placeholder },
    style: {
      height: field.multiline ? (field.rows ?? 3) * 21.6 + 24 : 44,
      minWidth: 0,
      padding: 12,
      fontSize: 16,
      lineHeight: 1.2,
      borderWidth: 1,
      borderColor: colors.border,
      borderRadius: 12,
      backgroundColor: colors.surface,
      color: value ? colors.text : colors.muted,
      ...style,
      overflow: 'hidden',
    },
    children: value || placeholder,
  } satisfies HostProps);
}
