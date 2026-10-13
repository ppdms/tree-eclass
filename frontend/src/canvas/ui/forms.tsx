import { useRef } from 'react';
import { colors } from './palette';
import { Icon, Text, View } from './primitives';
import type { UIStyle } from './types';

export { Field } from './fieldControl';
export type { FieldProps } from './fieldControl';
export { Select } from './select';
export type { SelectOption, SelectProps } from './selectTypes';
export { Range } from './range';
export type { RangeProps } from './range';

export interface CheckboxProps {
  id: string;
  label: string;
  checked: boolean;
  onChange: (checked: boolean) => void;
  disabled?: boolean;
  style?: UIStyle;
}

/** The entire label/indicator row is one activation target, never two toggling children. */
export function Checkbox({ id, label, checked, onChange, disabled = false, style }: CheckboxProps) {
  const rowStyle: UIStyle = {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 10,
    minHeight: 44,
    paddingTop: 6,
    paddingBottom: 6,
    borderRadius: 10,
    hoverBackground: colors.hover,
    ...style,
  };
  if (disabled) rowStyle.opacity = 0.5;
  return (
    <View
      id={id}
      label={label}
      pressed={checked}
      disabled={disabled}
      focusable={!disabled}
      onClick={() => {
        if (!disabled) onChange(!checked);
      }}
      style={rowStyle}
    >
      <View
        style={{
          width: 22,
          height: 22,
          flexShrink: 0,
          alignItems: 'center',
          justifyContent: 'center',
          borderRadius: 6,
          borderWidth: 1,
          borderColor: checked ? colors.accent : colors.border,
          backgroundColor: checked ? colors.accent : colors.surface,
        }}
      >
        {checked && <Icon name="check" style={{ width: 16, height: 16, color: colors.onAccent }} />}
      </View>
      <Text style={{ minWidth: 0, flexGrow: 1, flexShrink: 1, fontSize: 14, color: colors.text }}>{label}</Text>
    </View>
  );
}

export interface FilePickerProps {
  id: string;
  label: string;
  accept?: string;
  multiple?: boolean;
  disabled?: boolean;
  resetKey?: string | number;
  onFiles: (files: File[]) => void;
  style?: UIStyle;
}

function filePickerStyle(style: UIStyle | undefined, disabled: boolean): UIStyle {
  const resolved: UIStyle = {
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'center',
    gap: 8,
    minHeight: 44,
    padding: 12,
    borderWidth: 1,
    borderColor: colors.border,
    borderRadius: 12,
    backgroundColor: colors.raised,
    hoverBackground: colors.hover,
    ...style,
  };
  if (disabled) resolved.opacity = 0.5;
  return resolved;
}

/** Main-thread activation owns the real file dialog and preserves File objects unchanged. */
export function FilePicker({
  id,
  label,
  accept,
  multiple,
  disabled = false,
  resetKey,
  onFiles,
  style,
}: FilePickerProps) {
  const current = useRef({ disabled, onFiles });
  current.current = { disabled, onFiles };
  return (
    <View
      id={id}
      label={label}
      disabled={disabled}
      focusable={!disabled}
      nativeAction={
        disabled
          ? undefined
          : {
              kind: 'file',
              accept,
              multiple,
              resetKey,
              onFiles: (files) => {
                if (!current.current.disabled) current.current.onFiles(files);
              },
            }
      }
      style={filePickerStyle(style, disabled)}
    >
      <Icon name="file" style={{ width: 18, height: 18, color: colors.muted }} />
      <Text style={{ minWidth: 0, fontWeight: 600, color: colors.text }}>{label}</Text>
    </View>
  );
}
