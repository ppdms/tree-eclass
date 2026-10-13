import type { UIStyle } from './types';

export interface SelectOption {
  value: string;
  label: string;
  disabled?: boolean;
}

export interface SelectProps {
  id: string;
  label?: string;
  title?: string;
  value: string;
  options: SelectOption[];
  onChange: (value: string) => void;
  disabled?: boolean;
  style?: UIStyle;
}

/** Values, not option positions, identify both the drawn row and its keyboard target. */
export function selectOptionId(id: string, value: string): string {
  return `${id}-option-${encodeURIComponent(value)}`;
}
