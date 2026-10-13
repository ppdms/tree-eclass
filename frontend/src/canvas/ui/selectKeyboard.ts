import { useRef } from 'react';
import { focusNode } from '../app/browser';
import type { CanvasKey } from './types';
import type { SelectOption, SelectProps } from './selectTypes';

interface SearchBuffer {
  text: string;
  at: number;
}

interface KeyboardState {
  props: SelectProps;
  open: boolean;
  active: string | undefined;
  options: SelectOption[];
  openWith: (value?: string) => void;
  dismiss: () => void;
  pick: (value: string | undefined) => void;
  activate: (value: string | undefined) => void;
}

function navigationValue(key: string, options: SelectOption[], active: string | undefined): string | undefined {
  if (!options.length) return undefined;
  if (key === 'Home') return options[0]?.value;
  if (key === 'End') return options[options.length - 1]?.value;
  const index = options.findIndex((option) => option.value === active);
  if (key === 'ArrowDown') return options[(index + 1) % options.length]?.value;
  if (key === 'ArrowUp')
    return options[index < 0 ? options.length - 1 : (index - 1 + options.length) % options.length]?.value;
  return undefined;
}

function searchValue(key: string, options: SelectOption[], active: string | undefined, buffer: SearchBuffer) {
  const now = performance.now();
  const character = key.toLocaleLowerCase();
  buffer.text = now - buffer.at > 700 ? character : buffer.text + character;
  buffer.at = now;
  const repeated = [...buffer.text].every((letter) => letter === character);
  const query = repeated ? character : buffer.text;
  const index = options.findIndex((option) => option.value === active);
  const start = repeated || buffer.text.length === character.length ? index + 1 : Math.max(0, index);
  for (let offset = 0; offset < options.length; offset++) {
    const option = options[(start + offset) % options.length];
    if (option?.label.trimStart().toLocaleLowerCase().startsWith(query)) return option.value;
  }
  return undefined;
}

export function useSelectKeyboard(state: KeyboardState) {
  const buffer = useRef<SearchBuffer>({ text: '', at: -Infinity });
  return (event: CanvasKey) => {
    if (state.props.disabled || event.composing || event.ctrl || event.meta || event.alt) return;
    if (event.key === 'Tab' && state.open) {
      buffer.current.text = '';
      state.dismiss();
      focusNode(state.props.id);
      return;
    }
    if (event.key === 'Escape' && state.open) {
      buffer.current.text = '';
      state.dismiss();
      focusNode(state.props.id);
      return true;
    }
    if (event.key === 'Enter' || event.key === ' ') {
      if (event.repeat) return true;
      buffer.current.text = '';
      if (state.open) state.pick(state.active);
      else state.openWith();
      return true;
    }
    if (['ArrowDown', 'ArrowUp', 'Home', 'End'].includes(event.key)) {
      buffer.current.text = '';
      const next = navigationValue(event.key, state.options, state.active);
      if (state.open) state.activate(next);
      else state.openWith(event.key === 'Home' || event.key === 'End' ? next : (state.active ?? next));
      return true;
    }
    if ([...event.key].length !== 1 || event.key === ' ') return;
    const next = searchValue(event.key, state.options, state.active, buffer.current);
    if (next !== undefined) {
      if (state.open) state.activate(next);
      else state.openWith(next);
    }
    return true;
  };
}
