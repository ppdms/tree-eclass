import { useEffect, useState } from 'react';
import { focusNode, scrollToNode } from '../app/browser';
import { useSelectKeyboard } from './selectKeyboard';
import { selectOptionId, type SelectProps } from './selectTypes';

export function useSelectController(props: SelectProps) {
  const [open, setOpen] = useState(false);
  const [activeValue, setActive] = useState<string>();
  const options = props.options.filter((option) => !option.disabled);
  const selected = options.find((option) => option.value === props.value)?.value;
  const active = open
    ? options.some((option) => option.value === activeValue)
      ? activeValue
      : (selected ?? options[0]?.value)
    : selected;
  const dismiss = () => {
    setOpen(false);
  };
  const openWith = (value = selected ?? options[0]?.value) => {
    if (props.disabled) return;
    setActive(value);
    setOpen(true);
  };
  const pick = (value: string | undefined) => {
    if (props.disabled || value === undefined || !options.some((option) => option.value === value)) return;
    if (value !== props.value) props.onChange(value);
    setOpen(false);
    focusNode(props.id);
  };
  useEffect(() => {
    if (props.disabled) setOpen(false);
  }, [props.disabled]);
  useEffect(() => {
    if (!open || props.disabled || active === undefined) return;
    const id = selectOptionId(props.id, active);
    focusNode(id);
    scrollToNode(id);
  }, [open, active, props.id, props.disabled]);
  const onKeyInput = useSelectKeyboard({ props, open, active, options, openWith, dismiss, pick, activate: setActive });
  return {
    open: open && !props.disabled,
    active,
    dismiss,
    pick,
    onKeyInput,
    toggle: () => {
      if (open) dismiss();
      else openWith();
    },
  };
}
