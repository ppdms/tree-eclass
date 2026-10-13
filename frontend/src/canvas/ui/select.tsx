import { Popover, ScrollArea } from './overlays';
import { colors } from './palette';
import { Icon, Text, View } from './primitives';
import { useSelectController } from './selectState';
import { selectOptionId, type SelectOption, type SelectProps } from './selectTypes';
import type { UIStyle } from './types';

export type { SelectOption, SelectProps } from './selectTypes';

type SelectController = ReturnType<typeof useSelectController>;

function triggerStyle(disabled: boolean): UIStyle {
  const style: UIStyle = {
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'space-between',
    gap: 12,
    minHeight: 44,
    padding: 12,
    borderWidth: 1,
    borderColor: colors.border,
    borderRadius: 12,
    backgroundColor: colors.surface,
    hoverBackground: colors.hover,
  };
  if (disabled) style.opacity = 0.5;
  return style;
}

function SelectTrigger({ props, state }: { props: SelectProps; state: SelectController }) {
  const { id, label, title, value, options, disabled = false } = props;
  const selected = options.find((option) => option.value === value);
  return (
    <View
      id={id}
      title={title}
      label={label ?? title ?? selected?.label}
      disabled={disabled}
      focusable={!disabled}
      expanded={state.open}
      controls={`${id}-popup`}
      onClick={disabled ? undefined : state.toggle}
      onKeyInput={state.onKeyInput}
      style={triggerStyle(disabled)}
    >
      <Text style={{ minWidth: 0, flexGrow: 1, maxLines: 1, color: selected ? colors.text : colors.muted }}>
        {selected?.label ?? 'Select…'}
      </Text>
      <Icon name={state.open ? 'chevron-up' : 'chevron-down'} style={{ width: 14, height: 14, color: colors.muted }} />
    </View>
  );
}

function optionStyle(option: SelectOption, active: SelectController['active']): UIStyle {
  const style: UIStyle = {
    minHeight: 42,
    padding: 10,
    gap: 10,
    borderRadius: 12,
    flexDirection: 'row',
    alignItems: 'center',
    // Rows sit on overlay glass: translucent fills only, never an opaque surface.
    hoverBackground: colors.glassHover,
    backgroundColor: active === option.value ? colors.glassFill : undefined,
  };
  if (option.disabled) style.opacity = 0.45;
  return style;
}

interface OptionRowProps {
  id: string;
  value: string;
  option: SelectOption;
  state: SelectController;
}

function SelectOptionRow({ id, value, option, state }: OptionRowProps) {
  const selected = option.value === value;
  return (
    <View
      id={selectOptionId(id, option.value)}
      label={option.label}
      disabled={option.disabled}
      focusable={!option.disabled}
      pressed={selected}
      onClick={option.disabled ? undefined : () => state.pick(option.value)}
      onKeyInput={option.disabled ? undefined : state.onKeyInput}
      style={optionStyle(option, state.active)}
    >
      <View style={{ width: 18, height: 18, flexShrink: 0 }}>
        {selected && <Icon name="check" style={{ width: 18, height: 18, color: colors.accent }} />}
      </View>
      <Text style={{ minWidth: 0, flexGrow: 1, color: selected ? colors.accent : colors.text }}>{option.label}</Text>
    </View>
  );
}

export function Select(props: SelectProps) {
  const { id, label, title, value, options, style } = props;
  const state = useSelectController(props);
  return (
    <View id={`${id}-group`} style={{ minWidth: 0, gap: 6, ...style }}>
      {label && (
        <Text id={`${id}-label`} style={{ fontSize: 13, fontWeight: 600, color: colors.text }}>
          {label}
        </Text>
      )}
      <SelectTrigger props={props} state={state} />
      <Popover id={`${id}-popup`} title={title ?? label} open={state.open} onClose={state.dismiss} anchorId={id}>
        <ScrollArea id={`${id}-options`} height={Math.min(288, Math.max(44, options.length * 44))}>
          <View style={{ gap: 2 }}>
            {options.length ? (
              options.map((option) => (
                <SelectOptionRow key={option.value} id={id} value={value} option={option} state={state} />
              ))
            ) : (
              <Text style={{ padding: 12, color: colors.muted }}>No options available</Text>
            )}
          </View>
        </ScrollArea>
      </Popover>
    </View>
  );
}
