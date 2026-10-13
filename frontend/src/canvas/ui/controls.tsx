import { Children, createContext, Fragment, isValidElement, useContext, type ReactNode } from 'react';
import { colors } from './palette';
import { Text, View } from './primitives';
import type { UIStyle } from './types';

/** True inside overlay glass: controls there use translucent capsule fills, never opaque surfaces. */
export const OnGlass = createContext(false);

export interface ButtonProps {
  id: string;
  children?: ReactNode;
  onClick?: () => void;
  disabled?: boolean;
  tone?: 'normal' | 'primary' | 'danger';
  style?: UIStyle;
  title?: string;
  label?: string;
  expanded?: boolean;
  controls?: string;
  pressed?: boolean;
  copyText?: string;
  onCopyResult?: (error: string | null) => void;
}

function* flatChildren(children: ReactNode): Generator<ReactNode> {
  for (const child of Children.toArray(children)) {
    if (isValidElement<{ children?: ReactNode }>(child) && child.type === Fragment) {
      yield* flatChildren(child.props.children);
    } else yield child;
  }
}

function isTextChild(child: ReactNode): child is string | number {
  return typeof child === 'string' || typeof child === 'number';
}

function canvasChildren(children: ReactNode): ReactNode {
  const result: ReactNode[] = [];
  const text: string[] = [];
  const flush = () => {
    if (!text.length) return;
    result.push(<Text key={`text:${result.length}`}>{text.join('')}</Text>);
    text.length = 0;
  };
  for (const child of flatChildren(children)) {
    if (isTextChild(child)) text.push(String(child));
    else {
      flush();
      result.push(child);
    }
  }
  flush();
  return result;
}

function childLabel(children: ReactNode): string {
  return Children.toArray(children)
    .map((child) => {
      if (isTextChild(child)) return String(child);
      if (isValidElement<{ children?: ReactNode }>(child)) return childLabel(child.props.children);
      return '';
    })
    .filter(Boolean)
    .join(' ');
}

function buttonStyle(tone: ButtonProps['tone'], pressed: boolean | undefined, glass: boolean): UIStyle {
  const primary = tone === 'primary';
  const edge = glass ? colors.glassEdge : colors.border;
  return {
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'center',
    gap: 8,
    minHeight: 44,
    minWidth: 44,
    paddingTop: 10,
    paddingBottom: 10,
    paddingLeft: 14,
    paddingRight: 14,
    borderWidth: 1,
    borderRadius: 22,
    borderColor: primary || pressed ? colors.accent : tone === 'danger' ? colors.danger : edge,
    backgroundColor: primary ? colors.accent : pressed ? colors.hover : glass ? colors.glassFill : colors.raised,
    hoverBackground: primary ? colors.accent : glass ? colors.glassHover : colors.hover,
    color: primary ? colors.onAccent : tone === 'danger' ? colors.danger : colors.text,
    fontSize: 14,
    fontWeight: 600,
    lineHeight: 1.35,
  };
}

export function Button({
  id,
  children,
  onClick,
  disabled = false,
  tone = 'normal',
  style,
  title,
  label,
  expanded,
  controls,
  pressed,
  copyText,
  onCopyResult,
}: ButtonProps) {
  const resolvedStyle = { ...buttonStyle(tone, pressed, useContext(OnGlass)), ...style };
  if (disabled) resolvedStyle.opacity = 0.5;
  return (
    <View
      id={id}
      disabled={disabled}
      focusable={!disabled}
      title={title}
      label={label ?? childLabel(children)}
      expanded={expanded}
      controls={controls}
      pressed={pressed}
      onClick={
        onClick
          ? () => {
              if (!disabled) onClick();
            }
          : undefined
      }
      nativeAction={
        !disabled && copyText !== undefined ? { kind: 'copy', value: copyText, onResult: onCopyResult } : undefined
      }
      style={resolvedStyle}
    >
      {canvasChildren(children)}
    </View>
  );
}

export interface PanelProps {
  id?: string;
  children?: ReactNode;
  style?: UIStyle;
}

export function Panel({ id, children, style }: PanelProps) {
  return (
    <View
      id={id}
      style={{
        minWidth: 0,
        gap: 12,
        padding: 18,
        borderWidth: 1,
        borderColor: colors.border,
        borderRadius: 18,
        backgroundColor: colors.surface,
        ...style,
      }}
    >
      {canvasChildren(children)}
    </View>
  );
}

export interface NoticeProps {
  id?: string;
  children?: ReactNode;
  tone?: 'info' | 'success' | 'warning' | 'danger';
  style?: UIStyle;
}

export function Notice({ id, children, tone = 'info', style }: NoticeProps) {
  const border = { info: colors.link, success: colors.success, warning: colors.warning, danger: colors.danger }[tone];
  return (
    <View
      id={id}
      style={{
        minWidth: 0,
        padding: 14,
        gap: 8,
        borderWidth: 1,
        borderColor: border,
        borderRadius: 14,
        backgroundColor: colors.surface,
        color: colors.text,
        ...style,
      }}
    >
      {canvasChildren(children)}
    </View>
  );
}

export interface ProgressProps {
  id?: string;
  value: number;
  max?: number;
  label?: string;
  style?: UIStyle;
}

export function Progress({ id, value, max = 100, label, style }: ProgressProps) {
  const ratio = Number.isFinite(max) && max > 0 && Number.isFinite(value) ? Math.max(0, Math.min(1, value / max)) : 0;
  return (
    <View id={id} label={label} style={{ minWidth: 0, gap: 6, ...style }}>
      {label && <Text style={{ fontSize: 12, color: colors.muted }}>{label}</Text>}
      <View
        style={{
          height: 8,
          borderWidth: 1,
          borderColor: colors.border,
          borderRadius: 4,
          backgroundColor: colors.raised,
          overflow: 'hidden',
        }}
      >
        <View style={{ height: 6, width: `${ratio * 100}%`, borderRadius: 3, backgroundColor: colors.accent }} />
      </View>
    </View>
  );
}
