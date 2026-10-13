import {
  AlignItems,
  AlignSelf,
  Display,
  FlexDirection,
  FlexWrap,
  JustifyContent,
  Position,
  Style,
  type Dimension,
  type StylePropertyValues,
  type TrackSizingFunction,
} from 'taffy-layout';
import type { Length, Track, UIStyle } from './types';
import { color, colors } from './palette';

const align = { start: AlignItems.Start, end: AlignItems.End, center: AlignItems.Center, stretch: AlignItems.Stretch };
const selfAlign = { start: AlignSelf.Start, end: AlignSelf.End, center: AlignSelf.Center, stretch: AlignSelf.Stretch };
const justify = {
  start: JustifyContent.Start,
  end: JustifyContent.End,
  center: JustifyContent.Center,
  'space-between': JustifyContent.SpaceBetween,
};

function dimension(value: Length | undefined): Dimension {
  if (value === undefined) return 'auto';
  if (isRem(value)) return Number.parseFloat(value) * 16;
  return value;
}

function isRem(value: Length): value is `${number}rem` {
  return typeof value === 'string' && value.endsWith('rem');
}

function track(value: Track): TrackSizingFunction {
  if (value === 'auto') return { min: 'auto', max: 'auto' };
  return { min: 0, max: value };
}

export function resolveStyle(style: UIStyle | undefined, parent: UIStyle, width: number): UIStyle {
  const resolved: UIStyle = {
    color: parent.color ?? colors.text,
    fontSize: parent.fontSize ?? 14,
    fontWeight: parent.fontWeight ?? 400,
    lineHeight: parent.lineHeight ?? 1.35,
    mono: parent.mono ?? false,
    ...style,
    ...(width <= 640 ? style?.mobile : undefined),
    ...(width >= 1024 ? style?.desktop : undefined),
  };
  for (const key of ['color', 'backgroundColor', 'borderColor', 'hoverBackground', 'hoverColor'] as const) {
    const value = resolved[key];
    if (value) resolved[key] = color(value);
  }
  return resolved;
}

function spacing(s: UIStyle): StylePropertyValues {
  return {
    padding: {
      left: s.paddingLeft ?? s.padding ?? 0,
      right: s.paddingRight ?? s.padding ?? 0,
      top: s.paddingTop ?? s.padding ?? 0,
      bottom: s.paddingBottom ?? s.padding ?? 0,
    },
    margin: {
      left: s.marginLeft ?? s.margin ?? 0,
      right: s.marginRight ?? s.margin ?? 0,
      top: s.marginTop ?? s.margin ?? 0,
      bottom: s.marginBottom ?? s.margin ?? 0,
    },
    border: {
      left: s.borderWidth ?? 0,
      right: s.borderWidth ?? 0,
      top: s.borderWidth ?? 0,
      bottom: s.borderBottomWidth ?? s.borderWidth ?? 0,
    },
    gap: { width: s.columnGap ?? s.gap ?? 0, height: s.rowGap ?? s.gap ?? 0 },
  };
}

/** Each Style is copied into the tree; release this temporary WASM allocation immediately. */
export function taffyStyle(s: UIStyle, fixed: boolean, hidden: boolean): Style {
  const props: StylePropertyValues = {
    display: hidden || s.display === 'none' ? Display.None : s.display === 'grid' ? Display.Grid : Display.Flex,
    flexDirection: s.flexDirection === 'row' ? FlexDirection.Row : FlexDirection.Column,
    flexWrap: s.flexWrap === 'wrap' ? FlexWrap.Wrap : FlexWrap.NoWrap,
    flexGrow: s.flexGrow ?? 0,
    flexShrink: s.flexShrink ?? 0,
    alignItems: s.alignItems ? align[s.alignItems] : AlignItems.Stretch,
    alignSelf: s.alignSelf ? selfAlign[s.alignSelf] : AlignSelf.Auto,
    justifyContent: s.justifyContent ? justify[s.justifyContent] : JustifyContent.Start,
    width: dimension(s.width),
    height: dimension(s.height),
    minWidth: dimension(s.minWidth ?? 0),
    minHeight: dimension(s.minHeight),
    maxWidth: dimension(s.maxWidth),
    maxHeight: dimension(s.maxHeight),
    gridTemplateColumns: s.gridTemplateColumns?.map(track) ?? [],
    gridColumn: { start: s.gridColumnStart ?? 'auto', end: { span: s.gridColumnSpan ?? 1 } },
    gridRow: { start: s.gridRowStart ?? 'auto', end: { span: s.gridRowSpan ?? 1 } },
    position: fixed || s.position === 'absolute' ? Position.Absolute : Position.Relative,
    inset: { top: s.top ?? 'auto', left: s.left ?? 'auto', right: s.right ?? 'auto', bottom: s.bottom ?? 'auto' },
    ...spacing(s),
  };
  return new Style(props);
}
