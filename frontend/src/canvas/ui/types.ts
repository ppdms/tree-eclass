import type { ReactNode } from 'react';
import type { TextRun } from './richTypes';

export type Length = number | `${number}rem` | `${number}%` | 'auto';
export type Track = number | `${number}fr` | 'auto';
export type IconName =
  | 'folder'
  | 'folder-open'
  | 'file'
  | 'file-diff'
  | 'megaphone'
  | 'check'
  | 'mail-open'
  | 'clock'
  | 'chevron-up'
  | 'chevron-down';

/** Deliberately bounded CSS vocabulary, shared by Taffy layout and canvas painting. */
export interface UIStyle {
  display?: 'flex' | 'grid' | 'none';
  flexDirection?: 'row' | 'column';
  flexWrap?: 'wrap' | 'nowrap';
  flexGrow?: number;
  flexShrink?: number;
  alignItems?: 'start' | 'end' | 'center' | 'stretch';
  alignSelf?: 'start' | 'end' | 'center' | 'stretch';
  justifyContent?: 'start' | 'end' | 'center' | 'space-between';
  width?: Length;
  height?: Length;
  minWidth?: Length;
  minHeight?: Length;
  maxWidth?: Length;
  maxHeight?: Length;
  padding?: number;
  paddingTop?: number;
  paddingBottom?: number;
  paddingLeft?: number;
  paddingRight?: number;
  margin?: number | 'auto';
  marginTop?: number;
  marginBottom?: number;
  marginLeft?: number | 'auto';
  marginRight?: number | 'auto';
  gap?: number;
  columnGap?: number;
  rowGap?: number;
  gridTemplateColumns?: Track[];
  gridColumnStart?: number;
  gridColumnSpan?: number;
  gridRowStart?: number;
  gridRowSpan?: number;
  position?: 'relative' | 'absolute';
  top?: number;
  left?: number;
  right?: number;
  bottom?: number;
  /** Ordering among fixed layers; modal scopes always remain above nonmodal layers. */
  zIndex?: number;
  borderWidth?: number;
  borderBottomWidth?: number;
  borderColor?: string;
  borderRadius?: number;
  backgroundColor?: string;
  color?: string;
  fontSize?: number;
  fontWeight?: number;
  /** Unitless font-size multiplier, matching CSS number line-height and SkParagraph. */
  lineHeight?: number;
  mono?: boolean;
  maxLines?: number;
  strike?: boolean;
  textAlign?: 'left' | 'center' | 'right';
  whiteSpace?: 'pre';
  opacity?: number;
  overflow?: 'hidden';
  /**
   * Liquid Glass, for the navigation layer only: floating bars (`bar`) and menus, popovers or dialogs
   * (`overlay`). Content surfaces use solid fills. Background and border tokens remain the e-ink fallback.
   */
  glass?: 'bar' | 'overlay';
  /** Soften content scrolling under this fixed bar (Apple's scroll edge effect). */
  scrollEdge?: boolean;
  hoverBackground?: string;
  hoverColor?: string;
  mobile?: UIStyle;
  desktop?: UIStyle;
}

export interface CanvasKey {
  key: string;
  code: string;
  shift: boolean;
  ctrl: boolean;
  meta: boolean;
  alt: boolean;
  repeat: boolean;
  composing: boolean;
  nativeHandled?: boolean;
}

export interface CanvasNodePointer {
  phase: 'down' | 'move' | 'up' | 'cancel' | 'hover';
  id: number;
  x: number;
  y: number;
  localX: number;
  localY: number;
  time: number;
  touch: boolean;
  shift: boolean;
  modified: boolean;
  button: number;
}

export interface FieldOptions {
  value: string;
  placeholder: string;
  onChange: (value: string) => void;
  type?: 'text' | 'password' | 'email' | 'url' | 'search' | 'number' | 'date' | 'time' | 'datetime-local';
  multiline?: boolean;
  rows?: number;
  sensitive?: boolean;
  readOnly?: boolean;
  required?: boolean;
  min?: number | string;
  max?: number | string;
  step?: number | string;
  maxLength?: number;
  autoComplete?: string;
  submitOnEnter?: boolean;
  autoResize?: boolean;
  maxHeight?: number;
  onMetrics?: (metrics: { scrollHeight: number; clientHeight: number }) => void;
  onBlur?: () => void;
  onSubmit?: () => void;
}

export interface ScrollMetrics {
  offset: number;
  contentHeight: number;
  viewportHeight: number;
  offsetX: number;
  contentWidth: number;
  viewportWidth: number;
}

export interface ScrollOptions {
  axis?: 'vertical' | 'horizontal' | 'both';
  onUserScroll?: () => void;
  onScroll?: (offset: number) => void;
  onScrollMetrics?: (metrics: ScrollMetrics) => void;
}

export type NativeAction =
  | { kind: 'file'; accept?: string; multiple?: boolean; resetKey?: string | number; onFiles: (files: File[]) => void }
  | { kind: 'copy'; value: string; onResult?: (error: string | null) => void }
  | { kind: 'focus-field'; fieldId: string };

export interface HostProps {
  id?: string;
  children?: ReactNode;
  style?: UIStyle;
  ready?: boolean;
  pending?: boolean;
  fixed?: boolean;
  modal?: boolean;
  focusScope?: 'dialog' | 'menu' | 'region';
  blockPointer?: boolean;
  /** Id of the control this overlay glass emerges from when it appears (renderer-owned motion). */
  morphFrom?: string;
  disabled?: boolean;
  focusable?: boolean;
  label?: string;
  title?: string;
  expanded?: boolean;
  controls?: string;
  pressed?: boolean;
  href?: string;
  target?: '_blank' | '_self';
  rel?: string;
  download?: string;
  external?: boolean;
  onClick?: () => void;
  onVisible?: () => void;
  onHoverChange?: (hovered: boolean) => void;
  onFocusWithinChange?: (focused: boolean) => void;
  name?: IconName;
  path?: string;
  viewBox?: { width: number; height: number };
  fill?: string;
  stroke?: string;
  source?: string;
  runs?: TextRun[];
  field?: FieldOptions;
  nativeAction?: NativeAction;
  scroll?: ScrollOptions;
  onLayout?: (rect: Rect) => void;
  onPointer?: (event: CanvasNodePointer) => boolean | void;
  onContextMenu?: (event: { x: number; y: number; localX: number; localY: number }) => void;
  onOutsidePointer?: (event: { targetId?: string }) => void;
  dragId?: string;
  onDragStart?: () => void;
  onDragOver?: (sourceId: string) => void;
  onDragEnd?: (cancelled: boolean) => void;
  onKeyInput?: (event: CanvasKey) => boolean | void;
}

export interface Rect {
  x: number;
  y: number;
  width: number;
  height: number;
}

export interface LinkRect extends Rect {
  href: string;
  internal?: boolean;
  target?: '_blank' | '_self';
  rel?: string;
  download?: string;
  title?: string;
  runIndex?: number;
  start?: number;
  end?: number;
}

export interface NodeSnapshot extends Rect {
  id: string;
  text: string;
  href: string;
}
