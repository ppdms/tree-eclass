import { useCallback, useState, type ReactNode } from 'react';
import { useViewport } from '../app/viewport';
import { OnGlass } from './controls';
import { Text, View } from './primitives';
import { colors } from './palette';
import { useNodeRect } from './layoutEvents';
import type { CanvasKey, Length, Rect, ScrollOptions, UIStyle } from './types';

export interface ScrollAreaProps extends ScrollOptions {
  id: string;
  height: Length;
  maxHeight?: Length;
  children?: ReactNode;
  style?: UIStyle;
}

export function ScrollArea({ id, height, maxHeight, children, style, ...scroll }: ScrollAreaProps) {
  return (
    <View
      id={id}
      scroll={scroll}
      style={{ minWidth: 0, minHeight: 0, height, maxHeight, overflow: 'hidden', ...style }}
    >
      {children}
    </View>
  );
}

interface ScopeProps {
  id: string;
  open: boolean;
  onClose: () => void;
  children?: ReactNode;
}

function escape(onClose: () => void, dismissible = true) {
  return (event: CanvasKey) => {
    if (event.key !== 'Escape' || event.composing || !dismissible) return;
    onClose();
    return true;
  };
}

export function FocusScope({ id, open, onClose, children }: ScopeProps) {
  return open ? (
    <View id={id} focusScope="region" onKeyInput={escape(onClose)} style={{ minWidth: 0, minHeight: 0 }}>
      {children}
    </View>
  ) : null;
}

export interface DialogProps extends ScopeProps {
  title: string;
  actions?: ReactNode;
  dismissible?: boolean;
}

function dialogSurfaceStyle(height: number): UIStyle {
  return {
    width: '100%',
    maxWidth: 720,
    maxHeight: Math.max(1, height - 32),
    minHeight: 0,
    padding: 20,
    gap: 16,
    borderRadius: 22,
    borderWidth: 1,
    borderColor: colors.border,
    backgroundColor: colors.surface,
    glass: 'overlay',
  };
}

export function Dialog({ id, title, open, onClose, children, actions, dismissible = true }: DialogProps) {
  const viewport = useViewport();
  if (!open) return null;
  const height = Math.max(1, viewport.bottom - viewport.top);
  return (
    <View
      id={`${id}-backdrop`}
      fixed
      modal
      focusScope="dialog"
      onClick={dismissible ? onClose : undefined}
      blockPointer
      onKeyInput={escape(onClose, dismissible)}
      style={{
        left: 0,
        top: viewport.top,
        width: viewport.width,
        height,
        alignItems: 'center',
        justifyContent: 'center',
        backgroundColor: '#00000080',
        padding: 16,
      }}
    >
      <View id={id} blockPointer style={dialogSurfaceStyle(height)}>
        <OnGlass.Provider value>
          <Text id={`${id}-title`} style={{ fontSize: 21, fontWeight: 650 }}>
            {title}
          </Text>
          <View
            id={`${id}-body`}
            scroll={{ axis: 'vertical' }}
            style={{ minHeight: 0, minWidth: 0, flexShrink: 1, overflow: 'hidden', gap: 12 }}
          >
            {children}
          </View>
          {actions && (
            <View style={{ flexDirection: 'row', flexWrap: 'wrap', justifyContent: 'end', gap: 8 }}>{actions}</View>
          )}
        </OnGlass.Provider>
      </View>
    </View>
  );
}

export interface PopoverProps extends ScopeProps {
  title?: string;
  anchorId?: string;
  virtualAnchor?: Rect;
  role?: 'menu' | 'dialog';
  modal?: boolean;
}

function usePopoverPlacement(anchorId: string | undefined, virtualAnchor: Rect | undefined) {
  const viewport = useViewport();
  const measuredAnchor = useNodeRect(anchorId ?? '');
  const anchor = virtualAnchor ?? measuredAnchor;
  const [size, setSize] = useState({ width: 0, height: 0 });
  const measured = useCallback((rect: Rect) => {
    setSize((previous) =>
      previous.width === rect.width && previous.height === rect.height
        ? previous
        : { width: rect.width, height: rect.height },
    );
  }, []);
  const width = Math.min(Math.max(anchor?.width ?? 0, 320), 480, Math.max(1, viewport.width - 24));
  const left = Math.max(12, Math.min(anchor?.x ?? 12, viewport.width - width - 12));
  const below = (anchor?.y ?? viewport.top) + (anchor?.height ?? 0) + 8;
  const top =
    below + size.height <= viewport.bottom - 12
      ? below
      : Math.max(viewport.top + 12, (anchor?.y ?? below) - size.height - 8);
  return { viewport, width, left, top, measured };
}

function popoverSurfaceStyle(height: number, modal: boolean, left: number, top: number, width: number): UIStyle {
  return {
    position: modal ? 'absolute' : 'relative',
    left: modal ? left : undefined,
    top: modal ? top : undefined,
    width,
    maxHeight: Math.max(1, height - 24),
    minHeight: 0,
    padding: 16,
    gap: 12,
    borderRadius: 18,
    borderWidth: 1,
    borderColor: colors.border,
    backgroundColor: colors.surface,
    glass: 'overlay',
  };
}

function OpenPopover({
  id,
  title,
  anchorId,
  virtualAnchor,
  onClose,
  children,
  role = 'dialog',
  modal = true,
}: PopoverProps) {
  const { viewport, width, left, top, measured } = usePopoverPlacement(anchorId, virtualAnchor);
  return (
    <View
      id={`${id}-backdrop`}
      fixed
      modal={modal}
      blockPointer
      focusScope={modal ? role : undefined}
      onClick={modal ? onClose : undefined}
      onOutsidePointer={modal ? undefined : onClose}
      onKeyInput={escape(onClose)}
      style={modal ? { left: 0, top: 0, width: viewport.width, height: viewport.height } : { left, top, width }}
    >
      <View
        id={id}
        blockPointer
        morphFrom={anchorId}
        onLayout={measured}
        style={popoverSurfaceStyle(viewport.bottom - viewport.top, modal, left, top, width)}
      >
        <OnGlass.Provider value>
          {title && <Text style={{ fontSize: 16, fontWeight: 650 }}>{title}</Text>}
          <View
            id={`${id}-body`}
            scroll={{ axis: 'vertical' }}
            style={{ minWidth: 0, minHeight: 0, flexShrink: 1, gap: 8, overflow: 'hidden' }}
          >
            {children}
          </View>
        </OnGlass.Provider>
      </View>
    </View>
  );
}

export function Popover(props: PopoverProps) {
  return props.open ? <OpenPopover {...props} /> : null;
}
