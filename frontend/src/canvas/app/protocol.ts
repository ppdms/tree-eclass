import type { EditInput, PointerInput, Viewport } from '../protocol';
import type { RichBlock } from '../ui/richTypes';
import type { FocusDescriptor } from '../ui/interaction';
import type { NodeSnapshot, Rect } from '../ui/types';
import type { StorageScope, StorageSnapshot } from './preferences';
import type {
  BrowserCancel,
  BrowserRequest,
  BrowserResult,
  NativeFieldDescriptor,
  NativeHotspot,
} from './browserProtocol';

interface HistoryFields {
  state?: HistoryState;
  key?: string;
  action?: 'PUSH' | 'REPLACE' | 'POP';
}
// Browser history and postMessage enforce structured-clone support. Object values
// stay opaque: dates, maps, buffers, cycles and legacy entry objects are not JSON.
export type HistoryState = object | string | number | boolean | bigint | null | undefined;
interface KeyInput {
  type: 'key';
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
/** Browser-measured, unscrolled rectangles in the native control's border-box coordinates. */
interface NativeEditGeometry {
  caret: Rect | null;
  selection: Rect[];
  composition: Rect[];
  contentX: number;
  contentY: number;
  contentWidth: number;
  contentHeight: number;
}
export interface NativeEditInput extends Omit<EditInput, 'selectionStart' | 'selectionEnd'> {
  scrollTop: number;
  selectionDirection: 'forward' | 'backward' | 'none' | null;
  selectionStart: number | null;
  selectionEnd: number | null;
  revision: number;
  instance: number;
  geometry?: NativeEditGeometry;
}
export interface Boot {
  type: 'boot';
  /** Single Canvas2D presentation surface; WebGL2 navigation is composited offscreen. */
  canvas: OffscreenCanvas;
  viewport: Viewport;
  url: string;
  runtimeSession: string;
  readIds: (string | number)[];
  navigationStart: number;
  storage: StorageSnapshot;
  systemDark: boolean;
  reducedMotion: boolean;
  visible: boolean;
  historyState: HistoryState;
  historyKey: string;
  top: number;
  bottom: number;
}

// Both endpoints are owned, version-matched code. Validate external API/storage/HTML
// at their intake boundaries, not by cloning and parsing every private frame payload.
export type ToApp =
  | Boot
  | { type: 'fonts'; inter: string; mono: string }
  | { type: 'layout'; module: WebAssembly.Module }
  | { type: 'resize'; viewport: Viewport }
  | { type: 'inset'; top: number; bottom: number }
  | (PointerInput & { modified: boolean; button: number; nativeHandled?: boolean })
  | { type: 'wheel'; dx: number; dy: number; x: number; y: number; shift: boolean }
  | NativeEditInput
  | KeyInput
  | { type: 'contextMenu'; x: number; y: number }
  | { type: 'command'; name: 'selectAll' | 'clearSelection' }
  | { type: 'fieldAction'; id: string; instance: number; action: 'focus' | 'blur' | 'submit' }
  | { type: 'fieldMetrics'; items: { id: string; instance: number; scrollHeight: number; clientHeight: number }[] }
  | { type: 'files'; id: string; files: File[] }
  | { type: 'nativeResult'; id: string; error: string | null }
  | { type: 'storage'; scope: StorageScope; key: string; value: string | null }
  | { type: 'activity'; kind: 'activity' | 'visibility' | 'pagehide'; at: number; visible: boolean }
  | { type: 'media'; systemDark: boolean; reducedMotion: boolean }
  | { type: 'runtimeChanged' }
  | BrowserResult
  | { type: 'htmlResult'; id: number; blocks: RichBlock[]; error?: string }
  | ({ type: 'navigate'; url: string } & HistoryFields)
  | { type: 'inspect' }
  | { type: 'readIds'; ids: (string | number)[] };

interface NativeLink extends Rect {
  id?: string;
  href: string;
  internal?: boolean;
  target?: '_blank' | '_self';
  download?: string;
  title?: string;
  rel?: string;
}
export type FromApp =
  | { type: 'error'; detail: string }
  | { type: 'scroll'; offset: number }
  | ({ type: 'navigate'; url: string } & HistoryFields)
  | { type: 'readIds'; ids: (string | number)[] }
  | { type: 'links'; items: NativeLink[] }
  | { type: 'fields'; items: NativeFieldDescriptor[]; hotspots: NativeHotspot[]; actions: NativeHotspot[] }
  | { type: 'focus'; target: FocusDescriptor | null }
  | { type: 'selection'; text: string }
  | BrowserRequest
  | BrowserCancel
  | { type: 'html'; id: number; value: string }
  | { type: 'cursor'; cursor: 'pointer' | 'default' | 'text'; title?: string }
  | { type: 'metrics'; values: Record<string, number>; renderer: string; nodes: NodeSnapshot[] };

export function emit(message: FromApp, transfer?: Transferable[]): void {
  if (transfer) postMessage(message, { transfer });
  else postMessage(message);
}
