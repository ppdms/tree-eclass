import type { Rect } from '../ui/types';
import type { StorageScope } from './preferences';

/** Private structured-clone messages between two modules from the same immutable build. */
export interface NativeFieldDescriptor extends Rect {
  id: string;
  instance: number;
  value: string;
  editRevision?: number;
  placeholder: string;
  fontSize: number;
  inset: number;
  paddingY?: number;
  paddingRight?: number;
  paddingBottom?: number;
  textAlign?: 'left' | 'center' | 'right';
  lineHeight?: number;
  fontWeight?: number;
  mono?: boolean;
  type?: 'text' | 'password' | 'email' | 'url' | 'search' | 'number' | 'date' | 'time' | 'datetime-local';
  multiline?: boolean;
  rows?: number;
  sensitive?: boolean;
  readOnly?: boolean;
  disabled?: boolean;
  required?: boolean;
  min?: number | string;
  max?: number | string;
  step?: number | string;
  maxLength?: number;
  autoComplete?: string;
  submitOnEnter?: boolean;
  autoResize?: boolean;
  maxHeight?: number;
}

export interface NativeHotspot extends Rect {
  id: string;
  kind: 'text' | 'file' | 'copy' | 'focus-field';
  fieldId?: string;
  disabled?: boolean;
  accept?: string;
  multiple?: boolean;
  resetKey?: string | number;
  value?: string;
}

export interface NativeValidation {
  valid: boolean;
  invalid: { id: string; message: string }[];
}

export type BrowserOperation =
  | { kind: 'downloadBlob'; blob: Blob; filename: string }
  | { kind: 'copyText'; value: string }
  | { kind: 'decodeImage'; source: string | Blob; width?: number; height?: number }
  | { kind: 'reloadPage' }
  | { kind: 'setUnloadGuard'; id: string; enabled: boolean }
  | { kind: 'mirrorSelection'; text: string }
  | { kind: 'validateNativeFields'; ids: string[] }
  | { kind: 'writeStorage'; scope: StorageScope; key: string; value: string | null };

export interface BrowserRequest {
  type: 'browserRequest';
  id: number;
  operation: BrowserOperation;
}
export interface BrowserCancel {
  type: 'browserCancel';
  id: number;
}
export type BrowserValue =
  | { kind: 'void' }
  | { kind: 'image'; bitmap: ImageBitmap }
  | { kind: 'validation'; value: NativeValidation }
  | { kind: 'error'; message: string };
export interface BrowserResult {
  type: 'browserResult';
  id: number;
  result: BrowserValue;
}
