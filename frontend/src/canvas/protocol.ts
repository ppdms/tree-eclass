export interface Viewport {
  width: number;
  height: number;
  dpr: number;
}

export interface PointerInput {
  type: 'pointer';
  phase: 'down' | 'move' | 'up' | 'cancel' | 'hover';
  id: number;
  x: number;
  y: number;
  time: number;
  touch: boolean;
  shift: boolean;
}

/** The overlay input's editing state; the worker draws the field from it every frame. */
export interface EditInput {
  type: 'edit';
  field: string;
  value: string;
  active: boolean;
  selectionStart: number;
  selectionEnd: number;
  /** UTF-16 range of uncommitted IME/dead-key text inside `value`. */
  composition: [number, number] | null;
  scrollLeft: number;
}
