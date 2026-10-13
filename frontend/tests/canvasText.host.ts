import { CanvasEditor } from '../src/canvas/app/editor';
import { loadEditorFonts } from '../src/canvas/app/editorFonts';
import type { NativeFieldDescriptor } from '../src/canvas/app/browserProtocol';
import type { NativeEditInput } from '../src/canvas/app/protocol';

export interface TextAdvance {
  text: string;
  fontSize: number;
  fontWeight: number;
  mono: boolean;
  width: number;
}

function assertFieldAlignment(editor: CanvasEditor, edits: readonly NativeEditInput[], sample: TextAdvance): void {
  const field: NativeFieldDescriptor = {
    id: 'font-alignment',
    instance: 1,
    x: 0,
    y: 0,
    width: 1000,
    height: 80,
    value: sample.text,
    placeholder: '',
    inset: 0,
    fontSize: sample.fontSize,
    fontWeight: sample.fontWeight,
    mono: sample.mono,
  };
  editor.blur();
  editor.update([field], []);
  editor.activateId(field.id);
  const geometry = edits.at(-1)?.geometry;
  if (!geometry?.caret) throw new Error('Native control did not publish its end caret');
  const difference = Math.abs(geometry.caret.x - geometry.contentX - sample.width);
  // WebKit's DOM Range rectangles snap to CSS pixels; text advances retain subpixels.
  if (difference > 1) {
    throw new Error(`${sample.fontSize}px/${sample.fontWeight}/${sample.mono}: caret diverged by ${difference}px`);
  }
}

/** Native editing's end caret must agree with the worker's painted primary-face advance. */
export async function nativeFieldAlignment(samples: readonly TextAdvance[]): Promise<void> {
  const input = document.createElement('input');
  const canvas = document.createElement('canvas');
  canvas.width = 1100;
  document.body.append(input, canvas);
  const edits: NativeEditInput[] = [];
  const editor = new CanvasEditor(
    input,
    (message) => {
      if (message.type === 'edit') edits.push(message);
    },
    loadEditorFonts('/Inter.woff2', '/RobotoMono.woff2'),
    canvas,
  );
  await editor.ready;
  try {
    for (const sample of samples) assertFieldAlignment(editor, edits, sample);
  } finally {
    editor.blur();
    editor.update([], []);
    document.body.replaceChildren();
  }
}
