import type { PDFOperatorList } from 'pdfjs-dist/types/src/display/api';
import type { PdfFontMetrics } from '../../pdf';
import { glyphNormalizeUnicode } from './glyphUnicode';
import { graphicsEntries, glyphOperands, isTextSpacing, matrixOperand, operatorArguments } from './pdfOperands';
import type { OperatorArguments, PdfGlyph, PdfMatrix } from './pdfOperands';
export type Operations = Record<string, number>;

export interface PdfGlyphPosition {
  text: string;
  font: string;
  x: number;
  y: number;
  endX: number;
  endY: number;
  used: boolean;
  /** Original ToUnicode cluster before PDF.js normalization and bidi. */
  unicode?: string;
  /** Text-run origin/end, before vertical-font bearing offsets and character spacing. */
  textX?: number;
  textY?: number;
  textEndX?: number;
  textEndY?: number;
  spaceScale?: number;
}
interface TextState {
  ctm: number[];
  matrix: PdfMatrix;
  font: string;
  size: number;
  direction: number;
  hScale: number;
  charSpacing: number;
  wordSpacing: number;
  rise: number;
  leading: number;
  x: number;
  y: number;
  lineX: number;
  lineY: number;
  textX: number;
  textY: number;
  textLineX: number;
  textLineY: number;
}
const identity = [1, 0, 0, 1, 0, 0];

function transform(a: PdfMatrix, b: PdfMatrix): number[] {
  return [
    a[0]! * b[0]! + a[2]! * b[1]!,
    a[1]! * b[0]! + a[3]! * b[1]!,
    a[0]! * b[2]! + a[2]! * b[3]!,
    a[1]! * b[2]! + a[3]! * b[3]!,
    a[0]! * b[4]! + a[2]! * b[5]! + a[4]!,
    a[1]! * b[4]! + a[3]! * b[5]! + a[5]!,
  ];
}

function point(matrix: number[], x: number, y: number): [number, number] {
  return [matrix[0]! * x + matrix[2]! * y + matrix[4]!, matrix[1]! * x + matrix[3]! * y + matrix[5]!];
}

function positionGlyph(
  state: TextState,
  glyph: PdfGlyph,
  advance: number,
  font: PdfFontMetrics,
  matrix: number[],
  width: number,
  textAdvance: number,
  textWidth: number,
  spaceScale: number,
): PdfGlyphPosition {
  const scale = state.size * font.matrix[0]!;
  const metrics = glyph.vmetric ?? font.defaultVMetrics;
  const hScale = state.hScale * state.direction;
  const vx = font.vertical ? -(glyph.vmetric ? metrics![1]! : glyph.width * 0.5) * scale : advance;
  const vy = font.vertical ? advance + (metrics?.[2] ?? 0) * scale : 0;
  const start = point(matrix, state.x + vx * hScale, state.y + state.rise - vy * state.direction);
  const end = font.vertical
    ? point(matrix, state.x + vx * hScale, state.y + state.rise - (vy + width) * state.direction)
    : point(matrix, state.x + (vx + width) * hScale, state.y + state.rise);
  const textStart = font.vertical
    ? point(matrix, state.textX, state.textY + state.rise - textAdvance * state.direction)
    : point(matrix, state.textX + textAdvance * hScale, state.textY + state.rise);
  const textEnd = font.vertical
    ? point(matrix, state.textX, state.textY + state.rise - (textAdvance + textWidth) * state.direction)
    : point(matrix, state.textX + (textAdvance + textWidth) * hScale, state.textY + state.rise);
  return {
    text: glyphNormalizeUnicode(glyph.unicode),
    unicode: glyph.unicode,
    font: state.font,
    x: start[0],
    y: start[1],
    endX: end[0],
    endY: end[1],
    used: false,
    textX: textStart[0],
    textY: textStart[1],
    textEndX: textEnd[0],
    textEndY: textEnd[1],
    spaceScale,
  };
}

function showText(
  state: TextState,
  values: (PdfGlyph | number)[],
  fonts: Record<string, PdfFontMetrics>,
  viewport: number[],
  positions: PdfGlyphPosition[],
): void {
  const font = fonts[state.font];
  if (!font || !state.size) return;
  const matrix = transform(transform(viewport, state.ctm), state.matrix);
  const scale = state.size * font.matrix[0]!;
  // Same font-size/textAdvanceScale units used by the pinned evaluator's space factors.
  const spaceScale =
    state.size *
    Math.hypot(viewport[0]!, viewport[1]!) *
    Math.hypot(state.ctm[0]!, state.ctm[1]!) *
    Math.hypot(state.matrix[0]!, state.matrix[1]!);
  let advance = 0;
  let textAdvance = 0;
  for (const glyph of values) {
    if (isTextSpacing(glyph)) {
      const spacing = ((font.vertical ? 1 : -1) * glyph * state.size) / 1000;
      advance += spacing;
      textAdvance += spacing;
      continue;
    }
    const width = font.vertical ? -((glyph.vmetric ?? font.defaultVMetrics)?.[0] ?? -glyph.width) : glyph.width;
    const glyphWidth = width * scale + (font.type3 ? font.matrix[4]! * state.size : 0);
    // Keep the evaluator pen separate from the renderer pen. The pinned
    // evaluator ignores format marks and assigns Mn clusters zero advance;
    // CanvasGraphics still paints/advances their actual font-program glyph.
    const category = glyph.unicode.match(/^(\s)|(\p{Mn})|(\p{Cf})$/u);
    const textWidth = category?.[2] || category?.[3] ? 0 : width * scale;
    positions.push(positionGlyph(state, glyph, advance, font, matrix, glyphWidth, textAdvance, textWidth, spaceScale));
    const spacing = state.charSpacing + (glyph.isSpace ? state.wordSpacing : 0);
    advance += glyphWidth + (font.vertical ? -1 : 1) * spacing * state.direction;
    if (!category?.[3]) textAdvance += textWidth + (font.vertical ? -1 : 1) * spacing * state.direction;
  }
  if (font.vertical) {
    state.y -= advance;
    state.textY -= textAdvance;
  } else {
    state.x += advance * state.hScale * state.direction;
    state.textX += textAdvance * state.hScale * state.direction;
  }
}

function moveText(state: TextState, x: number, y: number): void {
  state.x = state.lineX += x;
  state.y = state.lineY += y;
  state.textX = state.textLineX += x;
  state.textY = state.textLineY += y;
}

function textOperation(state: TextState, op: number, args: OperatorArguments, ops: Operations): void {
  if (op === ops.setFont) {
    state.font = String(args[0]);
    state.size = Math.abs(Number(args[1]));
    state.direction = Number(args[1]) < 0 ? -1 : 1;
  } else if (op === ops.setCharSpacing) state.charSpacing = Number(args[0]);
  else if (op === ops.setWordSpacing) state.wordSpacing = Number(args[0]);
  else if (op === ops.setHScale) state.hScale = Number(args[0]) / 100;
  else if (op === ops.setLeading) state.leading = -Number(args[0]);
  else if (op === ops.setTextRise) state.rise = Number(args[0]);
  else if (op === ops.moveText) moveText(state, Number(args[0]), Number(args[1]));
  else if (op === ops.setLeadingMoveText) {
    state.leading = Number(args[1]);
    moveText(state, Number(args[0]), Number(args[1]));
  } else if (op === ops.nextLine) moveText(state, 0, state.leading);
  else if (op === ops.setTextMatrix || op === ops.beginText) {
    state.matrix = op === ops.beginText ? identity : matrixOperand(args[0]);
    state.x = state.y = state.lineX = state.lineY = 0;
    state.textX = state.textY = state.textLineX = state.textLineY = 0;
  }
}

function graphicsOperation(state: TextState, op: number, args: unknown[], ops: Operations): void {
  if (op === ops.transform) state.ctm = transform(state.ctm, matrixOperand(args));
  if (op === ops.paintFormXObjectBegin && args[0]) state.ctm = transform(state.ctm, matrixOperand(args[0]));
  if (op === ops.setGState) {
    for (const [name, value] of graphicsEntries(args[0])) {
      if (name === 'Font') textOperation(state, ops.setFont!, operatorArguments(value), ops);
    }
  }
}

/** Interprets the same original glyph advances, matrices, spacing and rises used by CanvasGraphics. */
export function pdfGlyphPositions(
  list: PDFOperatorList,
  ops: Operations,
  fonts: Record<string, PdfFontMetrics>,
  viewport: number[],
): PdfGlyphPosition[] {
  const positions: PdfGlyphPosition[] = [];
  const stack: TextState[] = [];
  let state: TextState = {
    ctm: identity,
    matrix: identity,
    font: '',
    size: 1,
    direction: 1,
    hScale: 1,
    charSpacing: 0,
    wordSpacing: 0,
    rise: 0,
    leading: 0,
    x: 0,
    y: 0,
    lineX: 0,
    lineY: 0,
    textX: 0,
    textY: 0,
    textLineX: 0,
    textLineY: 0,
  };
  for (let i = 0; i < list.fnArray.length; i++) {
    const op = list.fnArray[i]!;
    const args = operatorArguments(list.argsArray[i]);
    if (op === ops.save || op === ops.paintFormXObjectBegin) stack.push({ ...state });
    if (op === ops.restore || op === ops.paintFormXObjectEnd) state = stack.pop() ?? state;
    else if (op === ops.showText) {
      showText(state, glyphOperands(args[0]), fonts, viewport, positions);
    } else {
      graphicsOperation(state, op, args, ops);
      textOperation(state, op, args, ops);
    }
  }
  return positions;
}
