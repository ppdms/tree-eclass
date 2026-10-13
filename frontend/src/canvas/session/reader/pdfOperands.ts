import type { PDFOperatorList } from 'pdfjs-dist/types/src/display/api';

/** Values decoded by the pinned PDF.js worker, before interpreting a known operator. */
export interface OperatorArguments extends Array<unknown> {}
export type PdfMatrix = ArrayLike<number>;
export interface PdfGlyph {
  unicode: string;
  width: number;
  vmetric?: PdfMatrix | null;
  isSpace?: boolean;
}
type GraphicsEntry = [string, OperatorArguments[number]];
const emptyArguments: OperatorArguments = [];

function isNumeric(value: OperatorArguments[number]): value is number {
  return typeof value === 'number';
}
function isMatrix(value: OperatorArguments[number]): value is PdfMatrix {
  if (Array.isArray(value)) return value.every(isNumeric);
  return (
    value instanceof Float32Array ||
    value instanceof Float64Array ||
    value instanceof Int8Array ||
    value instanceof Uint8Array ||
    value instanceof Uint8ClampedArray ||
    value instanceof Int16Array ||
    value instanceof Uint16Array ||
    value instanceof Int32Array ||
    value instanceof Uint32Array
  );
}
function isGlyph(value: OperatorArguments[number]): value is PdfGlyph {
  return (
    value !== null &&
    typeof value === 'object' &&
    'unicode' in value &&
    typeof value.unicode === 'string' &&
    'width' in value &&
    isNumeric(value.width) &&
    (!('vmetric' in value) || value.vmetric == null || isMatrix(value.vmetric)) &&
    (!('isSpace' in value) || value.isSpace === undefined || typeof value.isSpace === 'boolean')
  );
}
function isGraphicsEntry(value: OperatorArguments[number]): value is GraphicsEntry {
  return Array.isArray(value) && typeof value[0] === 'string';
}
function isTextOperand(value: OperatorArguments[number]): value is PdfGlyph | number {
  return isNumeric(value) || isGlyph(value);
}
export function operatorArguments(value: PDFOperatorList['argsArray'][number]): OperatorArguments {
  // PDF.js addOp also emits absent arguments for operand-free operators.
  if (value == null) return emptyArguments;
  if (!Array.isArray(value)) throw new Error('Unexpected PDF operator arguments.');
  return value;
}
export function matrixOperand(value: OperatorArguments[number]): PdfMatrix {
  if (!isMatrix(value)) throw new Error('Unexpected PDF matrix operand.');
  return value;
}
export function glyphOperands(value: OperatorArguments[number]): (PdfGlyph | number)[] {
  if (!Array.isArray(value) || !value.every(isTextOperand)) {
    throw new Error('Unexpected PDF glyph operands.');
  }
  return value;
}
export function graphicsEntries(value: OperatorArguments[number]): GraphicsEntry[] {
  if (!Array.isArray(value) || !value.every(isGraphicsEntry)) throw new Error('Unexpected PDF graphics state.');
  return value;
}
export function isTextSpacing(value: PdfGlyph | number): value is number {
  return isNumeric(value);
}
