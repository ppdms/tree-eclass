import assert from 'node:assert/strict';
import test from 'node:test';
import type { PDFOperatorList, TextContent } from 'pdfjs-dist/types/src/display/api';
import { caretPoint, hitText, pageGeometry, rangeRects } from './geometry.ts';
import { glyphNormalizeUnicode } from './glyphUnicode.ts';
import { pdfGlyphPositions } from './pdfPositions.ts';
import type { AnnotationRect } from './types.ts';

// Decoded PDF.js operator/text fixtures. Widths are original font-program units,
// not measurements of a replacement UI font. Regressions exercise observable
// glyph ownership, caret boundaries and selection rectangles at UTF-16 offsets.
interface Glyph {
  unicode: string;
  width: number;
  isSpace?: boolean;
}
interface Line {
  text: string;
  glyphs: (Glyph | number)[];
  width: number;
  y?: number;
  dir?: string;
}
const operations = { beginText: 31, setFont: 37, setTextMatrix: 42, showText: 44 };
const viewport = [1, 0, 0, -1, 0, 100];
const fonts = { fixture: { matrix: [0.001, 0, 0, 0.001, 0, 0], vertical: false, type3: false, defaultVMetrics: null } };

function geometry(lines: Line[]) {
  const operators: PDFOperatorList = {
    fnArray: [operations.beginText, operations.setFont],
    argsArray: [[], ['fixture', 10]],
  };
  const content: TextContent = {
    items: [],
    lang: null,
    styles: { fixture: { fontFamily: 'Helvetica', ascent: 0.8, descent: -0.2, vertical: false } },
  };
  for (const line of lines) {
    const y = 100 - (line.y ?? 20);
    operators.fnArray.push(operations.setTextMatrix, operations.showText);
    operators.argsArray.push([new Float32Array([1, 0, 0, 1, 20, y])], [line.glyphs]);
    content.items.push({
      str: line.text,
      dir: line.dir ?? 'ltr',
      transform: [10, 0, 0, 10, 20, y],
      width: line.width,
      height: 10,
      fontName: 'fixture',
      hasEOL: false,
    });
  }
  return pageGeometry(
    content,
    viewport,
    { width: 100, height: 100 },
    pdfGlyphPositions(operators, operations, fonts, viewport),
  );
}

function rectangle(actual: AnnotationRect[], x: number, y: number, width: number) {
  assert.equal(actual.length, 1);
  const expectedRect = { x: x / 100, y: y / 100, w: width / 100, h: 0.1 };
  const fields: (keyof AnnotationRect)[] = ['x', 'y', 'w', 'h'];
  for (const field of fields) {
    const expected = expectedRect[field];
    assert.ok(Math.abs(actual[0]![field] - expected) < 1e-10, `${field}: ${actual[0]![field]} != ${expected}`);
  }
}

test('10-point ba/ab lines with 11-point leading keep each glyph on its own baseline', () => {
  const page = geometry([
    {
      text: 'ba',
      glyphs: [
        { unicode: 'b', width: 556 },
        { unicode: 'a', width: 556 },
      ],
      width: 11.12,
      y: 20,
    },
    {
      text: 'ab',
      glyphs: [
        { unicode: 'a', width: 556 },
        { unicode: 'b', width: 556 },
      ],
      width: 11.12,
      y: 31,
    },
  ]);
  assert.equal(page.text, 'ba ab');
  rectangle(rangeRects(page, 0, 1), 20, 12, 5.56);
  rectangle(rangeRects(page, 1, 2), 25.56, 12, 5.56);
  rectangle(rangeRects(page, 3, 4), 20, 23, 5.56);
  rectangle(rangeRects(page, 4, 5), 25.56, 23, 5.56);
  assert.equal(hitText(page, 30.9, 15), 2);
  assert.equal(hitText(page, 20.1, 26), 3);
});

test('one ToUnicode glyph containing Hebrew alef/bet survives the exact bidi permutation', () => {
  const page = geometry([
    { text: '\u05d1\u05d0', dir: 'rtl', glyphs: [{ unicode: '\u05d0\u05d1', width: 667 }], width: 6.67 },
  ]);
  assert.equal(page.text, '\u05d1\u05d0');
  rectangle(rangeRects(page, 0, 1), 20, 12, 6.67);
  rectangle(rangeRects(page, 1, 2), 20, 12, 6.67);
  assert.equal(page.spans[0]!.glyphs[0]!.start, 0);
  assert.equal(page.spans[0]!.glyphs[0]!.end, 2);
});

test('identical RTL characters map by permutation and original cluster identity, not a string bag', () => {
  const page = geometry([
    {
      text: '\u05d0\u05d0\u05d0',
      dir: 'rtl',
      width: 9.45,
      glyphs: [
        { unicode: '\u05d0\u05d0', width: 667 },
        { unicode: '\u05d0', width: 278 },
      ],
    },
  ]);
  rectangle(rangeRects(page, 0, 1), 26.67, 12, 2.78);
  rectangle(rangeRects(page, 1, 2), 20, 12, 6.67);
  rectangle(rangeRects(page, 2, 3), 20, 12, 6.67);
});

test('mixed-direction clusters retain non-contiguous UTF-16 ownership and embedded LTR carets', () => {
  // Original clusters [alef,a] [b,bet] become [bet,a,b,alef]. Neither a blanket
  // reverse nor a literal match of each cluster can produce these associations.
  const page = geometry([
    {
      text: '\u05d1ab\u05d0',
      dir: 'rtl',
      width: 13.34,
      glyphs: [
        { unicode: '\u05d0a', width: 667 },
        { unicode: 'b\u05d1', width: 667 },
      ],
    },
  ]);
  rectangle(rangeRects(page, 0, 1), 26.67, 12, 6.67);
  rectangle(rangeRects(page, 1, 2), 20, 12, 6.67);
  rectangle(rangeRects(page, 2, 3), 26.67, 12, 6.67);
  rectangle(rangeRects(page, 3, 4), 20, 12, 6.67);
  assert.equal(caretPoint(page, 1)!.x, 20);
  assert.ok(Math.abs(caretPoint(page, 2)!.x - 26.67) < 1e-10);
  assert.equal(hitText(page, 19.9, 15), 4, 'Dragging beyond the RTL line includes its final logical unit');
  assert.equal(hitText(page, 33.5, 15), 0, 'The other outer edge remains the logical start');
});

test('Greek and Latin source strings preserve exact UTF-16 offsets and original advances', () => {
  const greek = 'Ελληνικά';
  const page = geometry([
    { text: greek, glyphs: [...greek].map((unicode) => ({ unicode, width: 600 })), width: 48 },
    {
      text: 'aba',
      glyphs: [
        { unicode: 'a', width: 556 },
        { unicode: 'b', width: 556 },
        { unicode: 'a', width: 556 },
      ],
      width: 16.68,
      y: 31,
    },
  ]);
  assert.equal(page.text, greek + ' aba');
  rectangle(rangeRects(page, 2, 3), 32, 12, 6);
  rectangle(rangeRects(page, 9, 10), 20, 23, 5.56);
  rectangle(rangeRects(page, 11, 12), 31.12, 23, 5.56);
});

test('Latin ligature normalization maps every expanded UTF-16 unit to the same original glyph', () => {
  const page = geometry([
    {
      text: 'office',
      width: 22.79,
      glyphs: [
        { unicode: 'o', width: 556 },
        { unicode: '\ufb03', width: 667 },
        { unicode: 'c', width: 500 },
        { unicode: 'e', width: 556 },
      ],
    },
  ]);
  assert.equal(page.text, 'office');
  for (let offset = 1; offset < 4; offset++) rectangle(rangeRects(page, offset, offset + 1), 25.56, 12, 6.67);
  rectangle(rangeRects(page, 4, 5), 32.23, 12, 5);
});

test('RTL presentation-form normalization happens before bidi without splitting its source cluster', () => {
  const page = geometry([
    { text: '\u05dc\u05d0', dir: 'rtl', width: 6.67, glyphs: [{ unicode: '\ufb4f', width: 667 }] },
  ]);
  rectangle(rangeRects(page, 0, 1), 20, 12, 6.67);
  rectangle(rangeRects(page, 1, 2), 20, 12, 6.67);
});

test('ordinary combining marks are not blanket-normalized and astral carets never split surrogates', () => {
  const page = geometry([
    {
      text: 'A😀e\u0301',
      width: 13.34,
      glyphs: [
        { unicode: 'A', width: 667 },
        { unicode: '😀', width: 667 },
        { unicode: 'e\u0301', width: 556 },
      ],
    },
  ]);
  assert.equal(page.text, 'A😀e\u0301');
  rectangle(rangeRects(page, 1, 2), 26.67, 12, 6.67);
  rectangle(rangeRects(page, 2, 3), 26.67, 12, 6.67);
  rectangle(rangeRects(page, 3, 4), 33.34, 12, 5.56);
  rectangle(rangeRects(page, 4, 5), 33.34, 12, 5.56);
  assert.notEqual(hitText(page, 30, 15), 2);
});

test('collapsed original whitespace and synthesized TJ gaps use their exact PDF extents', () => {
  const original = geometry([
    {
      text: 'a b',
      width: 13.9,
      glyphs: [
        { unicode: 'a', width: 556 },
        { unicode: '\u00a0', width: 278, isSpace: true },
        { unicode: 'b', width: 556 },
      ],
    },
  ]);
  const tj = geometry([
    { text: 'a b', width: 13.9, glyphs: [{ unicode: 'a', width: 556 }, -278, { unicode: 'b', width: 556 }] },
  ]);
  for (const page of [original, tj]) {
    assert.equal(page.text, 'a b');
    rectangle(rangeRects(page, 0, 1), 20, 12, 5.56);
    rectangle(rangeRects(page, 1, 2), 25.56, 12, 2.78);
    rectangle(rangeRects(page, 2, 3), 28.34, 12, 5.56);
  }
});

test('mixed RTL/LTR TJ whitespace is included in bidi before offset ownership is assigned', () => {
  const page = geometry([
    {
      text: 'A \u05d1\u05d0',
      dir: 'rtl',
      width: 16.12,
      glyphs: [{ unicode: '\u05d0\u05d1', width: 667 }, -278, { unicode: 'A', width: 667 }],
    },
  ]);
  assert.equal(page.text, 'A \u05d1\u05d0');
  rectangle(rangeRects(page, 0, 1), 29.45, 12, 6.67);
  rectangle(rangeRects(page, 1, 2), 26.67, 12, 2.78);
  rectangle(rangeRects(page, 2, 3), 20, 12, 6.67);
});

test('whitespace inside a multi-codepoint ToUnicode cluster remains owned by that original glyph', () => {
  const page = geometry([{ text: 'a b', width: 6.67, glyphs: [{ unicode: 'a\u00a0b', width: 667 }] }]);
  for (let offset = 0; offset < 3; offset++) rectangle(rangeRects(page, offset, offset + 1), 20, 12, 6.67);
});

test('zero-advance combining clusters retain their real raster advances before following glyphs', () => {
  const page = geometry([
    {
      text: 'e\u0301a',
      width: 5.56,
      glyphs: [
        { unicode: 'e\u0301', width: 556 },
        { unicode: 'a', width: 556 },
      ],
    },
  ]);
  assert.equal(page.text, 'e\u0301a');
  rectangle(rangeRects(page, 0, 1), 20, 12, 5.56);
  rectangle(rangeRects(page, 1, 2), 20, 12, 5.56);
  rectangle(rangeRects(page, 2, 3), 25.56, 12, 5.56);
});

test('ignored ToUnicode format clusters do not move the extracted baseline or steal the visible glyph', () => {
  const page = geometry([
    {
      text: 'b',
      width: 5.56,
      glyphs: [
        { unicode: 'a\u200e', width: 667 },
        { unicode: 'b', width: 556 },
      ],
    },
  ]);
  assert.equal(page.text, 'b');
  rectangle(rangeRects(page, 0, 1), 26.67, 12, 5.56);
});

test('normalization unions retain shared logical-interval extents and outer caret boundaries', () => {
  const text = glyphNormalizeUnicode('\ufb05\ufb05');
  const page = geometry([
    {
      text,
      width: 13.34,
      glyphs: [
        { unicode: '\ufb05', width: 667 },
        { unicode: '\ufb05', width: 667 },
      ],
    },
  ]);
  rectangle(rangeRects(page, 0, 1), 20, 12, 13.34);
  rectangle(rangeRects(page, text.length - 1, text.length), 20, 12, 13.34);
  assert.equal(caretPoint(page, 0)!.x, 20, 'The start handle bounds both source glyphs, not only the last owner');
  assert.equal(caretPoint(page, text.length - 1)!.x, 20, 'Every expanded unit shares the normalized cluster start');
  assert.ok(Math.abs(caretPoint(page, text.length)!.x - 33.34) < 1e-10);
  assert.equal(hitText(page, 19.9, 15), 0);
  assert.equal(hitText(page, 33.5, 15), text.length);
});

test('pinned bidi removal of angle brackets does not shift surviving repeated-character ownership', () => {
  const page = geometry([
    {
      text: '\u05d0\u05d0',
      dir: 'rtl',
      width: 15.83,
      glyphs: [
        { unicode: '\u05d0', width: 278 },
        { unicode: '<', width: 584 },
        { unicode: '\u05d0', width: 667 },
        { unicode: '>', width: 54 },
      ],
    },
  ]);
  rectangle(rangeRects(page, 0, 1), 28.62, 12, 6.67);
  rectangle(rangeRects(page, 1, 2), 20, 12, 2.78);
});

test('unmatched non-whitespace source text is explicit instead of silently dropped', () => {
  assert.throws(
    () =>
      geometry([
        {
          text: 'ab',
          width: 11.12,
          glyphs: [
            { unicode: 'a', width: 556 },
            { unicode: 'c', width: 556 },
          ],
        },
      ]),
    /PDF text geometry mismatch/,
  );
});
