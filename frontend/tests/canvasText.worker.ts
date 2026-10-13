// Real Canvas2D glyph checks, paired with native field geometry in canvasText.host.ts.
// Run offline via scripts/canvas/text-smoke.mjs, without mocked font metrics.
import { loadFonts, type Fonts } from '../src/canvas/ui/fonts';
import { MeasuredText } from '../src/canvas/ui/text';
import { graphemeStops, hardLines } from '../src/canvas/ui/textBreak';
import type { Paragraph } from '../src/canvas/ui/paragraph';
import type { TextRun } from '../src/canvas/ui/richTypes';
import type { UIStyle } from '../src/canvas/ui/types';
import type { TextAdvance } from './canvasText.host';

interface Result {
  name: string;
  passed: boolean;
  error?: string;
}
const style: UIStyle = { fontSize: 32, lineHeight: 1.5 };

function expect(condition: boolean, message: string): asserts condition {
  if (!condition) throw new Error(message);
}

function paragraph(fonts: Fonts, text: string, extra: UIStyle = {}, runs?: TextRun[], width = 800): Paragraph {
  const measured = new MeasuredText();
  measured.update(fonts, text, { ...style, ...extra }, runs);
  measured.layout(width);
  expect(measured.paragraph !== null, 'paragraph exists');
  return measured.paragraph;
}

function glyphs(fonts: Fonts, text: string, candidate?: Paragraph): Uint8ClampedArray {
  const canvas = new OffscreenCanvas(1000, 120);
  const context = canvas.getContext('2d');
  expect(context !== null, 'real Canvas2D context exists');
  const face = fonts.face(style);
  if (candidate) candidate.draw(context, 10, 0, '#000000', '#000000');
  else {
    context.font = face.font;
    context.direction = 'ltr';
    context.textAlign = 'left';
    context.textBaseline = 'alphabetic';
    context.fontKerning = 'normal';
    context.fillStyle = '#000000';
    context.fillText(text, 10, face.ascent);
  }
  // Link underlines are intentionally excluded; compare the complete glyph mask above baseline.
  return context.getImageData(0, 0, canvas.width, Math.floor(face.ascent)).data;
}

function equalPixels(actual: Uint8ClampedArray, expected: Uint8ClampedArray, label: string): void {
  let different = 0;
  for (let at = 0; at < expected.length; at++) if (actual[at] !== expected[at]) different++;
  expect(different === 0, `${label}: ${different} differing glyph-mask bytes`);
}

function shaping(fonts: Fonts): void {
  const text = 'AVATAR To Ελληνικά';
  const runs = [...text].map((letter) => ({ text: letter }));
  const rich = paragraph(fonts, text, {}, runs);
  const plain = paragraph(fonts, text);
  expect(
    Math.abs(rich.maxIntrinsicWidth - fonts.width(fonts.face(style), text)) < 0.001,
    'rich run boundaries do not lose kerning',
  );
  equalPixels(glyphs(fonts, text, rich), glyphs(fonts, text), 'same-font rich shaping');
  const width = plain.maxIntrinsicWidth - 0.1;
  rich.layout(width);
  plain.layout(width);
  expect(rich.height === plain.height, 'rich and plain wrapping agrees');
  expect(rich.height === rich.heightAt(width), 'heightAt agrees with display layout');
}

function linkedJoining(fonts: Fonts): void {
  for (const text of ['office AV Ελληνικά', 'سلام عليكم']) {
    const split = text.indexOf(' ') > 0 ? text.indexOf(' ') : 2;
    const runs = [
      { text: text.slice(0, 1) },
      { text: text.slice(1, split), href: '/synthetic' },
      { text: text.slice(split) },
    ];
    const rich = paragraph(fonts, text, {}, runs);
    equalPixels(glyphs(fonts, text, rich), glyphs(fonts, text), `link paint preserves joining: ${text}`);
  }
}

function mixedFaces(fonts: Fonts): void {
  const runs = [{ text: 'Greek ' }, { text: 'Ελληνικά ', bold: true }, { text: 'mono', mono: true, italic: true }];
  const text = runs.map((run) => run.text).join('');
  const value = paragraph(fonts, text, {}, runs);
  const faces = runs.map((run) => fonts.face(style, run));
  const total = runs.reduce((width, run, index) => width + fonts.width(faces[index]!, run.text), 0);
  expect(Math.abs(value.maxIntrinsicWidth - total) < 0.001, 'font-changing rich runs retain their real advances');
  const canvas = new OffscreenCanvas(1000, 120);
  const context = canvas.getContext('2d');
  expect(context !== null, 'real Canvas2D rich reference exists');
  const baseline = Math.max(...faces.map((face) => face.ascent));
  let x = 10;
  for (let index = 0; index < runs.length; index++) {
    context.font = faces[index]!.font;
    context.fillText(runs[index]!.text, x, baseline);
    x += fonts.width(faces[index]!, runs[index]!.text);
  }
  const expected = context.getImageData(0, 0, 1000, Math.floor(baseline)).data;
  context.clearRect(0, 0, 1000, 120);
  value.draw(context, 10, 0, '#000000', '#000000');
  equalPixels(context.getImageData(0, 0, 1000, Math.floor(baseline)).data, expected, 'mixed rich faces');
  value.layout(total / 2);
  expect(value.height === value.heightAt(total / 2), 'mixed-face wrap metrics agree');
}

function mixedBidiFaces(fonts: Fonts): void {
  const text = 'אבג 123';
  const runs = [{ text: 'אבג ', bold: true }, { text: '123' }];
  const value = paragraph(fonts, text, {}, runs);
  const bold = fonts.face(style, runs[0]);
  const normal = fonts.face(style, runs[1]);
  const canvas = new OffscreenCanvas(1000, 120);
  const context = canvas.getContext('2d');
  expect(context !== null, 'real Canvas2D mixed-direction reference exists');
  const baseline = Math.max(bold.ascent, normal.ascent);
  const digits = fonts.width(normal, '123');
  context.textAlign = 'left';
  context.fontKerning = 'normal';
  context.font = normal.font;
  context.direction = 'ltr';
  context.fillText('123', 10, baseline);
  context.font = bold.font;
  context.direction = 'rtl';
  context.fillText('אבג ', 10 + digits, baseline);
  const expected = context.getImageData(0, 0, 1000, Math.floor(baseline)).data;
  context.clearRect(0, 0, 1000, 120);
  value.draw(context, 10, 0, '#000000', '#000000');
  equalPixels(
    context.getImageData(0, 0, 1000, Math.floor(baseline)).data,
    expected,
    'mixed-font RTL fragment retains its trailing whitespace at the visual left edge',
  );
}

function bidi(fonts: Fonts): void {
  const rtl = paragraph(fonts, 'אבג');
  expect(
    rtl.offsetAt(-1, 1) === 3 && rtl.offsetAt(rtl.longestLine + 1, 1) === 0,
    'RTL hit testing follows visual order',
  );
  const first = rtl.rectsForRange(0, 1)[0];
  const last = rtl.rectsForRange(2, 3)[0];
  expect(
    Boolean(first && last && first.x > last.x && first.width > 0 && last.width > 0),
    'RTL selection rectangles have positive widths in visual order',
  );
  const mixed = paragraph(fonts, 'אבג xyz דה');
  expect(mixed.rectsForRange(1, 5).length >= 2, 'mixed-direction logical selection is visually disjoint');
  for (const text of [
    'abc אבג 123 (سلام) xyz',
    'abc אבג 123 xyz',
    'abc (سلام) xyz',
    'abc אבג xyz',
    'A \u2067אבג\u2069 Z',
    'A <אבג> Z',
    'A 𞤀𞤁 Z',
    'A\u0000\uE000\uD800',
  ]) {
    const value = paragraph(fonts, text);
    expect(value.text === text, 'bidi controls, brackets and supplementary source remain reversible');
    equalPixels(glyphs(fonts, text, value), glyphs(fonts, text), `native bidi glyph oracle: ${text}`);
    const richRuns = [...text].map((letter, index) => ({ text: letter, href: index % 2 ? '/synthetic' : undefined }));
    const rich = paragraph(fonts, text, {}, richRuns);
    equalPixels(glyphs(fonts, text, rich), glyphs(fonts, text), `complete rich bidi glyph oracle: ${text}`);
    const stops = new Set(graphemeStops(text, 0, text.length));
    for (let x = -1; x < value.longestLine + 2; x += 2) {
      expect(stops.has(value.offsetAt(x, 1)), 'bidi hit testing returns a source grapheme boundary');
    }
  }
}

function clusters(fonts: Fonts): void {
  const text = 'Α\u0301👨‍👩‍👧‍👦🇬🇷👍🏽क्ष';
  const value = paragraph(fonts, text, {}, undefined, 1);
  const stops = new Set(graphemeStops(text, 0, text.length));
  const lineHeight = style.fontSize! * style.lineHeight!;
  for (let y = 1; y < value.height; y += lineHeight) {
    const range = value.lineRange(value.offsetAt(0, y));
    expect(Boolean(range && stops.has(range.start) && stops.has(range.end)), 'wrapping keeps complete clusters');
    for (const x of [-10, 0, 10, 100]) expect(stops.has(value.offsetAt(x, y)), 'hit testing keeps complete clusters');
  }
  const split = paragraph(fonts, 'e\u0301', {}, [{ text: 'e' }, { text: '\u0301', bold: true }]);
  equalPixels(glyphs(fonts, 'e\u0301', split), glyphs(fonts, 'e\u0301'), 'style boundary inside one grapheme');
  const rectangles = split.rectsForRange(1, 2);
  expect(rectangles.length === 1 && rectangles[0]!.width > 0, 'partial UTF-16 range covers the full cluster');
  const word = split.wordBoundary(1);
  expect(word.start === 0 && word.end === 2, 'word selection preserves combining source offsets');
}

function ellipsis(fonts: Fonts): void {
  const mark = fonts.width(fonts.face(style), '…');
  for (const text of ['A\u0301 B C', '👨‍👩‍👧‍👦 emoji remainder', 'אבג abc def']) {
    for (const width of [0, mark / 4, mark, mark * 2]) {
      const value = paragraph(fonts, text, { maxLines: 1 }, undefined, width);
      expect(value.longestLine <= Math.max(0.01, width) + 0.02, 'ellipsis and content obey even narrow constraints');
      expect(value.text === text, 'ellipsis does not rewrite source text');
      const end = value.offsetAt(10000, 1);
      expect(graphemeStops(text, 0, text.length).includes(end), 'ellipsis cut is grapheme-safe');
      if (width < mark) expect(end === 0, 'narrow ellipsis can remove every source grapheme');
    }
  }
}

function linesAndSpaces(fonts: Fonts): void {
  const empty = paragraph(fonts, '');
  expect(empty.height > 0 && empty.offsetAt(100, 100) === 0, 'empty paragraph has one hittable line');
  expect(empty.wordBoundary(0).end === 0 && empty.lineRange(0)?.end === 0, 'empty range queries are well-defined');
  for (const text of ['\n', '\r\n', '\r', '\u000B', '\u000C', '\u0085', '\u2028', '\u2029']) {
    const value = paragraph(fonts, text);
    expect(value.height === empty.height * 2, 'terminator preserves the trailing empty line');
    expect(value.offsetAt(0, empty.height + 1) === text.length, 'trailing empty line uses source end offset');
    expect(value.rectsForRange(0, text.length).length === 1, 'a selected terminator has visible line geometry');
    expect(hardLines(text)[0]!.next === text.length, 'CRLF stays one reversible terminator');
  }
  const multiline = paragraph(fonts, 'ab\r\ncd\n');
  const first = multiline.lineRange(3);
  const second = multiline.lineRange(4);
  expect(
    first?.start === 0 && first.end === 4 && second?.start === 4 && second.end === 7,
    'source line ranges include exactly one complete hard terminator',
  );
  const spaces = paragraph(fonts, 'text   ', { textAlign: 'center' });
  expect(
    Math.abs(spaces.longestLine - fonts.width(fonts.face(style), 'text')) < 0.001,
    'trailing spaces hang without changing alignment width',
  );
  expect(
    spaces.rectsForRange(4, 7).some((rect) => rect.width > 0),
    'trailing spaces remain selectable',
  );
  const thai = paragraph(fonts, 'ภาษาไทยภาษาไทย');
  expect(thai.minIntrinsicWidth < thai.maxIntrinsicWidth, 'dictionary segmentation supplies Thai word breaks');
  for (const glue of ['\u00A0', '\u2007', '\u202F', '\u2060', '\uFEFF']) {
    const value = paragraph(fonts, `中${glue}文`);
    expect(value.minIntrinsicWidth === value.maxIntrinsicWidth, 'nonbreaking glue survives CJK break opportunities');
  }
}

function nativeAdvances(fonts: Fonts): TextAdvance[] {
  const text = 'AVATAR Μάθε δένδρα office affinity';
  return [
    { fontSize: 14, fontWeight: 400, mono: false },
    { fontSize: 24, fontWeight: 400, mono: false },
    { fontSize: 24, fontWeight: 700, mono: false },
    { fontSize: 16, fontWeight: 400, mono: true },
  ].map((style) => ({ ...style, text, width: paragraph(fonts, text, style).longestLine }));
}

function widthTransitions(fonts: Fonts): void {
  const text = 'alpha Μάθε e\u0301 👨‍👩‍👧‍👦 omega';
  const value = paragraph(fonts, text, {}, undefined, Infinity);
  const original = glyphs(fonts, text, value);
  const height = value.height;
  value.layout(value.maxIntrinsicWidth / 4);
  expect(value.height > height, 'a previously fitting paragraph wraps after narrowing');
  const min = value.minIntrinsicWidth;
  expect(min > 0 && min < value.maxIntrinsicWidth, 'wrapping retains the word-level minimum width');
  value.layout(Infinity);
  expect(value.height === height && value.lineRange(0)?.end === text.length, 'widening restores the full source line');
  expect(value.minIntrinsicWidth === min, 'intrinsic width survives repeated display layout');
  equalPixels(glyphs(fonts, text, value), original, 'narrow-to-wide layout preserves complete shaped glyphs');
}

async function run(): Promise<{ results: Result[]; advances: TextAdvance[] }> {
  const fonts = await loadFonts('/Inter.woff2', '/RobotoMono.woff2');
  const cases = [
    shaping,
    linkedJoining,
    mixedFaces,
    mixedBidiFaces,
    bidi,
    clusters,
    ellipsis,
    linesAndSpaces,
    widthTransitions,
  ];
  const results = cases.map((check) => {
    try {
      check(fonts);
      return { name: check.name, passed: true };
    } catch (error) {
      return { name: check.name, passed: false, error: String(error) };
    }
  });
  return { results, advances: nativeAdvances(fonts) };
}

onmessage = () => {
  run()
    .then((result) => postMessage(result))
    .catch((error) => postMessage({ fatal: String(error) }));
};
