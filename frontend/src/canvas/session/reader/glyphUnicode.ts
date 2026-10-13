/* Copyright 2012 Mozilla Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// The normalization expression and replacement semantics below are extracted from
// PDF.js 6.2.108 (0365cbde0), src/shared/util.js:normalizeUnicode. Unlike a blanket
// NFKC pass, this leaves ordinary source text (including Greek combining marks)
// untouched. Ownership tracking is local code; glyphBidi.js factors the pinned
// extraction. Update both against the pinned worker when upgrading PDF.js.
const normalizeRegex = new RegExp(
  [
    String.raw`([\u00a0\u00b5\u037e\u0eb3\u2000-\u200a\u202f\u2126\ufb00-\ufb04\ufb06`,
    String.raw`\ufb20-\ufb36\ufb38-\ufb3c\ufb3e\ufb40\ufb41\ufb43\ufb44\ufb46-\ufba1`,
    String.raw`\ufba4-\ufba9\ufbae-\ufbb1\ufbd3-\ufbdc\ufbde-\ufbe7\ufbea-\ufbf8\ufbfc\ufbfd`,
    String.raw`\ufc00-\ufc5d\ufc64-\ufcf1\ufcf5-\ufd3d\ufd88\ufdf4\ufdfa\ufdfb`,
    String.raw`\ufe71\ufe77\ufe79\ufe7b\ufe7d]+)|(\ufb05+)`,
  ].join(''),
  'gu',
);

export function glyphNormalizeUnicode(text: string): string {
  return text.replaceAll(normalizeRegex, (_match, compatible: string | undefined, special: string) =>
    compatible ? compatible.normalize('NFKC') : String(special === '\ufb05' ? 'ſt' : undefined),
  );
}

export interface GlyphUnicode {
  text: string;
  owners: number[][];
}

function union(owners: number[][]): number[] {
  return [...new Set(owners.flat())];
}

function composedOwners(raw: string, at: number, owners: number[][]): number[][] {
  let prefix = '';
  let result = '';
  const sources: number[][] = [];
  let offset = at;
  for (const character of raw) {
    prefix += character;
    const next = prefix.normalize('NFKC');
    const incoming = union(owners.slice(offset, offset + character.length));
    let shared = 0;
    while (shared < result.length && shared < next.length && result[shared] === next[shared]) shared++;
    let suffix = 0;
    while (
      suffix < result.length - shared &&
      suffix < next.length - shared &&
      result[result.length - suffix - 1] === next[next.length - suffix - 1]
    )
      suffix++;
    const changed = union([...sources.slice(shared, result.length - suffix), incoming]);
    sources.splice(
      shared,
      result.length - shared - suffix,
      ...Array<number[]>(next.length - shared - suffix).fill(changed),
    );
    result = next;
    offset += character.length;
  }
  return sources;
}

function compatibleOwners(raw: string, replacement: string, at: number, owners: number[][]): number[][] {
  let expanded = '';
  const individual: number[][] = [];
  let offset = at;
  for (const character of raw) {
    const next = character.normalize('NFKC');
    const source = union(owners.slice(offset, offset + character.length));
    expanded += next;
    for (let i = 0; i < next.length; i++) individual.push(source);
    offset += character.length;
  }
  // Only composition across original characters needs prefix rewriting;
  // ordinary ligature runs retain the original linear ownership path.
  return expanded === replacement ? individual : composedOwners(raw, at, owners);
}

/** Tracks original glyph clusters, not character-value lookups, across normalization. */
export function normalizeGlyphUnicode(text: string, owners: number[][]): GlyphUnicode {
  let normalized = '';
  const mapped: number[][] = [];
  let copied = 0;
  for (const match of text.matchAll(normalizeRegex)) {
    const at = match.index;
    normalized += text.slice(copied, at);
    mapped.push(...owners.slice(copied, at));
    const raw = match[0];
    const replacement = glyphNormalizeUnicode(raw);
    if (match[2]) {
      // Preserve even the upstream repeated-U+FB05 replacement semantics.
      const source = union(owners.slice(at, at + raw.length));
      mapped.push(...Array<number[]>(replacement.length).fill(source));
    } else {
      mapped.push(...compatibleOwners(raw, replacement, at, owners));
    }
    normalized += replacement;
    copied = at + raw.length;
  }
  normalized += text.slice(copied);
  mapped.push(...owners.slice(copied));
  return { text: normalized, owners: mapped };
}
