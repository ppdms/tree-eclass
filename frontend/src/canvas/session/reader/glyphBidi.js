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

// PDF.js 6.2.108, build 0365cbde0; frontend/bun.lock and installed package agree.
// Hand-maintained factoring of the pinned src/core/bidi.js worker extraction.
// Upstream source SHA-256: 6821cddcb11529f4e81e5fa71308957297db41bbed50bfd533c9526178883386
// Local instrumentation carries UTF-16 indices through the SAME permutations/removals
// and exposes resolved character direction. Classification tables remain verbatim.
// Compare every stage with the pinned worker when upgrading; never substitute UBA heuristics.
import { baseTypes } from './glyphBidiBaseTypes.js';
import { arabicTypes } from './glyphBidiArabicTypes.js';

const chars = [];
const types = [];
function isOdd(i) {
	return (i & 1) !== 0;
}
function findUnequal(arr, start, value) {
	let j;
	for (j = start; j < arr.length; ++j) {
		if (arr[j] !== value) return j;
	}
	return j;
}
function reverseValues(arr, start, end) {
	for (let i = start, j = end - 1; i < j; ++i, --j) {
		const temp = arr[i];
		arr[i] = arr[j];
		arr[j] = temp;
	}
}
function createBidiText(str, isLTR, order, backward, vertical = false) {
	let dir = 'ltr';
	if (vertical) dir = 'ttb';
	else if (!isLTR) dir = 'rtl';
	return { str, dir, order, backward };
}
function classifyCharacters(str) {
	chars.length = types.length = str.length;
	let numBidi = 0;
	for (let i = 0; i < str.length; ++i) {
		chars[i] = str.charAt(i);
		const charCode = str.charCodeAt(i);
		let charType = 'L';
		if (charCode <= 0x00ff) charType = baseTypes[charCode];
		else if (0x0590 <= charCode && charCode <= 0x05f4) charType = 'R';
		else if (0x0600 <= charCode && charCode <= 0x06ff) {
			charType = arabicTypes[charCode & 0xff];
			if (!charType) console.warn('Bidi: invalid Unicode character ' + charCode.toString(16));
		} else if (
			(0x0700 <= charCode && charCode <= 0x08ac) ||
			(0xfb50 <= charCode && charCode <= 0xfdff) ||
			(0xfe70 <= charCode && charCode <= 0xfeff)
		)
			charType = 'AL';
		if (charType === 'R' || charType === 'AL' || charType === 'AN') numBidi++;
		types[i] = charType;
	}
	return numBidi;
}
// W1: nonspacing marks inherit the previous type, starting at sor.
function resolveNonspacing(sor) {
	let lastType = sor;
	for (let i = 0; i < types.length; ++i) {
		if (types[i] === 'NSM') types[i] = lastType;
		else lastType = types[i];
	}
}
// W2-W4: resolve Arabic/European numbers and numeric separators in upstream order.
function resolveNumbers(sor) {
	let lastType = sor;
	for (let i = 0; i < types.length; ++i) {
		const t = types[i];
		if (t === 'EN') types[i] = lastType === 'AL' ? 'AN' : 'EN';
		else if (t === 'R' || t === 'L' || t === 'AL') lastType = t;
	}
	for (let i = 0; i < types.length; ++i) {
		if (types[i] === 'AL') types[i] = 'R';
	}
	for (let i = 1; i < types.length - 1; ++i) {
		if (types[i] === 'ES' && types[i - 1] === 'EN' && types[i + 1] === 'EN') types[i] = 'EN';
		if (types[i] === 'CS' && (types[i - 1] === 'EN' || types[i - 1] === 'AN') && types[i + 1] === types[i - 1]) {
			types[i] = types[i - 1];
		}
	}
}
// W5: European terminator runs adjacent to an EN become EN.
function resolveTerminators() {
	for (let i = 0; i < types.length; ++i) {
		if (types[i] !== 'EN') continue;
		for (let j = i - 1; j >= 0; --j) {
			if (types[j] !== 'ET') break;
			types[j] = 'EN';
		}
		for (let j = i + 1; j < types.length; ++j) {
			if (types[j] !== 'ET') break;
			types[j] = 'EN';
		}
	}
}
// W6-W7: remaining separators become ON; EN after L becomes L.
function resolveRemainingWeak(sor) {
	for (let i = 0; i < types.length; ++i) {
		const t = types[i];
		if (t === 'WS' || t === 'ES' || t === 'ET' || t === 'CS') types[i] = 'ON';
	}
	let lastType = sor;
	for (let i = 0; i < types.length; ++i) {
		const t = types[i];
		if (t === 'EN') types[i] = lastType === 'L' ? 'L' : 'EN';
		else if (t === 'R' || t === 'L') lastType = t;
	}
}
// N1-N2: an ON run resolves from equal surrounding strong types, otherwise embedding direction.
function resolveNeutrals(e) {
	const sor = e;
	const eor = sor;
	for (let i = 0; i < types.length; ++i) {
		if (types[i] !== 'ON') continue;
		const end = findUnequal(types, i + 1, 'ON');
		let before = sor;
		for (let j = i - 1; j >= 0; j--) {
			const tt = types[j];
			if (tt === 'L') {
				before = 'L';
				break;
			}
			if (tt === 'R' || tt === 'EN' || tt === 'AN') {
				before = 'R';
				break;
			}
		}
		let after = eor;
		for (let j = end; j < types.length; j++) {
			const tt = types[j];
			if (tt === 'L') {
				after = 'L';
				break;
			}
			if (tt === 'R' || tt === 'EN' || tt === 'AN') {
				after = 'R';
				break;
			}
		}
		if (before === after) types.fill(before, i, end);
		i = end - 1;
	}
	for (let i = 0; i < types.length; ++i) {
		if (types[i] === 'ON') types[i] = e;
	}
}
// I1-I2: level increments are indexed by original UTF-16 positions.
function resolveLevels(levels) {
	for (let i = 0; i < types.length; ++i) {
		const t = types[i];
		if (!isOdd(levels[i])) {
			if (t === 'R') levels[i] += 1;
			else if (t === 'AN' || t === 'EN') levels[i] += 2;
		} else if (t === 'L' || t === 'AN' || t === 'EN') levels[i] += 1;
	}
}
// L2: levels remain in original order while characters and ownership move together.
function reorderCharacters(levels, order) {
	let highestLevel = -1;
	let lowestOddLevel = 99;
	for (let i = 0; i < levels.length; ++i) {
		const level = levels[i];
		if (highestLevel < level) highestLevel = level;
		if (lowestOddLevel > level && isOdd(level)) lowestOddLevel = level;
	}
	for (let level = highestLevel; level >= lowestOddLevel; --level) {
		let start = -1;
		for (let i = 0; i < levels.length; ++i) {
			if (levels[i] < level) {
				if (start >= 0) {
					reverseValues(chars, start, i);
					reverseValues(order, start, i);
					start = -1;
				}
			} else if (start < 0) start = i;
		}
		if (start >= 0) {
			reverseValues(chars, start, levels.length);
			reverseValues(order, start, levels.length);
		}
	}
}
export function glyphBidi(str, startLevel = -1, vertical = false) {
	let isLTR = true;
	const strLength = str.length;
	const order = Array.from({ length: strLength }, (_, i) => i);
	const backward = Array(strLength).fill(false);
	if (strLength === 0 || vertical) return createBidiText(str, isLTR, order, backward, vertical);
	const numBidi = classifyCharacters(str);
	if (numBidi === 0) return createBidiText(str, isLTR, order, backward);
	if (startLevel === -1) {
		if (numBidi / strLength < 0.3 && strLength > 4) startLevel = 0;
		else {
			isLTR = false;
			startLevel = 1;
		}
	}
	const levels = [];
	for (let i = 0; i < strLength; ++i) levels[i] = startLevel;
	const e = isOdd(startLevel) ? 'R' : 'L';
	resolveNonspacing(e);
	resolveNumbers(e);
	resolveTerminators();
	resolveRemainingWeak(e);
	resolveNeutrals(e);
	resolveLevels(levels);
	reorderCharacters(levels, order);
	for (let i = 0; i < chars.length; ++i) {
		const ch = chars[i];
		if (ch === '<' || ch === '>') chars[i] = '';
	}
	const kept = order.filter((_, i) => chars[i] !== '');
	return createBidiText(
		chars.join(''),
		isLTR,
		kept,
		kept.map((i) => isOdd(levels[i])),
	);
}
