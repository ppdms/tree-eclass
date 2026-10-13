import assert from 'node:assert/strict';
import test from 'node:test';
import { documentLayout } from './layout.ts';
import { neededPages, pageOffset, reconcileLayout } from './navigationPlan.ts';

const size = { width: 612, height: 792 };

test('a distant jump survives a delayed size discovery at an intermediate spring offset', () => {
  const before = documentLayout(40, size, new Map(), 1, 800);
  const after = documentLayout(
    40,
    size,
    new Map([
      [1, { width: 612, height: 1200 }],
      [30, { width: 612, height: 1600 }],
    ]),
    1,
    800,
  );
  const destination = { page: 30, fraction: 0.2 };
  const intermediate = pageOffset(before, { page: 12, fraction: 0 });
  const next = reconcileLayout(before, after, intermediate, destination);
  assert.equal(next.offset, intermediate);
  assert.deepEqual(next.scroll, { offset: pageOffset(after, destination), behavior: 'smooth' });
  assert.notEqual(next.scroll!.offset, intermediate);
});

test('a size change that does not move the jump destination leaves its spring untouched', () => {
  const before = documentLayout(40, size, new Map(), 1, 800);
  const after = documentLayout(40, size, new Map([[39, { width: 612, height: 1600 }]]), 1, 800);
  assert.deepEqual(reconcileLayout(before, after, 5000, { page: 30, fraction: 0 }), { offset: 5000, scroll: null });
});

test('a delayed final-page size retargets a jump whose original destination was scroll-bound clamped', () => {
  const short = { width: 612, height: 300 };
  const before = documentLayout(30, short, new Map(), 1, 800);
  const after = documentLayout(30, short, new Map([[30, { width: 612, height: 1200 }]]), 1, 800);
  const destination = { page: 30, fraction: 0 };
  assert.notEqual(pageOffset(before, destination, 900), pageOffset(after, destination, 900));
  assert.deepEqual(reconcileLayout(before, after, 5000, destination, 900), {
    offset: 5000,
    scroll: { offset: pageOffset(after, destination, 900), behavior: 'smooth' },
  });
});

test('ordinary zoom corrections keep the same position within the currently viewed page', () => {
  const before = documentLayout(20, size, new Map(), 1, 800);
  const after = documentLayout(20, size, new Map(), 1.5, 800);
  const offset = before.pages[8]!.top + size.height * 0.5;
  const expected = after.pages[8]!.top + after.pages[8]!.height * 0.5;
  assert.deepEqual(reconcileLayout(before, after, offset, null), {
    offset: expected,
    scroll: { offset: expected, behavior: 'auto' },
  });
});

test('scrolling before the original raster paints schedules the visible page alongside the target', () => {
  assert.deepEqual(neededPages([9, 10], 1, 9, 40, false), [1, 9, 10]);
  assert.deepEqual(neededPages([17, 18], 1, 18, 40, false), [1, 18, 17]);
  assert.deepEqual(neededPages([30], 30, 30, 40, false), [30]);
});

test('a painted destination yields priority to the current page and then adjacent prefetch', () => {
  assert.deepEqual(neededPages([9, 10], 1, 10, 40, true), [10, 9, 8, 11]);
  assert.deepEqual(neededPages([40], 40, 40, 40, true), [40, 39]);
});
