import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import test from 'node:test';

test('exam countdowns use Athens calendar dates on every host and across DST', { timeout: 60_000 }, () => {
  const script = `
    import { daysUntilCalendarDate, parseTimestamp } from './src/lib/format.ts';
    console.log(JSON.stringify([
      daysUntilCalendarDate('2026-09-08T10:00:00Z', '2026-09-07T21:30:00Z'),
      daysUntilCalendarDate('2026-03-29T08:00:00Z', '2026-03-28T10:00:00Z'),
      daysUntilCalendarDate('2026-10-25T08:00:00Z', '2026-10-24T10:00:00Z'),
      parseTimestamp('2026-09-08 10:00:00')?.toISOString(),
      Number.isNaN(daysUntilCalendarDate('invalid')),
    ]));
  `;
  for (const timezone of ['UTC', 'Europe/Athens', 'America/Los_Angeles']) {
    const result = spawnSync(process.execPath, ['--eval', script], {
      env: { ...process.env, TZ: timezone },
      encoding: 'utf8',
    });
    assert.equal(result.status, 0, result.stderr);
    assert.deepEqual(JSON.parse(result.stdout), [0, 1, 1, '2026-09-08T10:00:00.000Z', true]);
  }
});
