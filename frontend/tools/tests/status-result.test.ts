import { expect, test } from 'bun:test';
import { isSuccessfulResult } from '../../src/features/settings/format';

test('accepts the backend success vocabulary for status indicators', () => {
  expect(isSuccessfulResult('success')).toBe(true);
  expect(isSuccessfulResult('ok')).toBe(true);
  expect(isSuccessfulResult('error')).toBe(false);
  expect(isSuccessfulResult(undefined)).toBe(false);
});
