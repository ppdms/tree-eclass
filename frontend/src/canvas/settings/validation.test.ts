import assert from 'node:assert/strict';
import test from 'node:test';
import { validateWebhook } from './validation';

test('webhook HTTP schemes are case-insensitive without relaxing target safety', () => {
  for (const webhook_url of [
    'HTTPS://example.test/hooks/token',
    'hTtP://example.test/hook',
    'https://example.test/hook',
  ]) {
    assert.equal(validateWebhook({ webhook_url, clear_webhook: 'false' }), null);
  }
  for (const webhook_url of [
    'file:///private/secret',
    '/relative',
    'https://user:secret@example.test/hook',
    'HTTPS://example.test/hook#fragment',
    `https://example.test/${'x'.repeat(4096)}`,
  ]) {
    assert.equal(validateWebhook({ webhook_url, clear_webhook: 'false' })?.field, 'webhook_url');
  }
  assert.equal(validateWebhook({ webhook_url: '', clear_webhook: 'false' }), null);
  assert.equal(validateWebhook({ webhook_url: 'not-a-url', clear_webhook: 'true' }), null);
});
