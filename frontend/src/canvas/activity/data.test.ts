import assert from 'node:assert/strict';
import test from 'node:test';
import { ActivityFeed } from './data';

const group = (id: string) => ({ id, title: id, type: 'announcement' });
const page = (id: string, next_offset: number, has_more: boolean) =>
  Response.json({ groups: [group(id)], next_offset, has_more });
const failed = () => Response.json({ detail: 'Temporarily unavailable' }, { status: 503 });

test('retry refreshes an exhausted retained feed after its new first page fails', async (t) => {
  const responses = [page('old', 17, false), failed(), page('replacement', 29, false)];
  const offsets: string[] = [];
  t.mock.method(globalThis, 'fetch', async (path: string) => {
    offsets.push(new URL(path, 'http://localhost').searchParams.get('offset')!);
    return responses.shift()!;
  });
  const feed = new ActivityFeed(() => {});
  await feed.loadFirst();
  await feed.loadFirst();
  assert.equal(feed.getSnapshot().error, 'first');
  assert.equal(feed.getSnapshot().hasMore, false);
  assert.deepEqual(feed.getSnapshot().groups, [group('old')]);
  await feed.retry();
  assert.deepEqual(feed.getSnapshot().groups, [group('replacement')]);
  assert.equal(feed.getSnapshot().error, null);
  assert.equal(feed.getSnapshot().offset, 29);
  assert.deepEqual(offsets, ['0', '0', '0']);
});

test('retrying an older page preserves loaded groups and the server-provided cursor', async (t) => {
  const responses = [page('first', 17, true), failed(), page('older', 41, false)];
  const offsets: string[] = [];
  t.mock.method(globalThis, 'fetch', async (path: string) => {
    offsets.push(new URL(path, 'http://localhost').searchParams.get('offset')!);
    return responses.shift()!;
  });
  const feed = new ActivityFeed(() => {});
  await feed.loadFirst();
  await feed.loadOlder();
  assert.equal(feed.getSnapshot().error, 'older');
  assert.equal(feed.getSnapshot().offset, 17);
  assert.deepEqual(feed.getSnapshot().groups, [group('first')]);
  await feed.retry();
  assert.deepEqual(feed.getSnapshot().groups, [group('first'), group('older')]);
  assert.equal(feed.getSnapshot().offset, 41);
  assert.equal(feed.getSnapshot().hasMore, false);
  assert.equal(feed.getSnapshot().error, null);
  assert.deepEqual(offsets, ['0', '17', '17']);
});
