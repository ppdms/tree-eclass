import { prepareWorker, storybook } from './commands';

await prepareWorker();
await storybook([
  'dev',
  '--host',
  '127.0.0.1',
  '--port',
  '6006',
  '--no-open',
  '--disable-telemetry',
  ...process.argv.slice(2),
]).catch((error: Error) => {
  console.error(error.message);
  process.exitCode = 1;
});
