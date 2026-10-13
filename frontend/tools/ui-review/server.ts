import { createServer } from 'node:http';
import { readFile, realpath } from 'node:fs/promises';
import { extname, resolve, sep } from 'node:path';
import { z } from 'zod/v4';

const mediaTypes = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.mjs': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json',
  '.svg': 'image/svg+xml',
  '.png': 'image/png',
  '.woff2': 'font/woff2',
  '.ttf': 'font/ttf',
  '.ico': 'image/x-icon',
} satisfies Record<string, string>;

function isKnownExtension(extension: string): extension is keyof typeof mediaTypes {
  return Object.hasOwn(mediaTypes, extension);
}

export async function serve(root: string) {
  const canonical = await realpath(root);
  const server = createServer(async (request, response) => {
    try {
      if (!['GET', 'HEAD'].includes(request.method || '')) {
        response.writeHead(405).end();
        return;
      }
      const url = new URL(request.url || '/', 'http://127.0.0.1');
      const file = await realpath(
        resolve(canonical, '.' + decodeURIComponent(url.pathname === '/' ? '/index.html' : url.pathname)),
      );
      if (!file.startsWith(canonical + sep)) throw new Error('Path escapes catalog');
      const body = await readFile(file);
      const extension = extname(file);
      response.writeHead(200, {
        'Content-Type': isKnownExtension(extension) ? mediaTypes[extension] : 'application/octet-stream',
        'Cache-Control': 'no-store',
        'Content-Length': body.length,
      });
      response.end(request.method === 'HEAD' ? undefined : body);
    } catch {
      response.writeHead(404, { 'Content-Type': 'text/plain' }).end('Not in the static UI catalog');
    }
  });
  await new Promise<void>((accept, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', accept);
  });
  const address = z.object({ port: z.number().int().min(1).max(65535) }).parse(server.address());
  return {
    url: `http://127.0.0.1:${address.port}`,
    close: () =>
      new Promise<void>((accept) => {
        server.close(() => accept());
        server.closeAllConnections();
      }),
  };
}
