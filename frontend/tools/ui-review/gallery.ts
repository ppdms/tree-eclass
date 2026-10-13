import { writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import type { RunManifest, ScenarioResult } from './evidence';

function escape(value: string) {
  return value.replace(
    /[&<>"']/g,
    (character) =>
      ({
        '&': '&amp;',
        '<': '&lt;',
        '>': '&gt;',
        '"': '&quot;',
        "'": '&#39;',
      })[character] || character,
  );
}

function scenario(result: ScenarioResult) {
  const name = escape(`${result.scenario.title} · ${result.scenario.name}`);
  const screenshots = result.screenshots
    .map(
      (image) => `
    <figure><a href="${escape(image.path)}"><img loading="lazy" src="${escape(image.path)}"
      alt="${name}: ${escape(image.name)}"></a><figcaption>${escape(image.name)}</figcaption></figure>`,
    )
    .join('');
  const errors = result.errors.length ? `<pre>${escape(result.errors.join('\n'))}</pre>` : '';
  const downloads = result.downloads
    .map((item) => `<a href="${escape(item.path)}">Download: ${escape(item.filename)}</a>`)
    .join(' ');
  const trace = result.trace ? `<a href="${escape(result.trace)}">Playwright trace</a>` : '';
  const log = result.browserLog ? `<a href="${escape(result.browserLog)}">Browser/network log</a>` : '';
  return `<section id="${escape(result.scenario.id)}"><h2>${name}</h2>
    <p class="${result.status}">${escape(result.status.toUpperCase())}
      · <code>${escape(result.scenario.id)}</code></p>
    <p>${escape(result.review?.fixture.id || 'Fixture not reached')} · synthetic
      · theme ${escape(result.review?.theme || 'unknown')}</p>
    ${errors}<p class="links">${trace} ${log} ${downloads}</p><div class="shots">${screenshots}</div></section>`;
}

export async function gallery(root: string, manifest: RunManifest) {
  const failures = manifest.scenarios.filter((item) => item.status === 'failed').length;
  const passed = manifest.scenarios.filter((item) => item.status === 'passed').length;
  const links = manifest.scenarios
    .map((item) => `<li><a href="#${escape(item.scenario.id)}">${escape(item.scenario.id)}</a> · ${item.status}</li>`)
    .join('');
  const html = `<!doctype html><html lang="en"><head><meta charset="utf-8">
    <meta name="viewport" content="width=device-width,initial-scale=1">
    <title>${escape(manifest.name)} · UI review</title>
    <style>
    body{margin:0;background:#fafafa;color:#18181b;font:16px/1.5 system-ui,sans-serif}
    main{max-width:1440px;padding:32px;margin:auto}a{color:#1d4ed8}h1,h2{line-height:1.25}
    section{padding:24px;border:1px solid #d4d4d8;border-radius:12px;background:white;margin:24px 0}
    .shots{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,440px),1fr));gap:20px}
    figure{margin:0}img{width:100%;height:auto;display:block;border:1px solid #d4d4d8}
    figcaption{padding:8px 0;font-weight:600}.links{display:flex;gap:20px;flex-wrap:wrap}
    pre{white-space:pre-wrap;overflow-wrap:anywhere;padding:16px;background:#fff1f2}
    .failed{color:#be123c}.passed{color:#4d7c0f}.pending,.running{color:#a16207}code{overflow-wrap:anywhere}
    </style></head><body><main><h1>${escape(manifest.name)}</h1>
    <p><strong>${manifest.status.toUpperCase()}</strong> · ${passed} passed · ${failures} failed
      · ${manifest.scenarios.length} selected · ${manifest.viewport.width} × ${manifest.viewport.height}</p>
    <p>Offline synthetic evidence; not proof of authentication, backend persistence, integrations or image parity.</p>
    <p><a href="manifest.json">Run manifest</a> · Build <code>${escape(manifest.provenance.sourceHash)}</code></p>
    ${manifest.errors.length ? `<pre>${escape(manifest.errors.join('\n'))}</pre>` : ''}
    <details><summary>${manifest.scenarios.length} scenarios</summary><ul>${links}</ul></details>
    ${manifest.scenarios.map(scenario).join('')}</main></body></html>`;
  await writeFile(resolve(root, 'index.html'), html);
}
