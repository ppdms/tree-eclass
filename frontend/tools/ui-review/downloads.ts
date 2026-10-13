import { readFile, mkdir } from 'node:fs/promises';
import { resolve } from 'node:path';
import type { Download, Page } from '@playwright/test';
import { digest } from './provenance';
import { scenarioTimeout } from './paths';
import type { DownloadArtifact, ScenarioResult } from './evidence';
import type { ScenarioState } from './schemas';

export class Downloads {
  private pending: Promise<DownloadArtifact>[] = [];
  private notify: { accept: () => void; reject: (error: Error) => void }[] = [];
  private used = new Set<DownloadArtifact>();
  private failure: string | undefined;

  constructor(
    private root: string,
    private result: ScenarioResult,
  ) {}

  attach(page: Page) {
    page.on('download', (download) => {
      const task = this.save(download, this.pending.length);
      this.pending.push(task);
      task
        .then(() => this.notify.splice(0).forEach((callback) => callback.accept()))
        .catch((error: Error) => {
          this.result.errors.push(`Download failed: ${error.message}`);
          this.cancel(error.message);
        });
    });
  }

  private async save(download: Download, index: number): Promise<DownloadArtifact> {
    const filename = download.suggestedFilename();
    const safeName = filename.replace(/[^a-zA-Z0-9._-]/g, '_');
    const path = `${this.result.scenario.id}/downloads/${index}-${safeName}`;
    await mkdir(resolve(this.root, this.result.scenario.id, 'downloads'), { recursive: true });
    await download.saveAs(resolve(this.root, path));
    const failure = await download.failure();
    if (failure) throw new Error(failure);
    const bytes = await readFile(resolve(this.root, path));
    const artifact = { filename, path, bytes: bytes.length, sha256: digest(bytes), url: download.url() };
    this.result.downloads.push(artifact);
    return artifact;
  }

  private async available(filename: string) {
    const find = () => this.result.downloads.find((item) => item.filename === filename && !this.used.has(item));
    let artifact = find();
    while (!artifact) {
      if (this.failure) throw new Error(this.failure);
      let timer: ReturnType<typeof setTimeout> | undefined;
      let waiter: (typeof this.notify)[number] | undefined;
      await new Promise<void>((accept, reject) => {
        waiter = { accept, reject };
        this.notify.push(waiter);
        timer = setTimeout(() => reject(new Error(`Missing download: ${filename}`)), scenarioTimeout);
      }).finally(() => {
        clearTimeout(timer);
        this.notify = this.notify.filter((item) => item !== waiter);
      });
      artifact = find();
    }
    this.used.add(artifact);
    return artifact;
  }

  cancel(message: string) {
    this.failure = message;
    this.notify.splice(0).forEach((waiter) => waiter.reject(new Error(message)));
  }

  async checkpoint(name: string, state: ScenarioState) {
    for (const expected of state.downloads.filter((item) => item.checkpoint === name)) {
      const artifact = await this.available(expected.filename);
      const bytes = await readFile(resolve(this.root, artifact.path));
      if (expected.mediaType === 'application/json') JSON.parse(bytes.toString('utf8'));
      else if (expected.mediaType === 'application/pdf' && !bytes.subarray(0, 5).equals(Buffer.from('%PDF-'))) {
        throw new Error(`Invalid PDF download: ${artifact.filename}`);
      } else if (expected.mediaType !== 'application/pdf') {
        throw new Error(`Unsupported download verification type: ${expected.mediaType}`);
      }
      if (expected.textIncludes && !bytes.toString('utf8').includes(expected.textIncludes)) {
        throw new Error(`Download ${artifact.filename} does not contain ${expected.textIncludes}`);
      }
      artifact.checkpoint = name;
      artifact.verifiedMediaType = expected.mediaType;
    }
  }

  async finish(state?: ScenarioState) {
    await Promise.allSettled(this.pending);
    const expected = state?.downloads || [];
    for (const artifact of this.result.downloads) {
      if (!this.used.has(artifact)) this.result.errors.push(`Unexpected download: ${artifact.filename}`);
    }
    if (this.result.downloads.length !== expected.length) {
      this.result.errors.push(`Expected ${expected.length} downloads; saw ${this.result.downloads.length}`);
    }
  }
}
