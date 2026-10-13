import type { Provenance, ScenarioEntry, ScenarioState } from './schemas';

export interface ScreenshotArtifact {
  name: string;
  path: string;
  sha256: string;
}

export interface DownloadArtifact {
  checkpoint?: string;
  filename: string;
  path: string;
  sha256: string;
  bytes: number;
  url: string;
  verifiedMediaType?: string;
}

export interface ScenarioResult {
  scenario: ScenarioEntry;
  status: 'pending' | 'running' | 'passed' | 'failed';
  startedAt?: string;
  finishedAt?: string;
  review?: ScenarioState;
  screenshots: ScreenshotArtifact[];
  downloads: DownloadArtifact[];
  trace?: string;
  browserLog?: string;
  errors: string[];
}

export interface RunManifest {
  schemaVersion: 1;
  name: string;
  status: 'incomplete' | 'completed';
  startedAt: string;
  finishedAt?: string;
  provenance: Provenance;
  viewport: { width: number; height: number };
  environment: { locale: string; timezoneId: string; colorScheme: string; deviceScaleFactor: number };
  fixedTime: string;
  concurrency: number;
  browser?: string;
  scenarios: ScenarioResult[];
  errors: string[];
}
