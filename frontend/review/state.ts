export interface ExpectedHttpError {
  method: string;
  path: string;
  status: number;
  count: number;
}

export interface ExpectedDownload {
  checkpoint: string;
  filename: string;
  mediaType: string;
  textIncludes?: string;
}

export interface ExpectedNetworkError {
  method: string;
  path: string;
  errorText: string;
  count: number;
  requestOrdinal: number;
  responseStatus: number;
  responseContentType: string;
  responseHeader: { name: string; value: string };
  failedCheckpoint: string;
  recoveredCheckpoint: string;
}

export interface ReviewState {
  status: 'running' | 'passed' | 'failed';
  fixture: { id: string; source: 'synthetic' };
  requiredCheckpoints: string[];
  checkpoints: string[];
  expectedHttpErrors: ExpectedHttpError[];
  expectedNetworkErrors: ExpectedNetworkError[];
  downloads: ExpectedDownload[];
  errors: string[];
  theme: 'dark' | 'light' | 'eink';
}

declare global {
  interface Window {
    __TREE_UI_REVIEW__?: ReviewState;
    __treeCaptureCheckpoint?: (name: string) => Promise<void>;
  }
}

export function currentReview(): ReviewState {
  if (!window.__TREE_UI_REVIEW__) throw new Error('Missing offline review state');
  return window.__TREE_UI_REVIEW__;
}
