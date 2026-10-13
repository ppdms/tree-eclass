/**
 * PDF.js may reject render work early, or leave operator/font promises pending,
 * when a document closes. Neither outcome acknowledges parser termination.
 */
export class PdfWorkLifetime {
  readonly #released = new AbortController();
  #closing = false;

  run<T>(work: Promise<T>): Promise<T> {
    const signal = this.#released.signal;
    if (signal.aborted) {
      void work.catch(() => {});
      return Promise.reject(signal.reason);
    }
    return new Promise<T>((resolve, reject) => {
      const abort = () => reject(signal.reason);
      signal.addEventListener('abort', abort, { once: true });
      void work.then(
        (value) => {
          if (this.#closing) return;
          signal.removeEventListener('abort', abort);
          resolve(value);
        },
        (error) => {
          if (this.#closing) return;
          signal.removeEventListener('abort', abort);
          reject(error);
        },
      );
    });
  }

  close(termination: Promise<void>): void {
    this.#closing = true;
    void termination.then(
      () => {
        this.#released.abort(new DOMException('PDF source was released', 'AbortError'));
      },
      () => {
        // A failed teardown is not proof that parser work stopped. The document
        // replacement barrier retains this rejection and prevents new parser work.
      },
    );
  }
}
