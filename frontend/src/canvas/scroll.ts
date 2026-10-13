// Scroll physics owned by the render worker. On macOS the wheel deltas already
// carry the operating system's momentum, so they apply directly. Touch drags get
// iOS-style fling deceleration, rubber-band overscroll and a spring back.
// All times are absolute epoch milliseconds (timeOrigin + now) so pointer
// timestamps from the main thread and worker frame times share one clock.

const DECELERATION = 0.998; // UIScrollView "normal" deceleration rate per millisecond
const LOG_DECELERATION = Math.log(DECELERATION);
const RUBBER_BAND = 0.55; // UIScrollView rubber-band coefficient
const SPRING_RATE = 0.022; // critically damped spring, radians per millisecond
const STOP_VELOCITY = 0.02; // px per ms
const VELOCITY_WINDOW_MS = 100;

interface Sample {
  time: number;
  y: number;
}

type Motion =
  | { kind: 'idle' }
  | { kind: 'drag'; anchorY: number; anchorRaw: number; samples: Sample[] }
  | { kind: 'fling'; start: number; origin: number; velocity: number }
  | { kind: 'spring'; start: number; origin: number; velocity: number; target: number };

export class Scroller {
  offset = 0;
  #min = 0;
  #max = 0;
  #extent = 1;
  #motion: Motion = { kind: 'idle' };

  get animating(): boolean {
    return this.#motion.kind === 'fling' || this.#motion.kind === 'spring';
  }

  get dragging(): boolean {
    return this.#motion.kind === 'drag';
  }

  /** `min` is negative when the top of the canvas is hidden (a panned visual viewport). */
  setBounds(min: number, max: number, extent: number, now: number): void {
    this.#min = Math.min(0, min);
    this.#max = Math.max(this.#min, max);
    this.#extent = Math.max(1, extent);
    if (this.#motion.kind === 'idle' && this.#clamp(this.offset) !== this.offset) this.#spring(now, 0);
  }

  /** Springs to `target` (clamped), e.g. to reveal a focused field above the keyboard. */
  animateTo(target: number, now: number): void {
    if (this.#motion.kind === 'drag') return;
    this.#motion = { kind: 'spring', start: now, origin: this.offset, velocity: 0, target: this.#clamp(target) };
  }

  wheel(delta: number): void {
    if (this.#motion.kind === 'drag') return;
    this.#motion = { kind: 'idle' };
    this.offset = this.#clamp(this.offset + delta);
  }

  dragStart(y: number, time: number): void {
    this.#motion = { kind: 'drag', anchorY: y, anchorRaw: this.#unrubber(this.offset), samples: [{ time, y }] };
  }

  /** Ends a drag without momentum (the press became a text selection). */
  cancelDrag(time: number): void {
    if (this.#motion.kind !== 'drag') return;
    this.#motion = { kind: 'idle' };
    if (this.#clamp(this.offset) !== this.offset) this.#spring(time, 0);
  }

  dragMove(y: number, time: number): void {
    const motion = this.#motion;
    if (motion.kind !== 'drag') return;
    this.offset = this.#rubber(motion.anchorRaw + motion.anchorY - y);
    motion.samples.push({ time, y });
    while (motion.samples.length > 2 && time - (motion.samples[0]?.time ?? time) > VELOCITY_WINDOW_MS) {
      motion.samples.shift();
    }
  }

  dragEnd(time: number): void {
    const motion = this.#motion;
    if (motion.kind !== 'drag') return;
    const first = motion.samples[0];
    const last = motion.samples[motion.samples.length - 1];
    const span = first && last ? last.time - first.time : 0;
    const recent = last !== undefined && time - last.time < 50;
    const velocity = first && last && span > 0 && recent ? (first.y - last.y) / span : 0;
    if (this.offset < this.#min || this.offset > this.#max) {
      this.#spring(time, velocity);
    } else if (Math.abs(velocity) > STOP_VELOCITY) {
      this.#motion = { kind: 'fling', start: time, origin: this.offset, velocity };
    } else {
      this.#motion = { kind: 'idle' };
    }
  }

  /** Advances fling or spring motion to `now`; returns whether motion continues. */
  step(now: number): boolean {
    const motion = this.#motion;
    if (motion.kind === 'fling') return this.#stepFling(motion, now);
    if (motion.kind === 'spring') return this.#stepSpring(motion, now);
    return false;
  }

  #stepFling(motion: Extract<Motion, { kind: 'fling' }>, now: number): boolean {
    const elapsed = Math.max(0, now - motion.start);
    const decay = DECELERATION ** elapsed;
    const velocity = motion.velocity * decay;
    this.offset = motion.origin + (motion.velocity * (decay - 1)) / LOG_DECELERATION;
    if (this.offset < this.#min || this.offset > this.#max) {
      this.#spring(now, velocity);
      return true;
    }
    if (Math.abs(velocity) >= STOP_VELOCITY) return true;
    this.#motion = { kind: 'idle' };
    return false;
  }

  #stepSpring(motion: Extract<Motion, { kind: 'spring' }>, now: number): boolean {
    const t = Math.max(0, now - motion.start);
    const x0 = motion.origin - motion.target;
    const decay = Math.exp(-SPRING_RATE * t);
    const displacement = (x0 + (motion.velocity + SPRING_RATE * x0) * t) * decay;
    const velocity = (motion.velocity - SPRING_RATE * (motion.velocity + SPRING_RATE * x0) * t) * decay;
    this.offset = motion.target + displacement;
    if (Math.abs(displacement) > 0.25 || Math.abs(velocity) > STOP_VELOCITY) return true;
    this.offset = motion.target;
    this.#motion = { kind: 'idle' };
    return false;
  }

  #spring(time: number, velocity: number): void {
    this.#motion = { kind: 'spring', start: time, origin: this.offset, velocity, target: this.#clamp(this.offset) };
  }

  #clamp(offset: number): number {
    return Math.min(this.#max, Math.max(this.#min, offset));
  }

  /** iOS rubber band: overscroll distance x shows as (1 - 1 / (x·c/d + 1))·d. */
  #rubber(raw: number): number {
    const d = this.#extent;
    const band = (x: number) => (1 - 1 / ((x * RUBBER_BAND) / d + 1)) * d;
    if (raw < this.#min) return this.#min - band(this.#min - raw);
    if (raw > this.#max) return this.#max + band(raw - this.#max);
    return raw;
  }

  #unrubber(shown: number): number {
    const d = this.#extent;
    const inverse = (f: number) => (d / RUBBER_BAND) * (1 / (1 - Math.min(f, d * 0.999) / d) - 1);
    if (shown < this.#min) return this.#min - inverse(this.#min - shown);
    if (shown > this.#max) return this.#max + inverse(shown - this.#max);
    return shown;
  }
}
