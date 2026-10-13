export interface RangeBounds {
  min: number;
  max: number;
  step: number;
  steps: number;
  precision: number;
}

function decimalPlaces(value: number): number {
  const [coefficient = '', exponent = '0'] = String(value).toLowerCase().split('e');
  const fraction = coefficient.split('.')[1]?.length ?? 0;
  return Math.max(0, Math.min(15, fraction - Number(exponent)));
}

export function rangeBounds(minimum: number, maximum: number, interval = 1): RangeBounds {
  const min = Number.isFinite(minimum) ? minimum : 0;
  const max = Number.isFinite(maximum) ? Math.max(min, maximum) : min;
  const step = Number.isFinite(interval) && interval > 0 ? interval : 1;
  const count = (max - min) / step;
  const steps = Math.floor(count + Number.EPSILON * Math.max(1, count) * 8);
  return { min, max, step, steps, precision: Math.max(decimalPlaces(min), decimalPlaces(step)) };
}

export function clampRange(value: number, bounds: RangeBounds): number {
  const finite = Number.isFinite(value) ? value : bounds.min;
  const tick = Math.max(0, Math.min(bounds.steps, Math.round((finite - bounds.min) / bounds.step)));
  return Number((bounds.min + tick * bounds.step).toFixed(bounds.precision));
}

export function rangeFraction(value: number, bounds: RangeBounds): number {
  return bounds.max > bounds.min ? (value - bounds.min) / (bounds.max - bounds.min) : 0;
}
