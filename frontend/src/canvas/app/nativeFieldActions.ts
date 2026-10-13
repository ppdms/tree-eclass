import { configureControl, type NativeControl } from './browserFields';
import type { NativeFieldDescriptor } from './browserProtocol';

export function showNativePicker(control: NativeControl, field: NativeFieldDescriptor): void {
  if (
    !(control instanceof HTMLInputElement) ||
    field.readOnly ||
    !['date', 'time', 'datetime-local'].includes(field.type ?? '')
  )
    return;
  try {
    control.showPicker();
  } catch {
    /* Focus still provides the native platform editor. */
  }
}

export function nativeValidationMessage(
  control: NativeControl,
  field: NativeFieldDescriptor,
  draft: string,
): string | null {
  configureControl(control, field, true);
  control.value = draft;
  if (field.type === 'number' && draft !== '' && control.value === '') {
    // The real number control's parser rejected an editable draft; no regex approximates its grammar.
    control.setCustomValidity('Enter a valid number.');
  }
  const message = control.checkValidity() ? null : control.validationMessage;
  control.value = '';
  return message;
}
