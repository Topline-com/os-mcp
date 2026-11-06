// A pasted credential (a Private Integration Token or a Location ID) is
// usable only when every character is printable ASCII, U+0020 to U+007E.
// Any other character can't go in an HTTP header, and the runtime's warning
// about such a header prints the value. So callers check this before a
// connection is stored and before a header is built, and their refusal
// never echoes the value or any part of it.
const PRINTABLE_ASCII = /^[\x20-\x7E]+$/;

/** True when `value` is non-empty and every character is printable ASCII. */
export function isPrintableAscii(value: string): boolean {
  return PRINTABLE_ASCII.test(value);
}
