const ALPHABET = "0123456789abcdefghijklmnopqrstuv";

export function newMessageId(): string {
  return `msg_${randomBase32(22)}`;
}

function randomBase32(length: number): string {
  const bytes = new Uint8Array(length);
  crypto.getRandomValues(bytes);
  let out = "";
  for (let i = 0; i < length; i++) {
    out += ALPHABET[bytes[i]! % 32];
  }
  return out;
}
