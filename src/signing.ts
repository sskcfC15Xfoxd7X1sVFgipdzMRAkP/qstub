export interface JwtClaims {
  iss: string;
  sub: string;
  iat: number;
  nbf: number;
  exp: number;
  jti: string;
  body: string;
}

export interface SignArgs {
  destination: string;
  messageId: string;
  body: Uint8Array;
  signingKey: string;
  now?: number;
  ttlSeconds?: number;
}

export async function signRequest(args: SignArgs): Promise<string> {
  const nowSec = Math.floor((args.now ?? Date.now()) / 1000);
  const ttl = args.ttlSeconds ?? 5 * 60;
  const claims: JwtClaims = {
    iss: "Upstash",
    sub: args.destination,
    iat: nowSec,
    nbf: nowSec,
    exp: nowSec + ttl,
    jti: args.messageId,
    body: await sha256Base64Url(args.body),
  };
  return signJwtHs256(claims, args.signingKey);
}

async function signJwtHs256(claims: JwtClaims, key: string): Promise<string> {
  const header = { alg: "HS256", typ: "JWT" };
  const headerSegment = base64UrlEncode(textEncoder.encode(JSON.stringify(header)));
  const payloadSegment = base64UrlEncode(textEncoder.encode(JSON.stringify(claims)));
  const signingInput = `${headerSegment}.${payloadSegment}`;

  const signature = await hmacSha256(textEncoder.encode(signingInput), key);
  return `${signingInput}.${base64UrlEncode(signature)}`;
}

async function hmacSha256(data: Uint8Array, key: string): Promise<Uint8Array> {
  const cryptoKey = await crypto.subtle.importKey(
    "raw",
    bytesToBuffer(textEncoder.encode(key)),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  const sig = await crypto.subtle.sign("HMAC", cryptoKey, bytesToBuffer(data));
  return new Uint8Array(sig);
}

async function sha256Base64Url(bytes: Uint8Array): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", bytesToBuffer(bytes));
  return base64UrlEncode(new Uint8Array(digest));
}

function bytesToBuffer(view: Uint8Array): ArrayBuffer {
  const out = new ArrayBuffer(view.byteLength);
  new Uint8Array(out).set(view);
  return out;
}

const textEncoder = new TextEncoder();

export function base64UrlEncode(bytes: Uint8Array): string {
  let binary = "";
  for (let i = 0; i < bytes.length; i++) {
    binary += String.fromCharCode(bytes[i]!);
  }
  const b64 =
    typeof btoa === "function" ? btoa(binary) : Buffer.from(binary, "binary").toString("base64");
  return b64.replaceAll("+", "-").replaceAll("/", "_").replace(/=+$/, "");
}
