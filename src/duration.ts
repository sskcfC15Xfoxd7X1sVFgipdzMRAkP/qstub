const UNIT_MS: Record<string, number> = {
  ms: 1,
  s: 1_000,
  m: 60_000,
  h: 3_600_000,
  d: 86_400_000,
};

export function parseDurationMs(input: string): number {
  const trimmed = input.trim();
  if (trimmed === "") {
    throw new Error("empty duration");
  }

  if (/^\d+$/.test(trimmed)) {
    return Number.parseInt(trimmed, 10) * 1_000;
  }

  const match = trimmed.match(/^(\d+)(ms|s|m|h|d)$/i);
  if (!match) {
    throw new Error(`invalid duration: ${input}`);
  }
  const value = Number.parseInt(match[1]!, 10);
  const unit = match[2]!.toLowerCase();
  const factor = UNIT_MS[unit];
  if (factor === undefined) {
    throw new Error(`unknown duration unit: ${unit}`);
  }
  return value * factor;
}
