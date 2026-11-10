const FIXED_6DP_SCALE = 1_000_000n

export function formatMoney(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return '\u2014'
  return `$${value.toLocaleString('en-US', {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  })}`
}

function parseFixed6(units: string | null | undefined): bigint | null {
  if (units == null || !/^-?\d+$/.test(units)) return null
  try {
    return BigInt(units)
  } catch {
    return null
  }
}

function formatFixed6Absolute(units: bigint, trimFraction: boolean): string {
  const absolute = units < 0n ? -units : units
  const whole = (absolute / FIXED_6DP_SCALE).toString().replace(/\B(?=(\d{3})+(?!\d))/g, ',')
  const rawFraction = (absolute % FIXED_6DP_SCALE).toString().padStart(6, '0')
  const fraction = trimFraction ? rawFraction.replace(/0+$/, '') : rawFraction
  return fraction.length > 0 ? `${whole}.${fraction}` : whole
}

export function formatFixed6Money(units: string | null | undefined): string {
  const parsed = parseFixed6(units)
  if (parsed == null) return '\u2014'
  const prefix = parsed > 0n ? '+$' : parsed < 0n ? '-$' : '$'
  return `${prefix}${formatFixed6Absolute(parsed, false)}`
}

export function formatFixed6Quantity(units: string | null | undefined): string {
  const parsed = parseFixed6(units)
  if (parsed == null) return '\u2014'
  const prefix = parsed < 0n ? '-' : ''
  return `${prefix}${formatFixed6Absolute(parsed, true)}`
}

export function formatFixed6MoneyCompact(units: string | null | undefined): string {
  const parsed = parseFixed6(units)
  if (parsed == null) return '\u2014'
  const absolute = parsed < 0n ? -parsed : parsed
  const roundedCents = (absolute + 5_000n) / 10_000n
  const dollars = (roundedCents / 100n).toString().replace(/\B(?=(\d{3})+(?!\d))/g, ',')
  const cents = (roundedCents % 100n).toString().padStart(2, '0')
  const prefix = roundedCents === 0n ? '$' : parsed > 0n ? '+$' : '-$'
  return `${prefix}${dollars}.${cents}`
}

export function pnlToneClass(units: string | null | undefined): string {
  const parsed = parseFixed6(units)
  if (parsed == null || parsed === 0n) return 'pnl-neutral'
  return parsed > 0n ? 'pnl-positive' : 'pnl-negative'
}

export function formatAbsoluteTime(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value) || value <= 0) return '\u2014'
  return new Date(value).toLocaleString('en-US', {
    month: 'short',
    day: 'numeric',
    hour: 'numeric',
    minute: '2-digit',
    second: '2-digit',
    hour12: true,
  })
}
