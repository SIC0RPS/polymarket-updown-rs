import {
  apiUrl,
  type PricePointSnapshot,
  type SymbolOracleSnapshot,
} from '../lib/backend'
import { DISPLAY_ASSETS, toDisplayAsset, type DisplayAsset } from '../lib/displayAsset'

export type ChartRange = 'live' | '15m' | '1h' | 'all'
export type ChartAsset = DisplayAsset

export type ValuePoint = {
  time: number
  value: number
}

export type PriceSeriesResponse = {
  asset: ChartAsset
  range: ChartRange
  points: ValuePoint[]
}

export type EquityHistoryPoint = {
  timestamp: number
  equity: number
}

export type EquityHistoryResponse = {
  range: 'all'
  points: EquityHistoryPoint[]
  downsampled: boolean
  coverage_start_ms: number | null
  coverage_end_ms: number | null
  history_complete: boolean
}

export const CHART_ASSETS = [
  { symbol: 'BTC', color: '#f7931a' },
  { symbol: 'ETH', color: '#627eea' },
  { symbol: 'SOL', color: '#14f195' },
  { symbol: 'XRP', color: '#25a7df' },
] as const satisfies readonly { symbol: ChartAsset; color: string }[]

export function emptyPriceSeries(): Record<ChartAsset, ValuePoint[]> {
  return Object.fromEntries(DISPLAY_ASSETS.map((asset) => [asset, [] as ValuePoint[]])) as Record<
    ChartAsset,
    ValuePoint[]
  >
}

export function livePriceSeries(
  chartPoints: PricePointSnapshot[],
): Record<ChartAsset, ValuePoint[]> {
  const grouped = Object.fromEntries(
    DISPLAY_ASSETS.map((asset) => [asset, [] as ValuePoint[]]),
  ) as Record<ChartAsset, ValuePoint[]>
  for (const point of chartPoints) {
    const normalized = toChartValuePoint(point.symbol, point.ts_ms, point.price)
    if (normalized != null) grouped[normalized.asset].push(normalized.point)
  }
  for (const asset of DISPLAY_ASSETS) grouped[asset] = normalizePoints(grouped[asset])
  return grouped
}

export function livePriceUpdates(
  symbols: SymbolOracleSnapshot[],
): Partial<Record<ChartAsset, ValuePoint>> {
  const updates: Partial<Record<ChartAsset, ValuePoint>> = {}
  for (const symbol of symbols) {
    const normalized = toChartValuePoint(symbol.symbol, symbol.ts_ms, symbol.price)
    if (normalized != null) updates[normalized.asset] = normalized.point
  }
  return updates
}

export function equitySeries(
  response: EquityHistoryResponse | null,
  timeline: number[],
): ValuePoint[] {
  const recordedChanges = normalizePoints(
    (response?.points ?? []).map((point) => ({
      time: point.timestamp / 1_000,
      value: point.equity,
    })),
  ).filter((point, index, points) => index === 0 || point.value !== points[index - 1].value)
  const pointCount = Math.min(recordedChanges.length, timeline.length)
  if (pointCount === 0) return []
  const values = sampleEvenly(recordedChanges, pointCount)
  const displayTimes = sampleEvenly(timeline, pointCount)
  return values.map((point, index) => ({ ...point, time: displayTimes[index] }))
}

export async function fetchPriceSeries(
  asset: ChartAsset,
  range: Exclude<ChartRange, 'live'>,
  signal: AbortSignal,
): Promise<ValuePoint[]> {
  const response = await fetch(apiUrl(`/api/price-series?asset=${asset}&range=${range}`), {
    cache: 'no-store',
    signal,
  })
  if (!response.ok) throw new Error(`price_series_http_${response.status}`)
  const payload = (await response.json()) as Partial<PriceSeriesResponse>
  if (payload.asset !== asset || payload.range !== range || !Array.isArray(payload.points)) {
    throw new Error('price_series_invalid_response')
  }
  if (
    payload.points.some(
      (point) =>
        point == null || !Number.isFinite(point.time) || !Number.isFinite(point.value),
    )
  ) {
    throw new Error('price_series_invalid_response')
  }
  return normalizePoints(payload.points)
}

export async function fetchEquityHistory(signal: AbortSignal): Promise<EquityHistoryResponse> {
  const response = await fetch(apiUrl('/api/equity-history?range=all'), {
    cache: 'no-store',
    signal,
  })
  if (!response.ok) throw new Error(`equity_history_http_${response.status}`)
  const payload = (await response.json()) as Partial<EquityHistoryResponse>
  if (
    payload.range !== 'all' ||
    !Array.isArray(payload.points) ||
    typeof payload.downsampled !== 'boolean' ||
    typeof payload.history_complete !== 'boolean' ||
    !isNullableFiniteNumber(payload.coverage_start_ms) ||
    !isNullableFiniteNumber(payload.coverage_end_ms) ||
    payload.points.some(
      (point) =>
        point == null ||
        !Number.isFinite(point.timestamp) ||
        !Number.isFinite(point.equity),
    )
  ) {
    throw new Error('equity_history_invalid_response')
  }
  const points = payload.points.sort((left, right) => left.timestamp - right.timestamp)
  return { ...payload, points } as EquityHistoryResponse
}

function normalizePoints(points: ValuePoint[]): ValuePoint[] {
  const byTimestamp = new Map<number, ValuePoint>()
  for (const point of points) {
    if (!Number.isFinite(point.time) || !Number.isFinite(point.value)) continue
    byTimestamp.set(point.time, point)
  }
  return [...byTimestamp.values()].sort((left, right) => left.time - right.time)
}

function toChartValuePoint(
  symbol: string,
  timestampMs: number,
  value: number,
): { asset: ChartAsset; point: ValuePoint } | null {
  const asset = toDisplayAsset(symbol)
  if (asset == null || !Number.isFinite(timestampMs) || !Number.isFinite(value)) return null
  return { asset, point: { time: Math.floor(timestampMs / 1_000), value } }
}

function sampleEvenly<T>(values: T[], pointCount: number): T[] {
  if (pointCount === 1) return [values[values.length - 1]]
  return Array.from({ length: pointCount }, (_, index) =>
    values[Math.round(index * (values.length - 1) / (pointCount - 1))],
  )
}

function isNullableFiniteNumber(value: unknown): value is number | null {
  return value === null || (typeof value === 'number' && Number.isFinite(value))
}
