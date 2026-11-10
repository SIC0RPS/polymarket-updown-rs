import { useEffect, useMemo, useRef, useState } from 'react'
import {
  ColorType,
  LineSeries,
  createChart,
  type IChartApi,
  type ISeriesApi,
  type LineData,
  type UTCTimestamp,
} from 'lightweight-charts'
import type {
  AccountSnapshot,
  PricePointSnapshot,
  SymbolOracleSnapshot,
} from '../lib/backend'
import {
  CHART_ASSETS,
  emptyPriceSeries,
  equitySeries,
  fetchEquityHistory,
  fetchPriceSeries,
  livePriceSeries,
  livePriceUpdates,
  type ChartAsset,
  type ChartRange,
  type EquityHistoryResponse,
  type ValuePoint,
} from './priceChartModel'

const HISTORICAL_REFRESH_MS = 60_000
const EQUITY_COLOR = '#9ffff0'

export function OperatorPriceChartControls({
  range,
  activeAssets,
  equityVisible,
  onToggleAsset,
  onToggleEquity,
  onSelectRange,
}: {
  range: ChartRange
  activeAssets: Set<ChartAsset>
  equityVisible: boolean
  onToggleAsset: (asset: ChartAsset) => void
  onToggleEquity: () => void
  onSelectRange: (range: ChartRange) => void
}) {
  return (
    <div className="flex flex-wrap items-center gap-2">
      <div className="flex gap-1">
        {CHART_ASSETS.map(({ symbol, color }) => (
          <button
            key={symbol}
            type="button"
            aria-pressed={activeAssets.has(symbol)}
            onClick={() => onToggleAsset(symbol)}
            className="rounded border px-2 py-1 text-[11px] leading-[1.3] font-bold font-[family-name:var(--font-mono)]"
            style={{
              borderColor: activeAssets.has(symbol) ? color : 'rgba(255,255,255,0.08)',
              color: activeAssets.has(symbol) ? color : 'var(--color-text-secondary)',
            }}
          >
            {symbol}
          </button>
        ))}
        <button
          type="button"
          aria-pressed={equityVisible}
          onClick={onToggleEquity}
          className="rounded border px-2 py-1 text-[11px] leading-[1.3] font-bold font-[family-name:var(--font-mono)]"
          style={{
            borderColor: equityVisible ? EQUITY_COLOR : 'rgba(255,255,255,0.08)',
            color: equityVisible ? EQUITY_COLOR : 'var(--color-text-secondary)',
          }}
        >
          EQUITY
        </button>
      </div>
      <div className="flex gap-1">
        {(['live', '15m', '1h', 'all'] as const).map((item) => (
          <button
            key={item}
            type="button"
            onClick={() => onSelectRange(item)}
            className={`rounded border px-2 py-1 text-[11px] leading-[1.3] uppercase font-[family-name:var(--font-mono)] ${
              range === item
                ? 'border-cyan-400/50 text-cyan-200'
                : 'border-white/[0.08] text-[var(--color-text-secondary)]'
            }`}
          >
            {item}
          </button>
        ))}
      </div>
    </div>
  )
}

function OperatorPriceChart({
  className = '',
  chartPoints,
  symbols,
  account,
  range,
  activeAssets,
  equityVisible,
}: {
  className?: string
  chartPoints: PricePointSnapshot[]
  symbols: SymbolOracleSnapshot[]
  account: AccountSnapshot | null
  range: ChartRange
  activeAssets: Set<ChartAsset>
  equityVisible: boolean
}) {
  const [historicalPrices, setHistoricalPrices] = useState(emptyPriceSeries)
  const [priceUnavailable, setPriceUnavailable] = useState<ChartAsset[]>([])
  const [priceLoading, setPriceLoading] = useState(false)
  const [equityHistory, setEquityHistory] = useState<EquityHistoryResponse | null>(null)
  const [equityUnavailable, setEquityUnavailable] = useState(false)
  const [equityLoading, setEquityLoading] = useState(false)
  const hostRef = useRef<HTMLDivElement>(null)
  const chartRef = useRef<IChartApi | null>(null)
  const seriesRef = useRef<Partial<Record<ChartAsset, ISeriesApi<'Line'>>>>({})
  const equitySeriesRef = useRef<ISeriesApi<'Line'> | null>(null)
  const fittedViewKeyRef = useRef<string | null>(null)
  const renderedRangeRef = useRef<ChartRange | null>(null)
  const renderedEquityHistoryRef = useRef<EquityHistoryResponse | null>(null)
  const renderedEquityViewRef = useRef<string | null>(null)

  const activeAssetKey = [...activeAssets].sort().join(',')
  const equityUpdatedAtMs = account?.updated_at_ms
  const dataByAsset = useMemo(
    () =>
      range === 'live'
        ? livePriceSeries(chartPoints)
        : historicalPrices,
    [chartPoints, historicalPrices, range],
  )
  const liveUpdates = useMemo(() => livePriceUpdates(symbols), [symbols])

  useEffect(() => {
    if (range === 'live') {
      setHistoricalPrices(emptyPriceSeries())
      setPriceUnavailable([])
      setPriceLoading(false)
      return
    }

    let current = true
    const controller = new AbortController()
    const selected = [...activeAssets]
    setHistoricalPrices(emptyPriceSeries())
    setPriceUnavailable([])
    setPriceLoading(true)
    const load = async () => {
      const results = await Promise.allSettled(
        selected.map(async (asset) => [asset, await fetchPriceSeries(asset, range, controller.signal)] as const),
      )
      if (!current) return
      const next = emptyPriceSeries()
      const unavailable: ChartAsset[] = []
      for (let index = 0; index < results.length; index += 1) {
        const result = results[index]
        const asset = selected[index]
        if (result?.status === 'fulfilled') next[result.value[0]] = result.value[1]
        else if (asset != null) unavailable.push(asset)
      }
      setHistoricalPrices(next)
      setPriceUnavailable(unavailable)
      setPriceLoading(false)
    }
    void load()
    const refresh = window.setInterval(() => void load(), HISTORICAL_REFRESH_MS)
    return () => {
      current = false
      controller.abort()
      window.clearInterval(refresh)
    }
  }, [activeAssetKey, range])

  useEffect(() => {
    if (
      equityUpdatedAtMs == null ||
      !Number.isFinite(equityUpdatedAtMs) ||
      equityUpdatedAtMs <= 0
    ) {
      return
    }
    let current = true
    const controller = new AbortController()
    setEquityHistory(null)
    setEquityUnavailable(false)
    const load = async () => {
      setEquityLoading(true)
      try {
        const response = await fetchEquityHistory(controller.signal)
        if (!current) return
        setEquityHistory(response)
        setEquityUnavailable(false)
      } catch {
        if (!current) return
        setEquityHistory(null)
        setEquityUnavailable(true)
      } finally {
        if (current) setEquityLoading(false)
      }
    }
    void load()
    return () => {
      current = false
      controller.abort()
    }
  }, [equityUpdatedAtMs])

  useEffect(() => {
    if (!hostRef.current) return
    const chart = createChart(hostRef.current, {
      autoSize: true,
      localization: { locale: 'en-US' },
      layout: {
        background: { type: ColorType.Solid, color: 'transparent' },
        textColor: '#94a3b8',
      },
      grid: {
        vertLines: { color: 'rgba(148, 163, 184, 0.08)' },
        horzLines: { color: 'rgba(148, 163, 184, 0.08)' },
      },
      leftPriceScale: {
        visible: true,
        borderColor: 'rgba(148, 163, 184, 0.15)',
      },
      rightPriceScale: {
        visible: false,
        borderColor: 'rgba(148, 163, 184, 0.15)',
      },
      timeScale: { borderColor: 'rgba(148, 163, 184, 0.15)', timeVisible: true },
    })
    chartRef.current = chart
    for (const asset of CHART_ASSETS) {
      const scaleId = `asset-${asset.symbol}`
      seriesRef.current[asset.symbol] = chart.addSeries(LineSeries, {
        priceScaleId: scaleId,
        color: asset.color,
        lineWidth: 2,
        visible: true,
        lastValueVisible: false,
        priceLineVisible: false,
        priceFormat: { type: 'price', precision: 4, minMove: 0.0001 },
      })
      chart.priceScale(scaleId).applyOptions({
        autoScale: true,
        visible: false,
        scaleMargins: { top: 0.08, bottom: 0.08 },
      })
    }
    equitySeriesRef.current = chart.addSeries(LineSeries, {
      priceScaleId: 'left',
      color: EQUITY_COLOR,
      lineWidth: 3,
      lastValueVisible: true,
      priceLineVisible: true,
      priceLineColor: 'rgba(159, 255, 240, 0.98)',
      priceFormat: { type: 'price', precision: 4, minMove: 0.0001 },
    })
    return () => {
      chart.remove()
      chartRef.current = null
      seriesRef.current = {}
      equitySeriesRef.current = null
      fittedViewKeyRef.current = null
      renderedRangeRef.current = null
      renderedEquityHistoryRef.current = null
      renderedEquityViewRef.current = null
    }
  }, [])

  useEffect(() => {
    const replaceSeriesData = range !== 'live' || renderedRangeRef.current !== 'live'
    for (const { symbol } of CHART_ASSETS) {
      const series = seriesRef.current[symbol]
      if (!series) continue
      series.applyOptions({ visible: activeAssets.has(symbol) })
      const data = toLineData(dataByAsset[symbol])
      if (replaceSeriesData) {
        series.setData(data)
        continue
      }
      const current = series.data()
      const latestTime = current.length > 0 ? Number(current[current.length - 1].time) : null
      if (latestTime == null) {
        series.setData(data)
        continue
      }
      for (const point of data) {
        if (Number(point.time) >= latestTime) series.update(point)
      }
    }
    renderedRangeRef.current = range
    const hasVisiblePrice = [...activeAssets].some(
      (asset) => dataByAsset[asset].length > 0,
    )
    const viewKey = `${range}:${activeAssetKey}:${equityVisible}`
    if (hasVisiblePrice && fittedViewKeyRef.current !== viewKey) {
      chartRef.current?.timeScale().fitContent()
      fittedViewKeyRef.current = viewKey
    }
  }, [activeAssetKey, activeAssets, dataByAsset, equityVisible, range])

  useEffect(() => {
    if (range !== 'live') return
    for (const { symbol } of CHART_ASSETS) {
      const point = liveUpdates[symbol]
      const series = seriesRef.current[symbol]
      if (point == null || series == null) continue
      const data = series.data()
      const latestTime = data.length > 0 ? Number(data[data.length - 1].time) : null
      const next = { time: point.time as UTCTimestamp, value: point.value }
      if (latestTime == null) series.setData([next])
      else if (point.time >= latestTime) series.update(next)
    }
  }, [liveUpdates, range])

  useEffect(() => {
    equitySeriesRef.current?.applyOptions({ visible: equityVisible })
    chartRef.current?.priceScale('left').applyOptions({ visible: equityVisible })
  }, [equityVisible])

  useEffect(() => {
    const equityView = `${range}:${activeAssetKey}`
    if (
      range !== 'live' ||
      renderedEquityHistoryRef.current !== equityHistory ||
      renderedEquityViewRef.current !== equityView
    ) {
      let timeline: number[] = []
      for (const { symbol } of CHART_ASSETS) {
        if (!activeAssets.has(symbol)) continue
        const points = range === 'live'
          ? (seriesRef.current[symbol]?.data() ?? []).map((point) => Number(point.time))
          : dataByAsset[symbol].map((point) => point.time)
        if (points.length > timeline.length) timeline = points
      }
      equitySeriesRef.current?.setData(toLineData(equitySeries(equityHistory, timeline)))
      renderedEquityHistoryRef.current = equityHistory
      renderedEquityViewRef.current = equityView
    }
  }, [activeAssetKey, activeAssets, dataByAsset, equityHistory, range])

  const hasVisiblePrice = [...activeAssets].some(
    (asset) => dataByAsset[asset].length > 0 || (range === 'live' && liveUpdates[asset] != null),
  )
  const notices: string[] = []
  if (priceUnavailable.length > 0) notices.push(`Price unavailable: ${priceUnavailable.join(', ')}`)
  else if (!priceLoading && !hasVisiblePrice) {
    notices.push(range === 'live' ? 'Waiting for live price history' : 'Price history unavailable')
  }
  if (equityVisible && equityUpdatedAtMs != null) {
    if (equityUnavailable) notices.push('Equity unavailable')
    else if (!equityLoading && (equityHistory?.points.length ?? 0) === 0) {
      notices.push('Equity history unavailable')
    }
  }

  return (
    <section className={`relative overflow-hidden rounded-xl border border-white/[0.08] bg-black/30 ${className}`}>
      <div ref={hostRef} className="h-full min-h-[280px] w-full" />
      {notices.length > 0 && (
        <div className="pointer-events-none absolute inset-0 flex flex-col items-center justify-center gap-1 text-xs text-[var(--color-text-secondary)] font-[family-name:var(--font-mono)]">
          {notices.map((notice) => <div key={notice}>{notice}</div>)}
        </div>
      )}
    </section>
  )
}

function toLineData(points: ValuePoint[]): LineData<UTCTimestamp>[] {
  return points.map((point) => ({ time: point.time as UTCTimestamp, value: point.value }))
}

export default OperatorPriceChart
