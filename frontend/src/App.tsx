import { useCallback, useEffect, useState, type ReactNode } from 'react'
import { GlassCard } from './components/GlassCard'
import { Header } from './components/Header'
import { ClosedHistoryMetricsCard } from './components/ClosedHistoryMetricsCard'
import { LiveOrdersPanel } from './components/LiveOrdersPanel'
import { LivePositionsTable } from './components/LivePositionsTable'
import { LiveRedeemablesPanel } from './components/LiveRedeemablesPanel'
import { LiveResolvedPanel } from './components/LiveResolvedPanel'
import { LiveWalletEquityStrip } from './components/LiveWalletEquityStrip'
import PriceChart, { OperatorPriceChartControls } from './components/OperatorPriceChart'
import { CHART_ASSETS, type ChartAsset, type ChartRange } from './components/priceChartModel'
import {
  forwardOperatorCommand,
  openOperatorSnapshotStream,
  parseOperatorSnapshotEvent,
  type ConnectionStatus,
  type OperatorCommand,
  type OperatorSnapshot,
  type PositionSnapshot,
} from './lib/backend'

function DashboardSection({
  title,
  subtitle,
  className = '',
  bodyClassName = '',
  children,
}: {
  title: string
  subtitle?: string
  className?: string
  bodyClassName?: string
  children: ReactNode
}) {
  return (
    <GlassCard className={`flex flex-col ${className}`}>
      <div className="dashboard-section-header mb-2 flex items-center justify-between gap-3">
        <h2 className="text-[var(--color-text-primary)] font-semibold text-sm tracking-wide uppercase font-[family-name:var(--font-mono)]">
          {title}
        </h2>
        {subtitle && (
          <span className="text-xs font-[family-name:var(--font-mono)] text-[var(--color-text-secondary)]">
            {subtitle}
          </span>
        )}
      </div>
      <div className={`min-h-0 flex-1 ${bodyClassName}`}>{children}</div>
    </GlassCard>
  )
}

export default function App() {
  const [status, setStatus] = useState<ConnectionStatus>('disconnected')
  const [snapshot, setSnapshot] = useState<OperatorSnapshot | null>(null)
  const [loading, setLoading] = useState(true)
  const [commandPending, setCommandPending] = useState<OperatorCommand['command'] | null>(null)
  const [chartRange, setChartRange] = useState<ChartRange>('live')
  const [equityVisible, setEquityVisible] = useState(true)
  const [activeChartAssets, setActiveChartAssets] = useState<Set<ChartAsset>>(
    () => new Set(CHART_ASSETS.map(({ symbol }) => symbol)),
  )

  const toggleChartAsset = useCallback((asset: ChartAsset) => {
    setActiveChartAssets((current) => {
      const next = new Set(current)
      if (next.has(asset) && next.size > 1) next.delete(asset)
      else next.add(asset)
      return next
    })
  }, [])

  const applySnapshot = useCallback((next: OperatorSnapshot, replaceChartHistory: boolean) => {
    setSnapshot((current) =>
      current == null || replaceChartHistory
        ? next
        : { ...next, chart: current.chart },
    )
    setLoading(false)
  }, [])

  const sendCommand = useCallback(async (command: OperatorCommand) => {
    setCommandPending(command.command)
    try {
      return await forwardOperatorCommand(command)
    } finally {
      setCommandPending(null)
    }
  }, [])

  const openOrdersCount = snapshot?.open_orders.length ?? 0
  const redeemConditions = (snapshot?.redeem_conditions ?? []).filter(
    (condition) => condition.status !== 'confirmed',
  )
  const readyConditions = new Set(
    redeemConditions.filter((condition) => condition.status === 'ready').map((condition) => condition.condition_id),
  )
  const redeemablePositions = snapshot?.redeemables.filter(
    (claim) => readyConditions.has(claim.condition_id),
  ) ?? []
  const readyTokens = new Set(redeemablePositions.map((claim) => claim.token))
  const openPositions: PositionSnapshot[] = []
  const settlementPositions: PositionSnapshot[] = []
  for (const position of snapshot?.positions ?? []) {
    const units = position.quantity_units_6dp
    if (!/^-?\d+$/.test(units) || BigInt(units) === 0n || readyTokens.has(position.token)) continue
    const expired = position.end_time_ms != null
      && position.end_time_ms > 0
      && position.end_time_ms <= snapshot!.generated_at_ms
    if (position.redeemable || expired) settlementPositions.push(position)
    else openPositions.push(position)
  }
  const ordersExpanded = openOrdersCount > 0
  const showRedeemables = redeemablePositions.length > 0
    || settlementPositions.length > 0
    || redeemConditions.length > 0

  useEffect(() => {
    let socket: WebSocket | null = null
    let reconnectTimer: number | null = null
    let stopped = false

    function connect() {
      if (stopped || socket != null) return
      let receivedSnapshot = false
      setStatus('connecting')
      const next = openOperatorSnapshotStream()
      socket = next
      next.onopen = () => {
        if (socket !== next) return
        setStatus('connected')
      }
      next.onmessage = (event) => {
        if (socket !== next || typeof event.data !== 'string') return
        try {
          const nextSnapshot = parseOperatorSnapshotEvent(event.data)
          if (nextSnapshot != null) {
            applySnapshot(nextSnapshot, !receivedSnapshot)
            receivedSnapshot = true
          }
        } catch {
          // The backend remains authoritative; retain the last valid typed snapshot.
        }
      }
      next.onclose = () => {
        if (socket !== next) return
        socket = null
        setStatus('disconnected')
        if (!stopped) reconnectTimer = window.setTimeout(connect, 3_000)
      }
      next.onerror = () => next.close()
    }

    connect()
    return () => {
      stopped = true
      if (reconnectTimer != null) window.clearTimeout(reconnectTimer)
      const current = socket
      socket = null
      current?.close()
    }
  }, [applySnapshot])

  return (
    <div className="dashboard-app relative">
      <div className="relative z-10 flex h-full w-full max-w-[1440px] flex-col mx-auto px-3 md:px-4 py-3 md:py-4 gap-3">
        <Header
          snapshot={snapshot}
          loading={loading}
          status={status}
          commandPending={commandPending}
          sendCommand={sendCommand}
          chartControls={(
            <OperatorPriceChartControls
              range={chartRange}
              activeAssets={activeChartAssets}
              equityVisible={equityVisible}
              onToggleAsset={toggleChartAsset}
              onToggleEquity={() => setEquityVisible((visible) => !visible)}
              onSelectRange={setChartRange}
            />
          )}
        />

        {snapshot?.runtime.last_error && (
          <div className="rounded border border-amber-500/35 bg-amber-500/10 px-3 py-2 text-xs text-amber-200 font-[family-name:var(--font-mono)]">
            {snapshot.runtime.last_error.message}
          </div>
        )}

        {snapshot && snapshot.canonical_stats.violations.length > 0 && (
          <div className="rounded border border-rose-500/35 bg-rose-500/10 px-3 py-2 text-xs text-rose-200 font-[family-name:var(--font-mono)]">
            {snapshot.canonical_stats.violations.join(' | ')}
          </div>
        )}

        <div className="dashboard-shell">
          <div className="dashboard-live-grid">
            <div className={`dashboard-column dashboard-left-column ${ordersExpanded ? 'orders-expanded' : 'orders-collapsed'}`}>
              <DashboardSection
                title="Open Orders"
                className="dashboard-orders-section"
                bodyClassName="dashboard-orders-body"
              >
                <LiveOrdersPanel
                  snapshot={snapshot}
                  loading={loading}
                  commandPending={commandPending}
                  sendCommand={sendCommand}
                />
              </DashboardSection>
              <DashboardSection
                title="Open Positions"
                className="dashboard-positions-section"
                bodyClassName="overflow-hidden flex flex-col"
              >
                <LivePositionsTable snapshot={snapshot} positions={openPositions} loading={loading} status={status} />
              </DashboardSection>
              {showRedeemables && (
                <DashboardSection
                  title="Settlement"
                  className="dashboard-redeemables-section"
                  bodyClassName="overflow-y-auto overflow-x-hidden"
                >
                  <LiveRedeemablesPanel
                    snapshot={snapshot}
                    redeemConditions={redeemConditions}
                    redeemablePositions={redeemablePositions}
                    settlementPositions={settlementPositions}
                    loading={loading}
                    status={status}
                    commandPending={commandPending}
                    sendCommand={sendCommand}
                  />
                </DashboardSection>
              )}
            </div>

            <div className="dashboard-column dashboard-center-column">
              <PriceChart
                className="dashboard-price-chart"
                chartPoints={snapshot?.chart ?? []}
                symbols={snapshot?.symbols ?? []}
                account={snapshot?.account ?? null}
                range={chartRange}
                activeAssets={activeChartAssets}
                equityVisible={equityVisible}
              />
              <LiveWalletEquityStrip snapshot={snapshot} status={status} />
            </div>

            <div className="dashboard-column dashboard-right-column">
              <DashboardSection title="Canonical CTF History" className="dashboard-closed-history" bodyClassName="overflow-hidden flex flex-col">
                <ClosedHistoryMetricsCard
                  className="dashboard-stats-card overflow-hidden"
                  snapshot={snapshot}
                  status={status}
                />
                <LiveResolvedPanel snapshot={snapshot} />
              </DashboardSection>
            </div>
          </div>
        </div>
        {snapshot?.raw_log_tail && snapshot.raw_log_tail.length > 0 && (
          <details className="rounded border border-white/[0.08] bg-black/30 px-3 py-2 text-[10px] text-[var(--color-text-secondary)] font-[family-name:var(--font-mono)]">
            <summary>Raw log tail</summary>
            <pre className="mt-2 overflow-auto whitespace-pre-wrap">{snapshot.raw_log_tail.join('\n')}</pre>
          </details>
        )}
      </div>
    </div>
  )
}
