import {
  formatFixed6Money,
  formatFixed6MoneyCompact,
  pnlToneClass,
} from '../lib/display'
import type { ConnectionStatus, ExactRealizedPnlSnapshot, OperatorSnapshot } from '../lib/backend'

const RECENT_WINDOW_MS = 15 * 60 * 1000

function availableUnits(value: ExactRealizedPnlSnapshot | undefined): string | null {
  return value?.status === 'available' ? (value.units_6dp ?? null) : null
}

function sumFixed6(values: string[]): string | null {
  try {
    return values.reduce((total, value) => total + BigInt(value), 0n).toString()
  } catch {
    return null
  }
}

export function ClosedHistoryMetricsCard({
  snapshot,
  status,
  className = '',
}: {
  snapshot: OperatorSnapshot | null
  status: ConnectionStatus
  className?: string
}) {
  const proof = snapshot?.canonical_stats.canonical
  const total = proof?.ctf_summary.find((row) => row.symbol === 'TOTAL')
  const totalPnl = availableUnits(snapshot?.closed_realized_pnl_by_symbol?.TOTAL)
  const closed = snapshot?.canonical_stats.closed_cohorts ?? []
  const wins = closed.filter((cohort) => BigInt(cohort.realized_pnl_units_6dp) > 0n).length
  const losses = closed.filter((cohort) => BigInt(cohort.realized_pnl_units_6dp) < 0n).length
  const recentCutoff = (snapshot?.generated_at_ms ?? 0) - RECENT_WINDOW_MS
  const recentPnl = snapshot == null
    ? null
    : sumFixed6(
        closed
          .filter((cohort) => cohort.last_ts_ms >= recentCutoff)
          .map((cohort) => cohort.realized_pnl_units_6dp),
      )
  const liveLabel = snapshot == null ? 'WAITING' : status === 'connected' ? 'LIVE' : 'LAST KNOWN'
  const exact = totalPnl != null

  return (
    <div className={`dashboard-ctf-overview ${className}`}>
      <div className="dashboard-ctf-status-row">
        <div className="dashboard-ctf-status">
          <span className={`dashboard-ctf-status-dot ${status === 'connected' ? 'is-live' : ''}`} />
          CTF <i>•</i> {liveLabel} {exact && <><i>•</i> EXACT</>}
        </div>
        <span>SESSION</span>
      </div>

      <div className="dashboard-ctf-primary-metrics">
        <div><span>Closed PnL</span><strong className={pnlToneClass(totalPnl)}>{formatFixed6MoneyCompact(totalPnl)}</strong></div>
      </div>

      <div className="dashboard-ctf-secondary-metrics">
        <div><span>15m PnL</span><b className={pnlToneClass(recentPnl)}>{formatFixed6MoneyCompact(recentPnl)}</b></div>
        <div><span>Cohorts</span><b>{total?.closed_cohort_count ?? '—'}</b></div>
        <div><span>W / L</span><b>{snapshot == null ? '—' : `${wins} / ${losses}`}</b></div>
      </div>

      <details className="dashboard-history-details dashboard-ctf-exact-details">
        <summary>Exact accounting</summary>
        <dl>
          <div><dt>Total closed PnL</dt><dd>{formatFixed6Money(totalPnl)}</dd></div>
          <div><dt>Stats revision</dt><dd>{proof?.revision ?? '—'}</dd></div>
        </dl>
      </details>
    </div>
  )
}
