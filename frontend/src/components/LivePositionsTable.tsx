import type { ConnectionStatus, OperatorSnapshot, PositionSnapshot } from '../lib/backend'
import {
  formatAbsoluteTime,
  formatFixed6Money,
  formatFixed6MoneyCompact,
  formatFixed6Quantity,
  pnlToneClass,
} from '../lib/display'

function fmtPx(value: number | null): string {
  if (value == null || !Number.isFinite(value)) return '\u2014'
  return value.toFixed(4)
}

function fmtTimeLeft(endTime: number | null, snapshotTime: number | undefined): string {
  if (
    endTime == null || !Number.isFinite(endTime) || endTime <= 0 ||
    snapshotTime == null || !Number.isFinite(snapshotTime) || snapshotTime <= 0
  ) return '\u2014'
  const seconds = Math.max(0, Math.ceil((endTime - snapshotTime) / 1000))
  const minutes = Math.floor(seconds / 60)
  const remainder = String(seconds % 60).padStart(2, '0')
  if (minutes < 60) return `${minutes}:${remainder}`
  return `${Math.floor(minutes / 60)}:${String(minutes % 60).padStart(2, '0')}:${remainder}`
}

export function LivePositionsTable({
  snapshot,
  positions,
  loading,
  status,
}: {
  snapshot: OperatorSnapshot | null
  positions: PositionSnapshot[]
  loading: boolean
  status: ConnectionStatus
}) {
  if (loading && snapshot == null && positions.length === 0) {
    return (
      <div className="text-sm text-[var(--color-text-secondary)] font-[family-name:var(--font-mono)]">
        Loading live positions...
      </div>
    )
  }

  return (
    <div className="dashboard-position-list">
      {snapshot != null && status !== 'connected' ? (
        <div className="dashboard-last-known">Last-known backend positions</div>
      ) : null}
      {positions.map((position) => {
        const pnl = position.unrealized_pnl_units_6dp
        return (
          <article
            key={position.token}
            className="dashboard-position-card"
            title={position.display_label}
          >
            <div className="dashboard-position-topline">
              <div className="min-w-0">
                <div className="dashboard-position-market">Entry {position.display_label}</div>
              </div>
              <div className={`dashboard-position-pnl ${pnlToneClass(pnl)}`}>
                {formatFixed6MoneyCompact(pnl)}
                <span>Open PnL</span>
              </div>
            </div>
            <div className="dashboard-position-grid">
              <div>
                <span>Time left</span>
                <b>{fmtTimeLeft(position.end_time_ms, snapshot?.generated_at_ms)}</b>
              </div>
              <div>
                <span>Opening time</span>
                <b>{formatAbsoluteTime(position.opened_at_ms)}</b>
              </div>
              <div>
                <span>Closing time</span>
                <b>{formatAbsoluteTime(position.end_time_ms)}</b>
              </div>
            </div>
            <details className="dashboard-position-details">
              <summary>Exact position details</summary>
              <dl>
                <div><dt>Open PnL (bid-marked)</dt><dd>{formatFixed6Money(pnl)}</dd></div>
                <div><dt>Remaining quantity</dt><dd>{formatFixed6Quantity(position.quantity_units_6dp)}</dd></div>
                <div><dt>Remaining basis</dt><dd>{formatFixed6Money(position.remaining_basis_units_6dp)}</dd></div>
                <div><dt>Sellable quantity</dt><dd>{formatFixed6Quantity(position.sellable_quantity_units_6dp)}</dd></div>
                <div><dt>Canonical bid</dt><dd>{fmtPx(position.mark_price)}</dd></div>
                <div><dt>Token</dt><dd>{position.token}</dd></div>
                <div><dt>Quote timestamp</dt><dd>{formatAbsoluteTime(position.mark_updated_at_ms)}</dd></div>
              </dl>
              {position.mergeable ? <div className="dashboard-position-foot">mergeable</div> : null}
            </details>
          </article>
        )
      })}
    </div>
  )
}
