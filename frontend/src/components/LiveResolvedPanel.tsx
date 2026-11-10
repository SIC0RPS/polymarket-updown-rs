import type {
  CanonicalCohortCashflow,
  CanonicalClosedCohortStats,
  CanonicalCtfLegRealization,
  OperatorSnapshot,
} from '../lib/backend'
import {
  formatFixed6MoneyCompact,
  formatFixed6Quantity,
  pnlToneClass,
} from '../lib/display'
import { DISPLAY_ASSETS } from '../lib/displayAsset'

const FIVE_MINUTES_MS = 5 * 60 * 1000

function compactLocalTime(timestampMs: number): string {
  return new Intl.DateTimeFormat(undefined, {
    hour: 'numeric',
    minute: '2-digit',
    hour12: true,
  }).format(timestampMs).replace(/\s/g, '').toUpperCase()
}

function cohortLocalWindow(startedAtMs: number): string {
  const startMs = Math.floor(startedAtMs / FIVE_MINUTES_MS) * FIVE_MINUTES_MS
  return `${compactLocalTime(startMs)}-${compactLocalTime(startMs + FIVE_MINUTES_MS)}`
}

function addRealizations(realizations: Array<CanonicalCtfLegRealization | null>): string | null {
  try {
    return realizations.reduce(
      (total, realization) => total + BigInt(realization?.realized_pnl_units_6dp ?? '0'),
      0n,
    ).toString()
  } catch {
    return null
  }
}

function legPnl(cashflow: CanonicalCohortCashflow, side: 'up' | 'down'): string | null {
  const token = side === 'up' ? cashflow.up_token : cashflow.down_token
  return addRealizations([
    ...cashflow.managed_sell_receipts
      .filter((receipt) => receipt.token === token)
      .map((receipt) => receipt.realization),
    ...cashflow.terminal_receipts.map((receipt) => (
      side === 'up' ? receipt.up_realization : receipt.down_realization
    )),
  ])
}

function CashflowDetails({ cashflow, reason }: { cashflow: CanonicalCohortCashflow; reason: string }) {
  const legs = [
    ['UP', cashflow.up_created_qty_units_6dp, cashflow.up_confirmed_sold_qty_units_6dp, cashflow.up_confirmed_merged_qty_units_6dp, cashflow.up_confirmed_redeemed_qty_units_6dp, cashflow.up_remaining_qty_units_6dp],
    ['DOWN', cashflow.down_created_qty_units_6dp, cashflow.down_confirmed_sold_qty_units_6dp, cashflow.down_confirmed_merged_qty_units_6dp, cashflow.down_confirmed_redeemed_qty_units_6dp, cashflow.down_remaining_qty_units_6dp],
  ] as const
  return (
    <details className="dashboard-cohort-details">
      <summary>Quantities and receipt identities</summary>
      <div className="dashboard-market-entry-meta">{reason}</div>
      <div className="dashboard-market-leg-list">
        {legs.map(([side, created, sold, merged, redeemed, remaining]) => (
          <div key={side} className="dashboard-market-leg-row">
            <b>{side}</b>
            <span>created {formatFixed6Quantity(created)}</span>
            <span>sold {formatFixed6Quantity(sold)}</span>
            <span>merged {formatFixed6Quantity(merged)}</span>
            <span>redeemed {formatFixed6Quantity(redeemed)}</span>
            <span>remaining {formatFixed6Quantity(remaining)}</span>
          </div>
        ))}
      </div>
      <dl className="dashboard-receipt-list">
        <div><dt>Split receipt</dt><dd>{cashflow.split_receipt_id ?? '—'}</dd></div>
        <div><dt>Split transaction</dt><dd>{cashflow.split_transaction_id ?? '—'}</dd></div>
        {cashflow.managed_sell_receipts.map((receipt, index) => (
          <div key={`sell:${index}`}><dt>Managed sell receipt</dt><dd>{receipt.receipt_id}</dd></div>
        ))}
        {cashflow.terminal_receipts.map((receipt, index) => (
          <div key={`terminal:${index}`}><dt>{receipt.operation} receipt</dt><dd>{receipt.receipt_id} | {receipt.transaction_id}</dd></div>
        ))}
      </dl>
    </details>
  )
}

export function LiveResolvedPanel({ snapshot }: { snapshot: OperatorSnapshot | null }) {
  const closedCohorts = snapshot?.canonical_stats.closed_cohorts ?? []
  const unresolvedCohorts = snapshot?.canonical_stats.unresolved_cohorts ?? []
  const symbolOrder = new Map<string, number>(DISPLAY_ASSETS.map((symbol, index) => [symbol, index]))
  const cohortsByWindow = new Map<number, CanonicalClosedCohortStats[]>()
  for (const cohort of closedCohorts) {
    const windowStart = Math.floor(cohort.started_at_ms / FIVE_MINUTES_MS) * FIVE_MINUTES_MS
    const rows = cohortsByWindow.get(windowStart) ?? []
    rows.push(cohort)
    cohortsByWindow.set(windowStart, rows)
  }
  const closedWindows = [...cohortsByWindow.entries()]
    .sort(([left], [right]) => right - left)
    .map(([windowStart, cohorts]) => [
      windowStart,
      cohorts.sort((left, right) => (symbolOrder.get(left.symbol) ?? 99) - (symbolOrder.get(right.symbol) ?? 99)),
    ] as const)

  return (
    <div className="dashboard-resolved-panel">
      {snapshot == null ? (
        <div className="dashboard-resolved-empty">Loading canonical CTF history...</div>
      ) : closedCohorts.length === 0 && unresolvedCohorts.length === 0 ? (
        <div className="dashboard-resolved-empty">No canonical CTF cohorts recorded yet.</div>
      ) : (
        <div className="dashboard-resolved-list-wrap dashboard-resolved-list-wrap-unified">
          <div className="dashboard-resolved-list">
            {closedWindows.map(([windowStart, cohorts]) => (
              <section key={windowStart} className="dashboard-ctf-window">
                <header className="dashboard-ctf-window-head">
                  <time>{cohortLocalWindow(windowStart)}</time>
                  <span>LOCAL</span>
                </header>
                <div className="dashboard-ctf-window-grid">
                  {cohorts.map((cohort) => {
                    const upPnl = legPnl(cohort.cashflow, 'up')
                    const downPnl = legPnl(cohort.cashflow, 'down')
                    return (
                      <article
                        key={cohort.paired_cohort_key}
                        className="dashboard-market-entry-card dashboard-market-entry-closed dashboard-ctf-pnl-card"
                        title={cohort.market_name ?? cohort.market_id ?? cohort.paired_cohort_key}
                      >
                        <div className="dashboard-ctf-pnl-row dashboard-ctf-pnl-summary">
                          <b>{cohort.symbol}</b>
                          <strong className={pnlToneClass(cohort.realized_pnl_units_6dp)}>
                            {formatFixed6MoneyCompact(cohort.realized_pnl_units_6dp)}
                          </strong>
                        </div>
                        <div className="dashboard-ctf-pnl-row">
                          <span>UP</span>
                          <strong className={pnlToneClass(upPnl)}>{formatFixed6MoneyCompact(upPnl)}</strong>
                        </div>
                        <div className="dashboard-ctf-pnl-row">
                          <span>DN</span>
                          <strong className={pnlToneClass(downPnl)}>{formatFixed6MoneyCompact(downPnl)}</strong>
                        </div>
                      </article>
                    )
                  })}
                </div>
              </section>
            ))}
            {unresolvedCohorts.length > 0 && (
              <section className="dashboard-ctf-window dashboard-ctf-unresolved">
                <header className="dashboard-ctf-window-head"><span>NEEDS ATTENTION</span></header>
                {unresolvedCohorts.map((cohort) => {
                  const pending = cohort.state.reason === 'confirmed_receipt_quantity_incomplete'
                  return (
                    <article key={`unresolved:${cohort.paired_cohort_key}`} className={`dashboard-market-entry-card ${pending ? 'dashboard-market-entry-open' : 'dashboard-market-entry-invalid'}`}>
                      <div className="dashboard-market-entry-state">{pending ? 'Pending open cohort' : 'Invalid canonical cohort'} | {cohort.symbol}</div>
                      <div className="dashboard-market-entry-title">{cohort.market_name ?? cohort.market_id ?? cohort.paired_cohort_key}</div>
                      {cohort.state.reason === 'confirmed_receipt_quantity_incomplete' ? (
                        <CashflowDetails cashflow={cohort.state.cashflow} reason={cohort.state.reason} />
                      ) : (
                        <details className="dashboard-cohort-details">
                          <summary>Raw reconciliation detail</summary>
                          <div className="dashboard-market-entry-meta">{cohort.state.reason}</div>
                          <div>{cohort.state.detail}</div>
                        </details>
                      )}
                    </article>
                  )
                })}
              </section>
            )}
          </div>
        </div>
      )}
    </div>
  )
}
