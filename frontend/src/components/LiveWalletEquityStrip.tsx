import type { ConnectionStatus, OperatorSnapshot } from '../lib/backend'
import {
  formatAbsoluteTime,
  formatFixed6Money,
  formatFixed6MoneyCompact,
  formatMoney,
  pnlToneClass,
} from '../lib/display'

function fmtSnapshotAge(tsMs: number | null): string {
  if (tsMs == null || !Number.isFinite(tsMs) || tsMs <= 0) return '—'
  const ageMs = Math.max(0, Date.now() - tsMs)
  if (ageMs < 1_000) return 'now'
  if (ageMs < 60_000) return `${Math.floor(ageMs / 1_000)}s`
  return `${Math.floor(ageMs / 60_000)}m`
}

export function LiveWalletEquityStrip({ snapshot, status }: { snapshot: OperatorSnapshot | null; status: ConnectionStatus }) {
  const account = snapshot?.account
  const exitPnl = snapshot?.exit_type_pnl?.find(([symbol]) => symbol === 'TOTAL')?.[1]
  const statusLabel = snapshot == null ? 'WAITING' : status === 'connected' ? 'LIVE' : 'LAST KNOWN'
  return (
    <div
      className="dashboard-wallet-strip rounded-md border border-[var(--color-glass-border)]"
      style={{
        background:
          'linear-gradient(180deg, rgba(4, 8, 11, 0.86), rgba(1, 3, 5, 0.94))',
      }}
    >
      <div className="dashboard-account-status-row">
        <div className="dashboard-account-status">
          <span className={`dashboard-account-status-dot ${status === 'connected' ? 'is-live' : ''}`} />
          ACCOUNT <i>•</i> {statusLabel}
        </div>
        <span>Last sync {fmtSnapshotAge(account?.wallet_observed_at_ms ?? null)}</span>
      </div>

      {snapshot == null ? (
        <div className="dashboard-account-loading">Waiting for account data</div>
      ) : (
        <>
          <div className="dashboard-account-primary-grid">
            <div className="dashboard-account-pnl-grid">
              <div className="dashboard-account-primary-card">
                <span>Unrealized PnL</span>
                <strong className={pnlToneClass(account?.market_marked_unrealized_pnl_units_6dp)}>
                  {formatFixed6MoneyCompact(account?.market_marked_unrealized_pnl_units_6dp)}
                </strong>
              </div>
              <div className="dashboard-account-primary-card">
                <span>Realized PnL</span>
                <strong className={pnlToneClass(account?.receipt_realized_pnl_units_6dp)}>
                  {formatFixed6MoneyCompact(account?.receipt_realized_pnl_units_6dp)}
                </strong>
              </div>
            </div>
            <div className="dashboard-account-primary-card">
              <span>Wallet Equity</span>
              <strong>{formatMoney(account?.current_wallet_equity)}</strong>
            </div>
          </div>

          <div className="text-[length:var(--text-ui-micro)]">
            <p className="border-b border-white/[0.08] py-0.5 font-[family-name:var(--font-sans)] font-bold text-[var(--color-text-secondary)]">Net realized PnL by exit type</p>
            <dl className="grid grid-cols-2 gap-x-2">
              {([
                ['first', 'First'],
                ['dead_cat', 'DeadCat'],
                ['profit_now', 'ProfitNow'],
                ['hold_to_settlement', 'Hold→settlement'],
              ] as const).map(([key, label]) => {
                const cell = exitPnl?.[key]
                const accountingError = exitPnl?.error || cell?.error
                return (
                  <div key={key} className="flex min-w-0 flex-wrap items-baseline justify-between gap-x-1 border-b border-white/[0.05] py-0.5">
                    <dt className="min-w-0 break-words font-[family-name:var(--font-sans)] font-bold text-[var(--color-text-secondary)]">{label}</dt>
                    <dd className="min-w-0 break-words text-[length:var(--text-ui-label)] font-black tabular-nums text-[var(--color-text-primary)]">
                      <b className={accountingError ? 'text-rose-300' : pnlToneClass(cell?.realized_pnl_units_6dp)}>
                        {accountingError ? 'Accounting error' : cell == null ? 'Unavailable' : formatFixed6Money(cell.realized_pnl_units_6dp)}
                      </b>
                    </dd>
                    {key !== 'hold_to_settlement' && (
                      <div className="w-full text-[var(--color-text-muted)]">
                        Orders / fills: {cell?.activity == null || cell.fills == null ? 'Unavailable' : `${cell.activity} / ${cell.fills}`}
                      </div>
                    )}
                    {key === 'dead_cat' && (
                      <div className="w-full space-y-0.5 text-[var(--color-text-muted)]">
                        <div className="flex flex-wrap justify-between gap-x-1"><span>Saved on losers</span><span className="tabular-nums">{exitPnl?.dead_cat_saved_units_6dp == null ? 'Unavailable' : formatFixed6Money(exitPnl.dead_cat_saved_units_6dp)}</span></div>
                        <div className="flex flex-wrap justify-between gap-x-1"><span>Forgone on winners</span><span className="tabular-nums">{exitPnl?.dead_cat_forgone_units_6dp == null ? 'Unavailable' : formatFixed6Money(exitPnl.dead_cat_forgone_units_6dp)}</span></div>
                        <div>Covered / unknown: {exitPnl?.dead_cat_comparison_covered == null || exitPnl.dead_cat_comparison_unknown == null ? 'Unavailable' : `${exitPnl.dead_cat_comparison_covered} / ${exitPnl.dead_cat_comparison_unknown}`}</div>
                      </div>
                    )}
                  </div>
                )
              })}
            </dl>
          </div>

          <details className="dashboard-wallet-details dashboard-account-details">
            <summary>Exact accounting</summary>
            <dl>
              <div><dt>Receipt realized PnL</dt><dd>{formatFixed6Money(account?.receipt_realized_pnl_units_6dp)}</dd></div>
              <div><dt>Bid-marked open PnL</dt><dd>{formatFixed6Money(account?.market_marked_unrealized_pnl_units_6dp)}</dd></div>
              <div><dt>Bid-marked total PnL</dt><dd>{formatFixed6Money(account?.market_marked_total_pnl_units_6dp)}</dd></div>
              <div><dt>CTF recovery value</dt><dd>{formatFixed6Money(account?.marked_remaining_value_units_6dp)}</dd></div>
              <div><dt>Valuation time</dt><dd>{formatAbsoluteTime(account?.valued_at_ms)}</dd></div>
              <div><dt>Balance sync</dt><dd>{formatAbsoluteTime(account?.wallet_observed_at_ms)}</dd></div>
            </dl>
            <div className="dashboard-wallet-meta">Bid-marked estimates are before future exit fees and slippage.</div>
          </details>
        </>
      )}
    </div>
  )
}
