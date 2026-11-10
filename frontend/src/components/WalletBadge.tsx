import type { OperatorSnapshot } from '../lib/backend'

function formatUSDC(raw: number | string | null): string {
  if (raw == null) return '\u2014'
  const num = Number(raw)
  if (!Number.isFinite(num)) return '\u2014'
  return `$${num.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
}

export function WalletBadge({
  snapshot,
  loading,
}: {
  snapshot: OperatorSnapshot | null
  loading: boolean
}) {
  const displayBalance = snapshot?.account.trading_collateral_balance ?? null

  if (loading) {
    return (
      <div className="dashboard-header-text px-1 py-0.5 font-[family-name:var(--font-mono)] text-[var(--color-text-secondary)] leading-none">
        Loading...
      </div>
    )
  }

  if (!snapshot) {
    return (
      <div className="flex min-w-0 items-center gap-1 px-1 py-0.5">
        <span className="dashboard-status-dot rounded-full bg-red-500" />
        <span className="dashboard-header-text truncate font-[family-name:var(--font-mono)] text-[var(--color-text-secondary)] leading-none">
          No Wallet
        </span>
      </div>
    )
  }

  return (
    <div className="flex min-w-0 items-center gap-1 px-1 py-0.5">
      <span className="dashboard-status-dot rounded-full bg-emerald-400" />
      <span className="dashboard-header-text truncate font-[family-name:var(--font-mono)] text-[var(--color-text-primary)] leading-none">
        {formatUSDC(displayBalance)}
      </span>
      <span className="dashboard-header-text truncate font-[family-name:var(--font-mono)] text-[var(--color-text-secondary)] uppercase leading-none">
        pUSD
      </span>
    </div>
  )
}
