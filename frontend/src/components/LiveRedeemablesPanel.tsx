import { useState } from 'react'
import type {
  ConnectionStatus,
  OperatorCommand,
  OperatorSnapshot,
  PositionSnapshot,
  RedeemConditionSnapshot,
  RedeemableSnapshot,
  SendOperatorCommand,
} from '../lib/backend'
import { formatAbsoluteTime, formatFixed6Money, formatFixed6Quantity } from '../lib/display'

function formatPayout(numerator: string, denominator: string): string {
  if (!/^\d+$/.test(numerator) || !/^\d+$/.test(denominator)) return '\u2014'
  try {
    const numeratorUnits = BigInt(numerator)
    const denominatorUnits = BigInt(denominator)
    if (denominatorUnits <= 0n) return '\u2014'
    if (numeratorUnits === 0n) return '$0'
    if (numeratorUnits === denominatorUnits) return '$1'
  } catch {
    return '\u2014'
  }
  return `${numerator}/${denominator}`
}

export function LiveRedeemablesPanel({
  snapshot,
  redeemConditions,
  redeemablePositions,
  settlementPositions,
  loading,
  status,
  commandPending,
  sendCommand,
}: {
  snapshot: OperatorSnapshot | null
  redeemConditions: RedeemConditionSnapshot[]
  redeemablePositions: RedeemableSnapshot[]
  settlementPositions: PositionSnapshot[]
  loading: boolean
  status: ConnectionStatus
  commandPending: OperatorCommand['command'] | null
  sendCommand: SendOperatorCommand
}) {
  const [redeemSubmitting, setRedeemSubmitting] = useState(false)
  const [redeemError, setRedeemError] = useState<string | null>(null)
  const redeemDisabled = redeemSubmitting || commandPending === 'redeem' || redeemablePositions.length === 0

  async function triggerRedeemNow() {
    setRedeemSubmitting(true)
    setRedeemError(null)
    try {
      await sendCommand({ command: 'redeem' })
    } catch (error) {
      setRedeemError(error instanceof Error ? error.message : String(error))
    } finally {
      setRedeemSubmitting(false)
    }
  }

  if (loading && snapshot == null) {
    return (
      <div className="text-sm text-[var(--color-text-secondary)] font-[family-name:var(--font-mono)]">
        Loading redeemable positions...
      </div>
    )
  }

  if (redeemablePositions.length === 0 && settlementPositions.length === 0 && redeemConditions.length === 0) {
    return (
      <div className="dashboard-settlement-empty">
        <span>No settlement ready</span>
      </div>
    )
  }

  return (
    <div className="space-y-2">
      {snapshot != null && status !== 'connected' ? (
        <div className="dashboard-last-known">Last-known backend settlement</div>
      ) : null}
      <div className="rounded border border-white/[0.08] bg-[var(--color-bg-surface)] px-2 py-2 text-[10px] text-[var(--color-text-secondary)] font-[family-name:var(--font-mono)]">
        <div className="flex flex-wrap items-center justify-between gap-2">
          <span>{`${redeemablePositions.length} ready | ${settlementPositions.length} waiting`}</span>
          <button
            type="button"
            className="rounded border border-cyan-300/40 bg-cyan-400/10 px-2 py-1 text-[11px] leading-[1.3] font-bold uppercase tracking-[0.08em] text-cyan-100 disabled:cursor-not-allowed disabled:opacity-45"
            disabled={redeemDisabled}
            onClick={() => void triggerRedeemNow()}
          >
            {redeemSubmitting || commandPending === 'redeem' ? 'Submitting' : 'Redeem now'}
          </button>
        </div>
        {redeemError ? <div className="mt-2 text-rose-300">{redeemError}</div> : null}
      </div>

      <div className="dashboard-redeemable-list">
        {settlementPositions.map((position) => (
          <article
            key={position.token}
            className="dashboard-redeemable-card"
            title={position.market_id ?? position.token}
          >
            <div className="dashboard-position-market">{position.display_label}</div>
            <div className="dashboard-position-meta">Settlement waiting</div>
            <div className="dashboard-position-grid dashboard-redeemable-grid">
              <div>
                <span>remaining qty</span>
                <b>{formatFixed6Quantity(position.quantity_units_6dp)}</b>
              </div>
              <div>
                <span>remaining basis</span>
                <b>{formatFixed6Money(position.remaining_basis_units_6dp)}</b>
              </div>
              <div>
                <span>market end</span>
                <b>{formatAbsoluteTime(position.end_time_ms)}</b>
              </div>
            </div>
            <div className="mt-2 truncate font-[family-name:var(--font-mono)] text-[10px] leading-[1.3] text-[var(--color-text-secondary)]">
              {position.token}
            </div>
          </article>
        ))}
        {redeemablePositions.map((position) => {
          return (
            <article
              key={position.token}
              className="dashboard-redeemable-card"
              title={position.condition_id}
            >
              <div className="flex min-w-0 items-start justify-between gap-2">
                <div className="min-w-0">
                  <div className="dashboard-position-market">
                    {position.title}
                  </div>
                  <div className="dashboard-position-meta">
                    {position.outcome} | ready
                  </div>
                </div>
                <div className="dashboard-position-pnl text-emerald-300">
                  {formatFixed6Money(position.claim_value_units_6dp)}
                </div>
              </div>
              <div className="dashboard-position-grid dashboard-redeemable-grid">
                <div>
                  <span>size</span>
                  <b>{formatFixed6Quantity(position.quantity_units_6dp)}</b>
                </div>
                <div>
                  <span>payout</span>
                  <b>{formatPayout(position.payout_numerator, position.payout_denominator)}</b>
                </div>
                <div>
                  <span>claim</span>
                  <b>{formatFixed6Money(position.claim_value_units_6dp)}</b>
                </div>
                <div>
                  <span>index</span>
                  <b>{position.outcome_index}</b>
                </div>
              </div>
              <div className="mt-2 truncate font-[family-name:var(--font-mono)] text-[10px] leading-[1.3] text-[var(--color-text-secondary)]">
                {position.token}
              </div>
            </article>
          )
        })}
        {redeemConditions.map((condition) => (
          <article
            key={condition.condition_id}
            className="dashboard-redeemable-card"
            title={condition.condition_id}
          >
            <div className="dashboard-position-market">{condition.condition_id}</div>
            <div className="dashboard-position-meta">
              {condition.status} | {condition.route} | attempts {condition.attempts}
            </div>
            <div className="dashboard-position-meta">
              first seen {formatAbsoluteTime(condition.first_seen_ms)} | updated {formatAbsoluteTime(condition.last_update_ms)}
            </div>
            {condition.tx_hash ? (
              <div className="mt-2 break-all font-[family-name:var(--font-mono)] text-[10px] text-[var(--color-text-secondary)]">
                transaction {condition.tx_hash}
              </div>
            ) : null}
            {condition.error ? <div className="mt-2 break-words text-xs text-rose-300">{condition.error}</div> : null}
          </article>
        ))}
      </div>
    </div>
  )
}
