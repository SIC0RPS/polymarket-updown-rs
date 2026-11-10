import { useState } from 'react'
import type {
  OperatorCommand,
  OperatorSnapshot,
  SendOperatorCommand,
} from '../lib/backend'

function truncateId(value: string): string {
  if (value.length <= 16) return value
  return `${value.slice(0, 8)}...${value.slice(-6)}`
}

function fmtTime(tsMs: number): string {
  if (!Number.isFinite(tsMs) || tsMs <= 0) return '-'
  return new Date(tsMs).toLocaleTimeString('en-US', { hour12: false })
}

export function LiveOrdersPanel({
  snapshot,
  loading,
  commandPending,
  sendCommand,
}: {
  snapshot: OperatorSnapshot | null
  loading: boolean
  commandPending: OperatorCommand['command'] | null
  sendCommand: SendOperatorCommand
}) {
  const [error, setError] = useState<string | null>(null)
  const openOrders = snapshot?.open_orders ?? []

  if (loading && snapshot == null) {
    return (
      <div className="text-sm text-[var(--color-text-secondary)] font-[family-name:var(--font-mono)]">
        Loading live open orders...
      </div>
    )
  }

  if (openOrders.length === 0) {
    return (
      <div className="text-sm text-[var(--color-text-secondary)] font-[family-name:var(--font-mono)]">
        No open orders currently resting on venue.
      </div>
    )
  }

  async function cancel(orderIds: string[]) {
    setError(null)
    try {
      const ack = await sendCommand({ command: 'cancel', order_ids: orderIds })
      if (ack.not_canceled_orders.length > 0) {
        setError(ack.not_canceled_orders.map((item) => `${item.order_id}: ${item.reason}`).join(' | '))
      }
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : 'cancel_failed')
    }
  }

  return (
    <div className="overflow-x-auto">
      <div className="mb-2 flex items-center justify-between gap-2">
        <span className="text-[10px] text-[var(--color-text-secondary)] font-[family-name:var(--font-mono)]">
          {openOrders.length} venue orders
        </span>
        <button
          type="button"
          disabled={commandPending === 'cancel'}
          onClick={() => void cancel(openOrders.map((order) => order.order_id))}
          className="rounded border border-rose-400/40 px-2 py-1 text-[11px] leading-[1.3] text-rose-200 disabled:opacity-45"
        >
          Cancel all
        </button>
      </div>
      {error && <div className="mb-2 text-[10px] text-rose-300">{error}</div>}
      <table className="min-w-full text-xs font-[family-name:var(--font-mono)]">
        <thead>
          <tr className="text-left text-[var(--color-text-secondary)] border-b border-white/[0.08]">
            <th className="py-2 pr-3">Order</th>
            <th className="py-2 pr-3">Side</th>
            <th className="py-2 pr-3">Asset</th>
            <th className="py-2 pr-3">Price</th>
            <th className="py-2 pr-3">Original</th>
            <th className="py-2 pr-3">Matched</th>
            <th className="py-2 pr-3">Remaining</th>
            <th className="py-2 pr-3">Status</th>
            <th className="py-2 pr-3">Updated</th>
            <th className="py-2 pr-3">Cohort</th>
            <th className="py-2 pr-3">Action</th>
          </tr>
        </thead>
        <tbody>
          {openOrders.map((order) => (
            <tr key={order.order_id} className="border-b border-white/[0.05] text-[var(--color-text-primary)]">
              <td className="py-2 pr-3">{truncateId(order.order_id)}</td>
              <td className={`py-2 pr-3 ${order.side.toUpperCase() === 'BUY' ? 'text-emerald-400' : 'text-rose-300'}`}>
                {order.side}
              </td>
              <td className="py-2 pr-3">{truncateId(order.token)}</td>
              <td className="py-2 pr-3">{order.price}</td>
              <td className="py-2 pr-3">{order.original_size}</td>
              <td className="py-2 pr-3">{order.matched_size}</td>
              <td className="py-2 pr-3">{order.remaining_size}</td>
              <td className="py-2 pr-3 text-amber-300">{order.status}</td>
              <td className="py-2 pr-3">{fmtTime(order.updated_at_ms)}</td>
              <td className="py-2 pr-3">{order.lifecycle_cohort ?? '-'}</td>
              <td className="py-2 pr-3">
                <button
                  type="button"
                  disabled={commandPending === 'cancel'}
                  onClick={() => void cancel([order.order_id])}
                  className="text-rose-300 disabled:opacity-45"
                >
                  Cancel
                </button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
