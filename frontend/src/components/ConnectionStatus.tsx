import type { ConnectionStatus as OperatorConnectionStatus } from '../lib/backend'

export function ConnectionStatus({
  status,
  compact = false,
}: {
  status: OperatorConnectionStatus
  compact?: boolean
}) {
  const color =
    status === 'connected'
      ? 'text-[var(--color-profit)]'
      : status === 'connecting'
        ? 'text-[var(--color-warning)]'
        : 'text-[var(--color-loss)]'

  const dot =
    status === 'connected'
      ? 'bg-[var(--color-profit)]'
      : status === 'connecting'
        ? 'bg-[var(--color-warning)]'
        : 'bg-[var(--color-loss)]'

  return (
    <div className={`dashboard-connection-status flex min-w-0 items-center ${compact ? 'gap-1' : 'gap-1.5'}`}>
      <div className={`dashboard-status-dot rounded-full ${dot} ${status === 'connecting' ? 'animate-pulse' : ''}`} />
      <span
        className={`dashboard-header-text truncate ${color} font-[family-name:var(--font-mono)] capitalize leading-none`}
      >
        {status}
      </span>
    </div>
  )
}
