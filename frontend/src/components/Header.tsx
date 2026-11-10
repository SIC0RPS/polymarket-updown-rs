import { useCallback, useState, type ReactNode } from 'react'
import { WalletBadge } from './WalletBadge'
import { ConnectionStatus } from './ConnectionStatus'
import type {
  ConnectionStatus as OperatorConnectionStatus,
  OperatorCommand,
  OperatorSnapshot,
  SendOperatorCommand,
} from '../lib/backend'

function EmergencyStopButton({
  snapshot,
  commandPending,
  sendCommand,
}: {
  snapshot: OperatorSnapshot | null
  commandPending: OperatorCommand['command'] | null
  sendCommand: SendOperatorCommand
}) {
  const pauseOn = snapshot?.runtime.pause ?? false
  const [pending, setPending] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const toggle = useCallback(async () => {
    if (pending) return
    setPending(true)
    setError(null)
    try {
      await sendCommand({ command: 'pause', enabled: !pauseOn })
    } catch (err) {
      setError(err instanceof Error ? err.message : 'pause_toggle_failed')
    } finally {
      setPending(false)
    }
  }, [pauseOn, pending, sendCommand])

  const baseClass =
    'dashboard-header-control dashboard-pause-control rounded-lg border font-bold font-[family-name:var(--font-mono)] uppercase leading-none'
  const stateClass = pauseOn
    ? 'border-[var(--color-loss)]/65 bg-[rgba(60,0,24,0.42)] text-[var(--color-loss)] hover:border-[var(--color-loss)]'
    : 'border-[var(--color-primary)]/50 bg-[rgba(0,42,36,0.62)] text-[var(--color-primary-light)] hover:border-[var(--color-primary-light)]'
  const availabilityClass =
    pending || commandPending === 'pause' ? 'cursor-not-allowed opacity-65' : 'cursor-pointer'

  return (
    <button
      type="button"
      onClick={() => void toggle()}
      disabled={pending || commandPending === 'pause'}
      style={{ color: pauseOn ? 'var(--color-loss)' : 'var(--color-primary-light)' }}
      title={
        error ??
        `Runtime state: ${pauseOn ? 'pause' : 'running'}`
      }
      className={`${baseClass} ${stateClass} ${availabilityClass}`}
    >
      {pending ? 'PAUSE ...' : pauseOn ? 'PAUSE ON' : 'PAUSE OFF'}
    </button>
  )
}

export function Header({
  snapshot,
  loading,
  status,
  commandPending,
  sendCommand,
  chartControls,
}: {
  snapshot: OperatorSnapshot | null
  loading: boolean
  status: OperatorConnectionStatus
  commandPending: OperatorCommand['command'] | null
  sendCommand: SendOperatorCommand
  chartControls: ReactNode
}) {
  const commitSha =
    typeof __APP_COMMIT_SHA__ === 'string' && __APP_COMMIT_SHA__.length > 0
      ? __APP_COMMIT_SHA__
      : 'unknown'
  const runtime = snapshot?.runtime ?? null
  const strategy = snapshot?.strategy ?? null

  return (
    <header className="dashboard-header flex min-w-0 items-center justify-between gap-2">
      <div className="dashboard-header-left flex min-w-0 flex-1 items-center">
        <div className="min-w-0">{chartControls}</div>
        <div className="dashboard-commit truncate font-[family-name:var(--font-mono)]">
          commit {commitSha}
        </div>
      </div>
      <div className="dashboard-header-right flex min-w-0 max-w-[34rem] flex-col items-end">
        <div className="dashboard-header-actions flex min-w-0 items-center">
          <EmergencyStopButton
            snapshot={snapshot}
            commandPending={commandPending}
            sendCommand={sendCommand}
          />
          <div className="dashboard-header-control dashboard-status-pill flex min-w-0 items-center rounded-lg bg-white/[0.05] border border-white/[0.1]">
            <ConnectionStatus status={status} compact />
            <span className="h-4 w-px bg-white/[0.1]" />
            <WalletBadge snapshot={snapshot} loading={loading} />
          </div>
        </div>
        <div className="dashboard-header-debug font-[family-name:var(--font-mono)]">
          {runtime ? `${runtime.mode} | ${runtime.execution_backend} | ${runtime.heartbeat_active ? 'heartbeat' : 'heartbeat down'}` : 'loading runtime'}
          {strategy ? ` | ${strategy.kind}:${strategy.status}` : ''}
        </div>
      </div>
    </header>
  )
}
