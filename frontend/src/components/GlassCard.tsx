import type { ReactNode } from 'react'

interface GlassCardProps {
  children: ReactNode
  className?: string
}

export function GlassCard({ children, className = '' }: GlassCardProps) {
  return (
    <div
      className={`
        relative overflow-hidden
        bg-[var(--color-bg-card)]
        backdrop-blur-xl
        border border-[var(--color-glass-border)]
        shadow-[0_12px_30px_rgba(0,0,0,0.28)]
        rounded-md p-2 md:p-2.5
        transition-colors duration-200
        hover:border-[rgba(159,255,240,0.16)]
        ${className}
      `}
    >
      {children}
    </div>
  )
}
