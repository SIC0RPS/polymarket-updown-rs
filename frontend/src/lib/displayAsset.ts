export const DISPLAY_ASSETS = ['BTC', 'ETH', 'SOL', 'XRP'] as const

export type DisplayAsset = (typeof DISPLAY_ASSETS)[number]

const EXTERNAL_SYMBOL_TO_ASSET: Readonly<Record<string, DisplayAsset>> = {
  BTCUSD: 'BTC',
  BTCUSDT: 'BTC',
  ETHUSD: 'ETH',
  ETHUSDT: 'ETH',
  SOLUSD: 'SOL',
  SOLUSDT: 'SOL',
  XRPUSD: 'XRP',
  XRPUSDT: 'XRP',
}

export function toDisplayAsset(symbol: string): DisplayAsset | null {
  const normalized = symbol.toUpperCase()
  if (DISPLAY_ASSETS.includes(normalized as DisplayAsset)) return normalized as DisplayAsset
  return EXTERNAL_SYMBOL_TO_ASSET[normalized] ?? null
}
