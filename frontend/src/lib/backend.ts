const DASHBOARD_HOST = import.meta.env.VITE_BACKEND_HOST || '127.0.0.1'
const DASHBOARD_PORT = import.meta.env.VITE_BACKEND_PORT || '3000'

const HTTP_ORIGIN = `http://${DASHBOARD_HOST}:${DASHBOARD_PORT}`
const WS_ORIGIN = `ws://${DASHBOARD_HOST}:${DASHBOARD_PORT}`

function normalizePath(path: string): string {
  return path.startsWith('/') ? path : `/${path}`
}

export function apiUrl(path: string): string {
  return `${HTTP_ORIGIN}${normalizePath(path)}`
}

export function wsUrl(path: string): string {
  return `${WS_ORIGIN}${normalizePath(path)}`
}

export type ConnectionStatus = 'disconnected' | 'connecting' | 'connected'

export interface RuntimeErrorSnapshot {
  ts_ms: number
  scope: string
  message: string
}

export interface RuntimeHealthSnapshot {
  mode: string
  started_at_ms: number
  execution_backend: string
  market_data_source: string
  pause: boolean
  trading_allowed: boolean | null
  heartbeat_active: boolean
  last_error: RuntimeErrorSnapshot | null
}

export interface AccountSnapshot {
  wallet_address: string
  wallet_kind: string
  updated_at_ms: number | null
  wallet_observed_at_ms: number | null
  wallet_usdc_balance: number | null
  trading_collateral_balance: number | null
  wallet_matic_balance: number | null
  exchange_allowance: number | null
  current_wallet_equity: number | null
  reconciliation_at_ms: number | null
  marked_remaining_value_units_6dp: string | null
  receipt_realized_pnl_units_6dp: string | null
  open_pnl_units_6dp: string | null
  total_pnl_units_6dp: string | null
  market_marked_unrealized_pnl_units_6dp: string | null
  market_marked_total_pnl_units_6dp: string | null
  valuation_portfolio_revision: number | null
  valuation_canonical_revision: number | null
  valuation_odds_revision: number | null
  valued_at_ms: number | null
}

export interface ExitTypePnlCellSnapshot {
  activity?: number
  fills?: number
  realized_pnl_units_6dp: string
  error: boolean
}

export interface ExitTypePnlSnapshot {
  first: ExitTypePnlCellSnapshot
  dead_cat: ExitTypePnlCellSnapshot
  profit_now: ExitTypePnlCellSnapshot
  hold_to_settlement: ExitTypePnlCellSnapshot
  dead_cat_saved_units_6dp?: string | null
  dead_cat_forgone_units_6dp?: string | null
  dead_cat_comparison_covered?: number
  dead_cat_comparison_unknown?: number
  error: boolean
}

export interface OpenOrderSnapshot {
  order_id: string
  market_id: string
  token: string
  side: string
  time_in_force: string
  price: number
  original_size: number
  matched_size: number
  remaining_size: number
  status: string
  placed_at_ms: number
  updated_at_ms: number
  lifecycle_cohort: string | null
  lifecycle_symbol: string | null
}

export interface PositionSnapshot {
  token: string
  market_id: string | null
  display_label: string
  quantity_units_6dp: string
  sellable_quantity_units_6dp: string
  average_price: number
  cost_basis: number
  mark_price: number | null
  unrealized_pnl_units_6dp: string | null
  remaining_basis_units_6dp: string | null
  mark_updated_at_ms: number | null
  opened_at_ms: number | null
  updated_at_ms: number
  end_time_ms: number | null
  redeemable: boolean
  mergeable: boolean
}

export interface RedeemableSnapshot {
  condition_id: string
  token: string
  title: string
  outcome: string
  outcome_index: number
  quantity_units_6dp: string
  payout_numerator: string
  payout_denominator: string
  claim_value_units_6dp: string
}

export interface RedeemConditionSnapshot {
  condition_id: string
  route: string
  status: string
  attempts: number
  first_seen_ms: number
  last_update_ms: number
  tx_hash: string | null
  error: string | null
}

export type ManagedSellReceiptIdentity =
  | {
      receipt_identity_kind: 'exchange_v2'
      venue_trade_id: string
      order_hash: string
      chain_id: string
      exchange: string
      transaction_hash: string
      block_number: string
      block_hash: string
      log_index: string
    }
  | {
      receipt_identity_kind: 'captured_simulation'
      tape_sha256: number[]
      symbol: string
      event_kind: number
      event_timestamp_ms: string
      event_start: number
      event_len: number
    }

export interface CanonicalCohortOrderIdentity {
  ledger_seq: string
  order_id: string
  intent_id: string
  asset_id: string
  accepted_at_ms: string
}

export interface CanonicalCtfLegRealization {
  confirmed_proceeds_units_6dp: string
  allocated_principal_units_6dp: string
  attributable_fee_units_6dp: string
  attributable_approved_direct_cost_units_6dp: string
  realized_pnl_units_6dp: string
}

export interface CanonicalCohortSellReceipt {
  ledger_seq: string
  receipt_id: string
  fill_id: string
  receipt_identity: ManagedSellReceiptIdentity
  order_id: string
  intent_id: string
  token: string
  qty_units_6dp: string
  gross_proceeds_units_6dp: string
  fee_units_6dp: string
  approved_direct_cost_units_6dp: string
  confirmed_at_ms: string
  close_reason: string
  realization: CanonicalCtfLegRealization
}

export interface CanonicalCohortTerminalReceipt {
  accounting_ledger_seq: string
  receipt_id: string
  transaction_id: string
  operation: 'merge' | 'redeem'
  settlement_run_id: string
  settlement_ledger_seq: string
  settlement_ts_ms: string
  finalized_log_index: string
  finalized_log_emitter: string
  finalized_block_number: string
  finalized_block_hash: string
  up_terminal_qty_units_6dp: string
  down_terminal_qty_units_6dp: string
  collateral_receipt_units_6dp: string
  fee_units_6dp: string
  approved_direct_cost_units_6dp: string
  up_realization: CanonicalCtfLegRealization | null
  down_realization: CanonicalCtfLegRealization | null
}

export interface ConfirmedCtfSplitFact {
  receipt_id: string
  transaction_id: string
  market_id: string
  condition_id: string
  cohort_id: string
  underlying: string
  market_name: string | null
  up_token: string
  down_token: string
  up_qty_units_6dp: string
  down_qty_units_6dp: string
  collateral_debit_units_6dp: string
  fee_units_6dp: string
  approved_direct_cost_units_6dp: string
}

export interface ConfirmedOrderSubmissionFact {
  order_id: string
  intent_id: string
  market_id: string
  cohort_id: string
  underlying: string
  asset_id: string
  accepted_at_ms: string
}

export interface ObservedCtfTradeFact {
  event_id: string
  trade_id: string
  market_id: string
  cohort_id: string
  trade_asset_id: string
  asset_id: string
  order_id: string
  taker_order_id: string | null
  maker_order_ids: string[]
  status: 'MATCHED' | 'MINED' | 'CONFIRMED' | 'RETRYING' | 'FAILED'
  price_units_6dp: string
  size_units_6dp: string
  owned_price_units_6dp: string
  owned_size_units_6dp: string
  matched_at_ms: string
  observed_at_ms: string
}

export interface ConfirmedCtfManagedSellFact {
  receipt_id: string
  fill_id: string
  receipt_identity: ManagedSellReceiptIdentity
  order_id: string
  intent_id: string
  market_id: string
  condition_id: string
  cohort_id: string
  token: string
  qty_units_6dp: string
  gross_proceeds_units_6dp: string
  fee_units_6dp: string
  approved_direct_cost_units_6dp: string
  confirmed_at_ms: string
  close_reason: string
  matched_trade: ObservedCtfTradeFact | null
}

export interface ConfirmedCtfTerminalFact {
  receipt_id: string
  transaction_id: string
  market_id: string
  condition_id: string
  cohort_id: string
  up_token: string
  down_token: string
  up_terminal_qty_units_6dp: string
  down_terminal_qty_units_6dp: string
  collateral_receipt_units_6dp: string
  fee_units_6dp: string
  approved_direct_cost_units_6dp: string
}

export interface ConfirmedCtfChargeFact {
  receipt_id: string
  cohort_id: string
  source_event_identity: string
  amount_units_6dp: string
  confirmed_at_ms: string
}

export interface FinalizedConditionResolutionReceipt {
  run_id: string
  ledger_seq: string
  observed_at_ms: string
  condition_id: string
  resolution_tx_hash: string
  resolution_log_index: string
  finalized_block_number: string
  finalized_block_hash: string
  finalized_block_timestamp_ms: string
  outcome_slot_0_numerator: string
  outcome_slot_1_numerator: string
  denominator: string
}

export type CanonicalCtfEvent =
  | ({ ctf_event_kind: 'split_confirmed' } & ConfirmedCtfSplitFact)
  | ({ ctf_event_kind: 'order_accepted' } & ConfirmedOrderSubmissionFact)
  | ({ ctf_event_kind: 'trade_matched' } & ObservedCtfTradeFact)
  | ({ ctf_event_kind: 'trade_confirmed' } & ConfirmedCtfManagedSellFact)
  | ({ ctf_event_kind: 'trade_failed' } & ObservedCtfTradeFact)
  | ({ ctf_event_kind: 'merge_confirmed' } & ConfirmedCtfTerminalFact)
  | ({ ctf_event_kind: 'redeem_confirmed' } & ConfirmedCtfTerminalFact)
  | ({ ctf_event_kind: 'fee_applied' } & ConfirmedCtfChargeFact)
  | ({ ctf_event_kind: 'cost_applied' } & ConfirmedCtfChargeFact)
  | ({ ctf_event_kind: 'resolution_observed' } & FinalizedConditionResolutionReceipt)

export interface CanonicalCohortEvent {
  ledger_seq: string
  ts_ms: string
  event: CanonicalCtfEvent
}

export interface CanonicalCohortCashflow {
  run_id: string
  split_receipt_id: string
  split_transaction_id: string
  split_ledger_seq: string
  split_ts_ms: string
  condition_id: string
  up_token: string
  down_token: string
  up_created_qty_units_6dp: string
  down_created_qty_units_6dp: string
  up_confirmed_sold_qty_units_6dp: string
  down_confirmed_sold_qty_units_6dp: string
  up_confirmed_merged_qty_units_6dp: string
  down_confirmed_merged_qty_units_6dp: string
  up_confirmed_redeemed_qty_units_6dp: string
  down_confirmed_redeemed_qty_units_6dp: string
  up_confirmed_disposed_qty_units_6dp: string
  down_confirmed_disposed_qty_units_6dp: string
  up_remaining_qty_units_6dp: string
  down_remaining_qty_units_6dp: string
  split_principal_units_6dp: string
  split_fee_units_6dp: string
  split_approved_direct_cost_units_6dp: string
  confirmed_sell_proceeds_units_6dp: string
  confirmed_terminal_receipts_units_6dp: string
  confirmed_fee_units_6dp: string
  confirmed_approved_direct_cost_units_6dp: string
  events: CanonicalCohortEvent[]
  order_submissions: CanonicalCohortOrderIdentity[]
  managed_sell_receipts: CanonicalCohortSellReceipt[]
  terminal_receipts: CanonicalCohortTerminalReceipt[]
}

export interface CanonicalClosedCohortStats {
  symbol: string
  paired_cohort_key: string
  trade_id: string
  market_id: string | null
  market_name: string | null
  authority_source: string | null
  owner: string | null
  durability_state: string | null
  close_hint: string | null
  reconciliation_status: string | null
  started_at_ms: number
  last_ts_ms: number
  event_count: number
  closed_outcome_share_quantity_units_6dp: string
  realized_pnl_units_6dp: string
  cashflow: CanonicalCohortCashflow
}

export interface ExactRealizedPnlSnapshot {
  status: 'available' | 'unavailable_canonical_receipt_fact'
  units_6dp?: string
}

export interface ExactOpenUnrecoveredCashSnapshot {
  status: 'available' | 'unavailable_canonical_open_cohort_fact'
  units_6dp?: string
}

export interface CanonicalCtfSummaryRow {
  symbol: 'BTC' | 'ETH' | 'SOL' | 'XRP' | 'TOTAL'
  confirmed_entry_market_count: number
  split_entries: number
  sell_now_intent_count: number
  paired_first_managed_sell_intent_count: number
  paired_post_proceeds_sell_intent_count: number
  submitted_orders: number
  paired_first_managed_sell_submitted_order_count: number
  paired_post_proceeds_sell_submitted_order_count: number
  paired_first_managed_sell_confirmed_fill_count: number
  paired_first_managed_sell_confirmed_fill_quantity_units_6dp: string
  paired_first_managed_sell_confirmed_proceeds_units_6dp: string
  paired_post_proceeds_sell_confirmed_fill_count: number
  paired_post_proceeds_sell_confirmed_fill_quantity_units_6dp: string
  paired_post_proceeds_sell_confirmed_proceeds_units_6dp: string
  confirmed_managed_sell_fill_count: number
  confirmed_managed_sell_quantity_units_6dp: string
  confirmed_managed_sell_proceeds_units_6dp: string
  sell_now_outcome_count: number
  hold_for_settlement_outcome_count: number
  closed_cohort_count: number
  open_cohort_count: number
  merge_confirmed_count: number
  redeem_confirmed_count: number
  settlement_receipt_count: number
  created_share_quantity_units_6dp: string
  confirmed_disposed_share_quantity_units_6dp: string
  open_share_quantity_units_6dp: string
  share_conservation_proven: boolean
  closed_outcome_share_quantity_units_6dp: string
  split_principal_units_6dp: string
  closed_cohort_principal_units_6dp: string
  open_cohort_principal_units_6dp: string
  open_confirmed_sell_proceeds_units_6dp: string
  open_confirmed_terminal_receipts_units_6dp: string
  open_confirmed_fee_units_6dp: string
  open_confirmed_approved_direct_cost_units_6dp: string
  open_unrecovered_cash: ExactOpenUnrecoveredCashSnapshot
  realized_pnl: ExactRealizedPnlSnapshot
}

export type CanonicalOwnedLegReason =
  | 'no_lifecycle_owned_qty'
  | 'market_window_elapsed'
  | 'no_sellable_actionable_qty'
  | 'ctf_entry_no_executable_sell_surface'
  | 'below_venue_min_size'
  | 'ctf_sell_lifecycle_active'
  | 'confirmed_first_sell_receipt_unavailable'
  | 'projection_missing'
  | 'invalid_limit_px'
  | 'ctf_sell_limit_depth_not_proven'
  | 'joint_entry_sell_up_selected'
  | 'joint_entry_sell_down_selected'
  | 'joint_entry_hold_for_settlement_selected'
  | 'dead_cat'
  | 'profit_now'
  | 'ctf_first_sell_no_side'
  | 'no_fillable_qty'
  | 'non_positive_net_proceeds'
  | 'loss_cut_floor_not_met'
  | 'paired_books_unavailable'
  | 'missing_market_oracle_input'
  | 'missing_live_fair_result'
  | 'missing_top_unwind_up'
  | 'missing_top_unwind_down'
  | 'missing_executable_ladder_both'
  | 'missing_executable_ladder_up'
  | 'missing_executable_ladder_down'
  | 'missing_executable_ladder'
  | 'invalid_probability_calibration_inputs'
  | 'oracle_emission_bounds_unavailable'
  | 'oracle_emission_lcb_nonpositive'
  | 'oracle_emission_dominance_insufficient'
  | 'oracle_cleanup_lcb_below_release'
  | 'market_quarantined_before_sell_route'

export interface CanonicalOwnedLegOutcome {
  symbol: 'BTC' | 'ETH' | 'SOL' | 'XRP'
  cohort_id: string
  ledger_seq: string
  ts_ms: string
  market_id: string
  token: string
  owned_qty_units_6dp: string
  action_qty_units_6dp: string
  execution_intent_id?: string
  causal_winner_side: 'up' | 'down' | null
  outcome: 'sell_now' | 'hold_for_settlement'
  reason: CanonicalOwnedLegReason
  close_reason: 'paired_first_managed_sell' | 'paired_post_proceeds_sell' | null
}

export interface CanonicalUnresolvedCohortStats {
  symbol: 'BTC' | 'ETH' | 'SOL' | 'XRP'
  market_id: string
  market_name: string | null
  paired_cohort_key: string
  started_at_ms: number
  last_ts_ms: number
  event_count: number
  state:
    | {
        reason: 'confirmed_receipt_quantity_incomplete'
        cashflow: CanonicalCohortCashflow
      }
    | {
        reason: 'invalid_confirmed_receipt_set'
        detail: string
      }
}

export interface CanonicalOrderTerminalSummaryRow {
  symbol: 'BTC' | 'ETH' | 'SOL' | 'XRP'
  close_reason: 'paired_first_managed_sell' | 'paired_post_proceeds_sell'
  provenance: 'submission_rejected' | 'submission_unmatched' | 'order_update'
  normalized_reject_kind: string | null
  raw_status: string | null
  raw_message_type: string | null
  raw_reason: string | null
  raw_reason_provenance: string | null
  partial_fill: boolean
  terminal_fact_count: number
  instruction_qty_units_6dp: string
  matched_qty_units_6dp: string
  unfilled_qty_units_6dp: string
}

/** Live wire shape of report_markdown Canonical CTF Exit Outcomes. */
export type CanonicalCtfExitClass =
  | 'first_to_post_proceeds'
  | 'first_to_settlement'
  | 'settlement_only_failed_mandatory_execution'
  | 'merge'

export interface CanonicalCtfExitBucket {
  cohort_count: number
  realized_pnl_units_6dp: string
}

export interface CanonicalCtfExitSummaryRow {
  symbol: string
  first_to_post_proceeds: CanonicalCtfExitBucket
  first_to_settlement: CanonicalCtfExitBucket
  settlement_only_failed_mandatory_execution: CanonicalCtfExitBucket
  merge: CanonicalCtfExitBucket
  total_realized_pnl_units_6dp: string
}

export interface CanonicalCtfExitCohortRow {
  symbol: string
  paired_cohort_key: string
  market_id: string
  market_name: string | null
  exit_class: CanonicalCtfExitClass
  split_ts_ms: string
  split_principal_units_6dp: string
  oracle_winner_side: string
  first_sell_token: string
  first_authorized_quantity_units_6dp: string
  first_evidence_revision: string
  first_submitted_order_count: number
  first_confirmed_fill_count: number
  first_confirmed_fill_quantity_units_6dp: string
  first_confirmed_proceeds_units_6dp: string
  sibling_token: string
  sibling_disposition: string
  /** Debug-style reason from exit projection: DeadCat / ProfitNow / hold reasons. */
  sibling_reason: string
  post_submitted_order_count: number
  post_confirmed_fill_count: number
  post_confirmed_fill_quantity_units_6dp: string
  post_confirmed_proceeds_units_6dp: string
  terminal_operation: string
  terminal_receipt_count: number
  terminal_up_quantity_units_6dp: string
  terminal_down_quantity_units_6dp: string
  terminal_receipt_value_units_6dp: string
  confirmed_fee_units_6dp: string
  confirmed_approved_direct_cost_units_6dp: string
  up_remaining_quantity_units_6dp: string
  down_remaining_quantity_units_6dp: string
  realized_pnl_units_6dp: string
}

export interface CanonicalSessionStats {
  schema_version: number
  canonical: {
    revision: number
    ctf_summary: CanonicalCtfSummaryRow[]
    ctf_exit_summary: CanonicalCtfExitSummaryRow[]
    ctf_exit_cohorts: CanonicalCtfExitCohortRow[]
    order_terminal_summary: CanonicalOrderTerminalSummaryRow[]
    order_execution_lineage: unknown[]
    owned_leg_transition_history: CanonicalOwnedLegOutcome[]
  }
  market_count: number
  trade_count: number
  event_count: number
  resolved_decisions: number | null
  emitted_side_decisions: number | null
  emitted_side_correct: number | null
  abstained_decisions: number | null
  wrong_side_first_sells: number | null
  owned_leg_outcomes: CanonicalOwnedLegOutcome[]
  closed_cohorts: CanonicalClosedCohortStats[]
  unresolved_cohorts: CanonicalUnresolvedCohortStats[]
  violations: string[]
}

export interface StrategySnapshot {
  ts_ms: number
  kind: string
  status: string
  p_up: number | null
  edge: number | null
}

export interface SymbolOracleSnapshot {
  symbol: string
  ts_ms: number
  price: number
  volatility: number | null
}

export interface PricePointSnapshot {
  symbol: string
  ts_ms: number
  price: number
}

export interface OperatorSnapshot {
  schema_version: 9
  sequence: number
  generated_at_ms: number
  runtime: RuntimeHealthSnapshot
  account: AccountSnapshot
  open_orders: OpenOrderSnapshot[]
  positions: PositionSnapshot[]
  redeemables: RedeemableSnapshot[]
  redeem_conditions: RedeemConditionSnapshot[]
  canonical_stats: CanonicalSessionStats
  closed_realized_pnl_by_symbol: Record<string, ExactRealizedPnlSnapshot>
  exit_type_pnl?: [string, ExitTypePnlSnapshot][]
  strategy: StrategySnapshot | null
  symbols: SymbolOracleSnapshot[]
  chart: PricePointSnapshot[]
  raw_log_tail: string[] | null
}

export type OperatorCommand =
  | { command: 'pause'; enabled: boolean }
  | { command: 'redeem' }
  | { command: 'cancel'; order_ids: string[] }

export interface CancelFailure {
  order_id: string
  reason: string
}

export interface OperatorCommandAck {
  command: 'pause' | 'redeem' | 'cancel'
  accepted: boolean
  canceled_order_ids: string[]
  not_canceled_orders: CancelFailure[]
}

export type SendOperatorCommand = (
  command: OperatorCommand,
) => Promise<OperatorCommandAck>

export function openOperatorSnapshotStream(): WebSocket {
  return new WebSocket(wsUrl('/ws/dashboard'))
}

export function parseOperatorSnapshotEvent(payload: string): OperatorSnapshot | null {
  const message = JSON.parse(payload) as {
    kind: 'snapshot'
    snapshot: OperatorSnapshot
  }
  return message.kind === 'snapshot' && message.snapshot?.schema_version === 9
    ? message.snapshot
    : null
}

export async function forwardOperatorCommand(
  command: OperatorCommand,
): Promise<OperatorCommandAck> {
  const response = await fetch(apiUrl('/api/command'), {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(command),
  })
  if (!response.ok) {
    const detail = await response.text().catch(() => '')
    throw new Error(detail.trim() || `command_http_${response.status}`)
  }
  const ack = (await response.json()) as OperatorCommandAck
  if (!ack.accepted) throw new Error(`${ack.command}_not_accepted`)
  return ack
}
