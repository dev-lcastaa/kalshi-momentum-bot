export const APP_NAME = 'AQLab Kalshi Scalper Center';

const labels: Record<string, string> = {
  sim: 'Test past markets', paper: 'Practice live · no real money', live: 'Real-money trading',
  calibration: 'Replay accuracy check', momentum: 'Follow price moves', noop: 'Watch only',
  queued: 'Waiting to start', running: 'In progress', succeeded: 'Finished', failed: 'Needs attention',
  canceled: 'Stopped', active: 'Connected', disabled: 'Off', enabled: 'On', engaged: 'Paused', clear: 'Ready',
  ok: 'Complete recording', degraded: 'Recovered interruptions', bad: 'Incomplete recording', unknown: 'Not checked',
  production: 'Real Kalshi account', demo: 'Kalshi practice account', user: 'Member', admin: 'Administrator',
  lifecycle: 'Market update', transport: 'Connection update', gap: 'Missing data',
  info: 'Update', warn: 'Warning', error: 'Problem',
  pnl: 'Profit / loss', fees: 'Trading fees', n_orders: 'Orders placed', n_fills: 'Completed trades',
  volume: 'Amount traded', realized_pnl: 'Profit / loss from closed trades', fees_paid: 'Fees paid',
  client_order_id: 'Order reference', trade_id: 'Trade reference', ticker: 'Market',
  side: 'Prediction', initial_count: 'Amount requested', fill_count: 'Amount bought / sold',
  remaining_count: 'Amount still waiting', count: 'Amount', price: 'Price', fee: 'Fee',
  is_taker: 'Matched immediately', ts_ms: 'Time', created_at: 'Created',
  position: 'Contracts held', cost: 'Cost', payout: 'Money paid out', result: 'Outcome',
  max_order_count: 'Most contracts in one order', max_position_per_market: 'Most contracts held per market',
  max_open_orders: 'Most orders waiting at once', max_daily_loss: 'Daily loss limit ($)',
  max_notional_per_market: 'Most money at risk per market ($)', max_orders_per_minute: 'Most orders per minute',
  lookback_s: 'Seconds of price history', lookback_seconds: 'Seconds of price history',
  min_move_bps: 'Minimum price move (basis points)', threshold: 'Minimum move to start trading',
  take_profit_cents: 'Profit target (cents)', stop_loss_cents: 'Loss limit (cents)',
  max_hold_s: 'Longest time to hold a trade (seconds)', close_before_s: 'Stop before market ends (seconds)',
  order_count: 'Contracts per order', reference: 'Price source',
  ref_source: 'Coin price source', size: 'Contracts per trade', max_position: 'Most contracts to hold',
  min_entry_price: 'Lowest buy price ($)', max_entry_price: 'Highest buy price ($)',
  max_spread: 'Largest buy / sell price gap ($)', take_profit: 'Profit target per contract ($)',
  stop_loss: 'Loss limit per contract ($)', no_entry_last_s: 'Stop new trades before end (seconds)',
  exit_before_close_s: 'Sell before market ends (seconds)', hold_to_settle: 'Hold until the market pays out',
  cooldown_s: 'Wait between trades (seconds)',
};

const help: Record<string, string> = {
  ref_source: 'Choose the coin price your bot watches.',
  lookback_s: 'How far back your bot looks to spot a price move.',
  min_move_bps: '1 basis point = 0.01%. A setting of 3 means a 0.03% move.',
  size: 'How many contracts to buy or sell in each trade.',
  max_position: 'The largest number of contracts your bot can hold.',
  min_entry_price: 'Do not buy contracts below this price. 0.15 means 15 cents.',
  max_entry_price: 'Do not buy contracts above this price. 0.85 means 85 cents.',
  max_spread: 'Skip trading when buy and sell offers are too far apart.',
  take_profit: 'Try to close a trade after the price rises by this amount.',
  stop_loss: 'Try to close a trade after the price falls by this amount. A sell price is not guaranteed.',
  max_hold_s: 'Try to close a trade after this many seconds.',
  no_entry_last_s: 'Do not start new trades this close to the end.',
  exit_before_close_s: 'Try to sell held contracts this close to the end.',
  hold_to_settle: 'Keep contracts for the final outcome instead of selling before the end.',
  cooldown_s: 'Wait this long after starting a trade before starting another.',
};

export function parameterHelp(name: string, fallback: string): string {
  return help[name] || fallback;
}

export function friendlyLabel(value: string): string {
  const key = value.toLowerCase().replaceAll(' ', '_');
  return labels[value] || labels[key] || value.replaceAll('_', ' ');
}

export function marketName(series: string): string {
  if (series.startsWith('KXBTC15M')) return 'Bitcoin';
  if (series.startsWith('KXETH15M')) return 'Ethereum';
  return series;
}

export function recordingHelp(quality: string): string {
  if (quality === 'ok') return 'No gaps or connection interruptions detected.';
  if (quality === 'degraded') return 'Connection or data gaps occurred. Updates resumed, but some events may be missing.';
  if (quality === 'bad') return 'Missing data, an unrecovered interruption, or unavailable order-book data.';
  return 'Recording completeness has not been checked.';
}

export function outcomeTone(result: string | null): string {
  return result === 'yes' ? 'good' : result === 'no' ? 'bad' : '';
}
