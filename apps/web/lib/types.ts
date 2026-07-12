export interface Position {
  symbol: string;
  direction: "long" | "short";
  quantity: number;
  entry_price: number;
  current_price?: number;
  unrealized_pnl?: number;
  stop_loss?: number;
  take_profit?: number;
  broker: string;
  broker_order_id?: string;
}

export interface Signal {
  symbol: string;
  direction: string;
  confidence: number;
  entry_price: number;
  timeframe: string;
  generated_at: string;
}

export interface Trade {
  id: number;
  symbol: string;
  direction: "long" | "short";
  quantity: number;
  broker: string;
  timeframe: string;
  signal_confidence: number | null;
  entry_price: number;
  exit_price: number | null;
  planned_sl: number | null;
  planned_tp: number | null;
  entry_datetime: string;
  exit_datetime: string | null;
  exit_reason: string | null;
  realized_pnl: number | null;
  duration_seconds: number | null;
  rr_achieved: number | null;
  is_winner: boolean | null;
}

export interface JournalStats {
  total_trades: number;
  winners: number;
  losers: number;
  win_rate: number;
  total_pnl: number;
  gross_profit: number;
  gross_loss: number;
  profit_factor: number;
  avg_winner: number;
  avg_loser: number;
  best_trade: number;
  worst_trade: number;
  avg_duration_seconds: number;
  avg_rr_achieved: number;
}

export interface EquityPoint {
  timestamp: string;
  equity: number;
  daily_pnl: number;
}

export interface WsEvent {
  type: string;
  [key: string]: unknown;
}
