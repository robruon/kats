export interface DemoPosition {
    symbol: string;
    direction: "long" | "short";
    quantity: number;
    entry_price: number;
    current_price: number;
    unrealized_pnl: number;
    stop_loss: number;
    take_profit: number;
    broker: string;
}

export interface DemoSigEvent {
    type: "signal";
    data: {
        symbol: string;
        direction: string;
        confidence: number;
        entry_price: number;
        timeframe: string;
        generated_at: string;
    };
}

export interface DemoPosEvent {
    type: "positions";
    data: DemoPosition[];
}

export interface DemoAccountEvent {
    type: "account";
    data: {
        equity: number;
        daily_pnl: number;
        broker: string;
    };
}

export interface DemoOrderEvent {
    type: "order";
    data: {
        symbol: string;
        side: string;
        quantity: number;
        status: string;
    };
}

export interface DemoExitEvent {
    type: "exit";
    data: {
        symbol: string;
        reason: string;
        price: number;
    };
}

export type DemoEvent = DemoSigEvent | DemoPosEvent | DemoAccountEvent | DemoOrderEvent | DemoExitEvent;

const BASE_PRICES: Record<string, number> = {
    BTCUSD: 67234.50,
    ETHUSD: 3456.80,
    EURUSD: 1.0876,
    GBPUSD: 1.2723,
    AUDJPY: 104.58,
    AAPL: 224.15,
    TSLA: 248.70,
    SPY: 541.20,
};

const sigs = [
    { symbol: "BTCUSD", direction: "long",  confidence: 0.82, entry_price: 67100, timeframe: "15m" },
    { symbol: "ETHUSD", direction: "long",  confidence: 0.74, entry_price: 3440,  timeframe: "15m" },
    { symbol: "EURUSD", direction: "short", confidence: 0.69, entry_price: 1.0890, timeframe: "1h" },
    { symbol: "GBPUSD", direction: "long",  confidence: 0.77, entry_price: 1.2700, timeframe: "1h" },
    { symbol: "AUDJPY", direction: "short", confidence: 0.71, entry_price: 105.10, timeframe: "15m" },
    { symbol: "AAPL",   direction: "long",  confidence: 0.65, entry_price: 223.50, timeframe: "5m" },
    { symbol: "TSLA",   direction: "short", confidence: 0.80, entry_price: 250.00, timeframe: "5m" },
    { symbol: "SPY",    direction: "long",  confidence: 0.73, entry_price: 540.80, timeframe: "1h" },
    { symbol: "EURUSD", direction: "long",  confidence: 0.62, entry_price: 1.0860, timeframe: "15m" },
    { symbol: "BTCUSD", direction: "short", confidence: 0.68, entry_price: 67300, timeframe: "1h" },
    { symbol: "ETHUSD", direction: "short", confidence: 0.75, entry_price: 3470,  timeframe: "1h" },
    { symbol: "AUDJPY", direction: "long",  confidence: 0.66, entry_price: 104.20, timeframe: "1h" },
    { symbol: "AAPL",   direction: "short", confidence: 0.70, entry_price: 225.00, timeframe: "15m" },
    { symbol: "GBPUSD", direction: "short", confidence: 0.63, entry_price: 1.2740, timeframe: "15m" },
    { symbol: "TSLA",   direction: "long",  confidence: 0.78, entry_price: 247.20, timeframe: "1h" },
    { symbol: "SPY",    direction: "short", confidence: 0.67, entry_price: 542.50, timeframe: "5m" },
];

const positions: DemoPosition[] = [
    { symbol: "BTCUSD", direction: "long",  quantity: 0.15, entry_price: 67100, current_price: 67234.50, unrealized_pnl: 20.17, stop_loss: 66500, take_profit: 68500, broker: "demo" },
    { symbol: "ETHUSD", direction: "long",  quantity: 1.5,  entry_price: 3440,   current_price: 3456.80,  unrealized_pnl: 25.20, stop_loss: 3400,  take_profit: 3520,  broker: "demo" },
    { symbol: "EURUSD", direction: "short", quantity: 10000, entry_price: 1.0890, current_price: 1.0876,   unrealized_pnl: 14.00, stop_loss: 1.0910, take_profit: 1.0830, broker: "demo" },
    { symbol: "AAPL",   direction: "long",  quantity: 20,    entry_price: 223.50, current_price: 224.15,   unrealized_pnl: 13.00, stop_loss: 221.00, take_profit: 228.00, broker: "demo" },
    { symbol: "TSLA",   direction: "short", quantity: 10,    entry_price: 250.00, current_price: 248.70,   unrealized_pnl: 13.00, stop_loss: 253.00, take_profit: 243.00, broker: "demo" },
];

export { positions, sigs, BASE_PRICES };

export function getDemoLogEntries(): string[] {
    return [
        "SIGNAL BTCUSD LONG @ 67100 conf=82%",
        "ORDER BTCUSD buy qty=0.15 status=filled",
        "SIGNAL ETHUSD LONG @ 3440 conf=74%",
        "ORDER ETHUSD buy qty=1.5 status=filled",
        "SIGNAL EURUSD SHORT @ 1.0890 conf=69%",
        "ORDER EURUSD sell qty=10000 status=filled",
        "EXIT BTCUSD reason=tp price=68500",
        "SIGNAL GBPUSD LONG @ 1.2700 conf=77%",
        "ORDER GBPUSD buy qty=5000 status=pending",
        "RISK REJECT: max_concurrent_positions",
        "SIGNAL AAPL LONG @ 223.50 conf=65%",
        "ORDER AAPL buy qty=20 status=filled",
        "SIGNAL TSLA SHORT @ 250.00 conf=80%",
        "ORDER TSLA sell qty=10 status=filled",
        "SIGNAL SPY LONG @ 540.80 conf=73%",
        "KILL SWITCH disengaged",
        "EXIT ETHUSD reason=tp price=3520",
        "SIGNAL BTCUSD SHORT @ 67300 conf=68%",
        "SIGNAL ETHUSD SHORT @ 3470 conf=75%",
        "EXIT EURUSD reason=sl price=1.0910",
    ];
}