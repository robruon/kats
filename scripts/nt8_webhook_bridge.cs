// ============================================================
// KronosTrade — NT8 WebhookBridge NinjaScript Strategy
// File: scripts/nt8_webhook_bridge.cs
//
// INSTALL:
//   1. Copy this file to:
//      Documents\NinjaTrader 8\bin\Custom\Strategies\
//   2. In NinjaTrader: Tools > Edit NinjaScript > Compile
//   3. Add "WebhookBridge" strategy to a chart (any instrument)
//   4. Set Parameters: Account, ListenPort (default 8080)
//
// The Python side (NinjaTraderAdapter) POSTs JSON to:
//   POST http://localhost:8080/command
//   GET  http://localhost:8080/ping
//   GET  http://localhost:8080/positions?account=...
//   GET  http://localhost:8080/account?id=...
// ============================================================

#region Using declarations
using System;
using System.Collections.Generic;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using NinjaTrader.Cbi;
using NinjaTrader.NinjaScript;
using NinjaTrader.NinjaScript.Strategies;
#endregion

namespace NinjaTrader.NinjaScript.Strategies
{
    public class WebhookBridge : Strategy
    {
        private HttpListener _listener;
        private Thread       _listenerThread;
        private bool         _running;

        #region Parameters
        [NinjaScriptProperty]
        public string AccountId { get; set; } = "Sim101";

        [NinjaScriptProperty]
        public int ListenPort { get; set; } = 8080;
        #endregion

        protected override void OnStateChange()
        {
            if (State == State.SetDefaults)
            {
                Name        = "WebhookBridge";
                Description = "KronosTrade HTTP bridge — receives commands from Python";
                Calculate   = Calculate.OnBarClose;
                IsOverlay   = true;
            }
            else if (State == State.DataLoaded)
            {
                StartListener();
            }
            else if (State == State.Terminated)
            {
                StopListener();
            }
        }

        protected override void OnBarUpdate() { }   // no chart trading logic

        // ── HTTP listener ─────────────────────────────────────────────────────

        private void StartListener()
        {
            _listener = new HttpListener();
            _listener.Prefixes.Add($"http://localhost:{ListenPort}/");
            _listener.Start();
            _running = true;
            _listenerThread = new Thread(ListenLoop) { IsBackground = true };
            _listenerThread.Start();
            Print($"[WebhookBridge] listening on port {ListenPort}");
        }

        private void StopListener()
        {
            _running = false;
            _listener?.Stop();
        }

        private void ListenLoop()
        {
            while (_running)
            {
                try
                {
                    var ctx = _listener.GetContext();
                    Task.Run(() => HandleRequest(ctx));
                }
                catch (HttpListenerException) { break; }
                catch (Exception ex) { Print($"[WebhookBridge] listener error: {ex.Message}"); }
            }
        }

        private void HandleRequest(HttpListenerContext ctx)
        {
            var req  = ctx.Request;
            var resp = ctx.Response;
            resp.ContentType = "application/json";

            try
            {
                string body  = "";
                string path  = req.Url.AbsolutePath.ToLower();
                string method = req.HttpMethod.ToUpper();

                if (path == "/ping")
                {
                    WriteJson(resp, new { status = "ok", bridge = "WebhookBridge" });
                    return;
                }

                if (path == "/command" && method == "POST")
                {
                    body = new System.IO.StreamReader(req.InputStream).ReadToEnd();
                    var cmd = JObject.Parse(body);
                    HandleCommand(cmd, resp);
                    return;
                }

                if (path == "/positions" && method == "GET")
                {
                    HandleGetPositions(resp);
                    return;
                }

                if (path == "/account" && method == "GET")
                {
                    HandleGetAccount(resp);
                    return;
                }

                resp.StatusCode = 404;
                WriteJson(resp, new { error = "not found" });
            }
            catch (Exception ex)
            {
                resp.StatusCode = 500;
                WriteJson(resp, new { error = ex.Message });
            }
        }

        // ── Command handler ───────────────────────────────────────────────────

        private void HandleCommand(JObject cmd, HttpListenerResponse resp)
        {
            string action = cmd["action"]?.ToString() ?? "";
            string symbol = cmd["symbol"]?.ToString() ?? "";
            double qty    = cmd["qty"]?.ToObject<double>() ?? 0;
            double stop   = cmd["stop"]?.ToObject<double>() ?? 0;
            double target = cmd["target"]?.ToObject<double>() ?? 0;

            string orderId = $"KT-{DateTime.UtcNow:yyyyMMddHHmmss}-{symbol}";

            switch (action.ToUpper())
            {
                case "ENTER_LONG":
                    EnterLong((int)qty, symbol);
                    if (stop   > 0) SetStopLoss(symbol, CalculationMode.Price, stop, false);
                    if (target > 0) SetProfitTarget(symbol, CalculationMode.Price, target);
                    Print($"[WebhookBridge] LONG {symbol} qty={qty} sl={stop} tp={target}");
                    WriteJson(resp, new { order_id = orderId, status = "submitted" });
                    break;

                case "ENTER_SHORT":
                    EnterShort((int)qty, symbol);
                    if (stop   > 0) SetStopLoss(symbol, CalculationMode.Price, stop, false);
                    if (target > 0) SetProfitTarget(symbol, CalculationMode.Price, target);
                    Print($"[WebhookBridge] SHORT {symbol} qty={qty} sl={stop} tp={target}");
                    WriteJson(resp, new { order_id = orderId, status = "submitted" });
                    break;

                case "EXIT":
                    ExitLong(symbol);
                    ExitShort(symbol);
                    Print($"[WebhookBridge] EXIT {symbol}");
                    WriteJson(resp, new { order_id = orderId, status = "exit_submitted" });
                    break;

                default:
                    resp.StatusCode = 400;
                    WriteJson(resp, new { error = $"unknown action: {action}" });
                    break;
            }
        }

        // ── Account / position queries ────────────────────────────────────────

        private void HandleGetPositions(HttpListenerResponse resp)
        {
            var positions = new List<object>();
            var acct = Account.All.Find(a => a.Name == AccountId) ?? Account.All[0];

            foreach (var pos in acct.Positions)
            {
                positions.Add(new {
                    instrument      = pos.Instrument.FullName,
                    quantity        = pos.Quantity,
                    market_position = pos.MarketPosition.ToString(),
                    average_price   = pos.AveragePrice,
                    last_price      = pos.Instrument.MasterInstrument.LastTick?.Last ?? 0.0,
                    unrealized_pnl  = pos.GetUnrealizedProfitLoss(PerformanceUnit.Currency, 0),
                });
            }
            WriteJson(resp, positions);
        }

        private void HandleGetAccount(HttpListenerResponse resp)
        {
            var acct = Account.All.Find(a => a.Name == AccountId) ?? Account.All[0];
            WriteJson(resp, new {
                id            = acct.Name,
                equity        = acct.Get(AccountItem.NetLiquidation, Currency.UsDollar),
                cash_value    = acct.Get(AccountItem.CashValue, Currency.UsDollar),
                buying_power  = acct.Get(AccountItem.BuyingPower, Currency.UsDollar),
                today_pnl     = acct.Get(AccountItem.RealizedProfitLoss, Currency.UsDollar),
            });
        }

        // ── Utility ───────────────────────────────────────────────────────────

        private static void WriteJson(HttpListenerResponse resp, object data)
        {
            byte[] buf = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(data));
            resp.ContentLength64 = buf.Length;
            resp.OutputStream.Write(buf, 0, buf.Length);
            resp.OutputStream.Close();
        }
    }
}
