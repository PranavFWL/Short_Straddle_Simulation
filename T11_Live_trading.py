import upstox_client
import requests
import threading
import time
import queue
import pickle
import subprocess
import sys
import os
import json
from datetime import datetime, timezone, timedelta, time as dt_time
from tabulate import tabulate

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────

ENTRY_TIME    = "09:16"
EXIT_TIME     = "15:30"
SL_POINTS     = 1
TARGET_POINTS = 1
LOT_SIZE      = 65
COST_PERCENT  = 0.0025

TOKEN_FILE            = 'upstox_token.txt'
CACHE_FILE            = 'option_chain_cache.pkl'
COLLECTOR_FILE        = 'T10_CSV_Collector.py'
SESSION_STATE         = 'session_state.json'
SESSION_STATE_TIMEOUT = 60
MAX_RECONNECTS        = 10

IST = timezone(timedelta(hours=5, minutes=30))


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def get_ist_time() -> datetime:
    return datetime.now(IST)

def _to_float(v, default=0.0):
    try: return float(v) if v else default
    except: return default

def _parse_time(hhmm: str) -> dt_time:
    h, m = map(int, hhmm.split(':'))
    return dt_time(h, m)

def _leg_cost(entry, exit_price):
    """Transaction cost for one leg."""
    return (entry + exit_price) * COST_PERCENT

def _leg_pnl_time(entry, exit_price):
    """PnL for time exit only — actual prices used."""
    return (entry - exit_price) - _leg_cost(entry, exit_price)


# ─────────────────────────────────────────────────────────────────────────────
# COLLECTOR LAUNCHER
# ─────────────────────────────────────────────────────────────────────────────

def launch_collector() -> subprocess.Popen:
    if not os.path.exists(COLLECTOR_FILE):
        print(f"⚠️  {COLLECTOR_FILE} not found — CSV collection will not run.")
        return None
    env = os.environ.copy()
    env['PYTHONUTF8'] = '1'
    proc = subprocess.Popen(
        [sys.executable, '-X', 'utf8', COLLECTOR_FILE],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        bufsize=1,
        text=True,
        encoding='utf-8',
        env=env,
    )
    def _forward():
        for line in proc.stdout:
            print(f"[T10] {line}", end='')
    threading.Thread(target=_forward, daemon=True, name="CollectorLog").start()
    print(f"🟢 T10_CSV_Collector started (PID {proc.pid})")
    return proc


# ─────────────────────────────────────────────────────────────────────────────
# OPTION STREAMER
# ─────────────────────────────────────────────────────────────────────────────

class OptionStreamer:

    def __init__(self, access_token: str, atm_strike: float,
                 atm_keys: dict, expiry_date: str):
        self.access_token = access_token
        self.atm_strike   = atm_strike
        self.atm_keys     = atm_keys
        self.expiry_date  = expiry_date
        self.streamer     = None
        self.is_connected = False

        self._key_to_type = {v: k for k, v in atm_keys.items()}
        self._latest      = {}
        self._tick_cb     = None
        self._snapshots   = []

    def set_tick_callback(self, cb):
        self._tick_cb = cb

    def setup(self):
        cfg = upstox_client.Configuration()
        cfg.access_token = self.access_token
        self.streamer = upstox_client.MarketDataStreamerV3(
            upstox_client.ApiClient(cfg),
            list(self.atm_keys.values()),
            "full",
        )
        self.streamer.on("message", self._on_message)
        self.streamer.on("open",    lambda: setattr(self, 'is_connected', True))
        self.streamer.on("close",   lambda *a: setattr(self, 'is_connected', False))
        self.streamer.on("error",   lambda e: print(f"❌ Option error: {e}"))

    def connect(self):
        if self.streamer: self.streamer.connect()

    def disconnect(self):
        if self.streamer: self.streamer.disconnect()

    def reset_snapshots(self):
        self._snapshots.clear()

    def _on_message(self, message):
        if not isinstance(message, dict) or 'feeds' not in message:
            return
        ts = get_ist_time()

        for ikey, data in message['feeds'].items():
            otype = self._key_to_type.get(ikey)
            if not otype:
                continue
            ltp = 0.0
            if 'fullFeed' in data and 'marketFF' in data['fullFeed']:
                ltp = _to_float(
                    data['fullFeed']['marketFF'].get('ltpc', {}).get('ltp', 0))
            elif 'ltpc' in data:
                ltp = _to_float(data['ltpc'].get('ltp', 0))
            if ltp > 0:
                self._latest[otype] = ltp

        if len(self._latest) == 2 and self._tick_cb is not None:
            self._tick_cb(self._latest['CALL'], self._latest['PUT'], ts)

    def is_stale(self) -> bool:
        if not self.is_connected or not self._latest:
            self._snapshots.clear()
            return False
        snap = dict(self._latest)
        self._snapshots.append(snap)
        if len(self._snapshots) > 15:
            self._snapshots.pop(0)
        return (len(self._snapshots) == 15 and
                all(s == self._snapshots[0] for s in self._snapshots))


# ─────────────────────────────────────────────────────────────────────────────
# DATA COLLECTOR
# ─────────────────────────────────────────────────────────────────────────────

class DataCollector:

    def __init__(self, access_token: str):
        self.access_token = access_token
        self._stop_event  = threading.Event()
        self._main_thread = None
        self._tick_cb     = None
        self.atm_strike   = None
        self.expiry_date  = None
        self.spot_at_open = None
        self.is_ready     = False

    def set_tick_callback(self, cb):
        self._tick_cb = cb

    def start(self):
        self._main_thread = threading.Thread(
            target=self._run, daemon=True, name="DataCollector")
        self._main_thread.start()

    def stop(self):
        self._stop_event.set()
        if self._main_thread:
            self._main_thread.join(timeout=10)

    def _run(self):
        try:
            self._collect()
        except Exception as e:
            print(f"\n❌ Collector error: {e}")
            import traceback; traceback.print_exc()

    def _collect(self):
        today_str   = get_ist_time().strftime('%Y-%m-%d')
        atm_strike  = None
        expiry_date = None
        atm_keys    = None
        spot_price  = 0.0

        print(f"⏳ Waiting for T10 to lock ATM strike "
              f"(max {SESSION_STATE_TIMEOUT}s)…")

        deadline = time.time() + SESSION_STATE_TIMEOUT
        while time.time() < deadline:
            if self._stop_event.is_set():
                return
            if os.path.exists(SESSION_STATE):
                try:
                    with open(SESSION_STATE) as f:
                        state = json.load(f)
                    if state.get('date') == today_str:
                        atm_strike  = state['atm_strike']
                        expiry_date = state['expiry_date']
                        atm_keys    = {'CALL': state['atm_ce_key'],
                                       'PUT' : state['atm_pe_key']}
                        spot_price  = float(state.get('spot_at_open', 0.0))
                        print(f"✅ Session state loaded from T10:")
                        print(f"   ATM Strike : {atm_strike}")
                        print(f"   Expiry     : {expiry_date}")
                        print(f"   Spot       : {spot_price:.2f}")
                        break
                except Exception:
                    pass
            time.sleep(0.5)

        if atm_strike is None:
            print(f"❌ T10 did not write session state within "
                  f"{SESSION_STATE_TIMEOUT}s. Cannot start strategy.")
            return

        self.atm_strike   = atm_strike
        self.expiry_date  = expiry_date
        self.spot_at_open = spot_price

        print(f"\n🔒 ATM Strike : {atm_strike}  |  Spot: {spot_price:.2f}"
              f"  |  Expiry: {expiry_date}")

        # Connect immediately — no minute-boundary sync needed.
        # Live strategy now works on raw ticks, not candles.
        print(f"✅ Connecting WebSocket now.\n")

        opt_stream = OptionStreamer(self.access_token, atm_strike,
                                    atm_keys, expiry_date)
        opt_stream.set_tick_callback(self._tick_cb)
        opt_stream.setup()
        threading.Thread(target=opt_stream.connect, daemon=True).start()

        opt_reconnects = 0
        self.is_ready  = True

        while not self._stop_event.is_set():
            if opt_stream.is_stale():
                opt_reconnects += 1
                if opt_reconnects > MAX_RECONNECTS:
                    print(f"❌ Max option reconnects ({MAX_RECONNECTS}) reached.")
                    break
                print(f"🔄 Option reconnect #{opt_reconnects}…")
                try: opt_stream.disconnect()
                except Exception: pass
                opt_stream.reset_snapshots()
                if opt_reconnects > 1:
                    wait = min(2 * (2 ** (opt_reconnects - 2)), 60)
                    self._stop_event.wait(wait)
                opt_stream.set_tick_callback(self._tick_cb)
                opt_stream.setup()
                threading.Thread(target=opt_stream.connect, daemon=True).start()
                for _ in range(10):
                    if opt_stream.is_connected or self._stop_event.is_set():
                        break
                    time.sleep(1)
                if opt_stream.is_connected:
                    print("✅ Option connection restored.")
                    opt_reconnects = 0

            self._stop_event.wait(1)

        opt_stream.disconnect()


# ─────────────────────────────────────────────────────────────────────────────
# TRADE SUMMARY
# ─────────────────────────────────────────────────────────────────────────────

def print_trade_summary(completed_trades):
    if not completed_trades:
        print("\nNo completed trades.")
        return

    headers = ["#", "Leg", "Strike", "Entry Time", "Exit Time",
               "Entry ₹", "Exit ₹", "Leg PnL (₹)", "Trade PnL (₹)", "Reason"]
    rows = []
    for i, t in enumerate(completed_trades, start=1):
        rows.append([i, "CE", t['ce_strike'],
                     t['ce_entry_time'], t['ce_exit_time'],
                     f"{t['ce_entry']:.2f}", f"{t['ce_exit']:.2f}",
                     f"{t['ce_pnl_val']:+.2f}", f"{t['trade_pnl_val']:+.2f}",
                     t['exit_reason']])
        rows.append(["", "PE", t['pe_strike'],
                     t['pe_entry_time'], t['pe_exit_time'],
                     f"{t['pe_entry']:.2f}", f"{t['pe_exit']:.2f}",
                     f"{t['pe_pnl_val']:+.2f}", "", ""])

    print("\n" + "=" * 110)
    print(f"  SESSION TRADE SUMMARY  ({len(completed_trades)} completed trades)")
    print("=" * 110)
    for line in tabulate(rows, headers=headers, tablefmt="rounded_outline",
                         stralign="center", numalign="center").split("\n"):
        print(line)

    total  = len(completed_trades)
    wins   = sum(1 for t in completed_trades if t['trade_pnl_val'] > 0)
    losses = total - wins
    pnl    = sum(t['trade_pnl_val'] for t in completed_trades)
    best   = max(completed_trades, key=lambda t: t['trade_pnl_val'])
    worst  = min(completed_trades, key=lambda t: t['trade_pnl_val'])

    print(f"\n  Total Trades  : {total}")
    print(f"  Winners       : {wins}  ({wins/total*100:.1f}%)")
    print(f"  Losers        : {losses}  ({losses/total*100:.1f}%)")
    print(f"  Best Trade    : ₹{best['trade_pnl_val']:+.2f}  (entered {best['ce_entry_time']})")
    print(f"  Worst Trade   : ₹{worst['trade_pnl_val']:+.2f}  (entered {worst['ce_entry_time']})")
    print(f"  Total Net PnL : ₹{pnl:+,.2f}")
    print("=" * 110 + "\n")


# ─────────────────────────────────────────────────────────────────────────────
# LIVE STRATEGY
#
# Tick-based SL/Target monitoring (no candle building for trade logic).
#
# Entry  : First tick at or after ENTRY_TIME (or re-entry minute boundary)
# SL     : ce_ltp >= ce_sl OR pe_ltp >= pe_sl  → close, fixed -SL_POINTS/leg
# Target : (ce_entry - ce_ltp) + (pe_entry - pe_ltp) >= TARGET_POINTS
#          → close, fixed +TARGET_POINTS/leg
# Re-entry: First tick of next minute boundary after SL/Target exit
# Time   : EXIT_TIME → close at actual LTP, real PnL
# ─────────────────────────────────────────────────────────────────────────────

class LiveStrategy:

    def __init__(self):
        self.completed_trades = []
        self.trade_counter    = 0
        self.active           = None
        self.session_done     = False
        self.entry_time       = _parse_time(ENTRY_TIME)
        self.exit_time        = _parse_time(EXIT_TIME)

        self._tick_queue      = queue.Queue()
        self._latest_ce       = 0.0
        self._latest_pe       = 0.0
        self._spot_at_open    = 0.0
        self._atm_strike      = None

        # Re-entry gate: datetime of next minute boundary after exit
        self._reentry_after   = None

    def on_tick(self, ce_ltp: float, pe_ltp: float, ts: datetime):
        self._tick_queue.put((ce_ltp, pe_ltp, ts))

    def _open_trade(self, ce_ltp: float, pe_ltp: float, ts: datetime):
        self.trade_counter += 1
        hhmm   = ts.strftime('%H:%M')
        strike = self._atm_strike
        self.active = {
            'trade_num'     : self.trade_counter,
            'ce_strike'     : strike,
            'pe_strike'     : strike,
            'ce_entry'      : ce_ltp,
            'pe_entry'      : pe_ltp,
            'ce_sl'         : ce_ltp + SL_POINTS,
            'pe_sl'         : pe_ltp + SL_POINTS,
            'ce_entry_time' : hhmm,
            'pe_entry_time' : hhmm,
        }
        print(f"\n  ➤ TRADE #{self.trade_counter} OPEN  [{ts.strftime('%H:%M:%S')}]  "
              f"Strike {strike}  |  "
              f"CE @ {ce_ltp:.2f}  SL {ce_ltp + SL_POINTS:.2f}  |  "
              f"PE @ {pe_ltp:.2f}  SL {pe_ltp + SL_POINTS:.2f}")

    def _close_trade(self, ce_ltp: float, pe_ltp: float,
                     reason: str, ts: datetime):
        """
        Close active trade.
        SL/Target → fixed ±points per leg.
        Time      → actual LTP prices.
        Returns True if re-entry should be scheduled (SL or Target).
        """
        t    = self.active
        self.active = None
        hhmm = ts.strftime('%H:%M')

        if reason == 'Target':
            # Fixed exit prices for record keeping
            ce_exit    = t['ce_entry'] - TARGET_POINTS
            pe_exit    = t['pe_entry'] - TARGET_POINTS
            ce_cost    = _leg_cost(t['ce_entry'], ce_exit)
            pe_cost    = _leg_cost(t['pe_entry'], pe_exit)
            ce_pnl_pts = TARGET_POINTS - ce_cost
            pe_pnl_pts = TARGET_POINTS - pe_cost

        elif reason == 'SL':
            # Fixed exit prices for record keeping
            ce_exit    = t['ce_sl']
            pe_exit    = t['pe_sl']
            ce_cost    = _leg_cost(t['ce_entry'], ce_exit)
            pe_cost    = _leg_cost(t['pe_entry'], pe_exit)
            ce_pnl_pts = -SL_POINTS - ce_cost
            pe_pnl_pts = -SL_POINTS - pe_cost

        else:  # Time exit — actual LTP
            ce_exit    = ce_ltp
            pe_exit    = pe_ltp
            ce_pnl_pts = _leg_pnl_time(t['ce_entry'], ce_exit)
            pe_pnl_pts = _leg_pnl_time(t['pe_entry'], pe_exit)

        ce_pnl_val = ce_pnl_pts * LOT_SIZE
        pe_pnl_val = pe_pnl_pts * LOT_SIZE
        trade_val  = ce_pnl_val + pe_pnl_val

        record = {
            'trade_num'     : t['trade_num'],
            'ce_strike'     : t['ce_strike'],
            'pe_strike'     : t['pe_strike'],
            'ce_entry_time' : t['ce_entry_time'],
            'ce_exit_time'  : hhmm,
            'ce_entry'      : t['ce_entry'],
            'ce_exit'       : ce_exit,
            'ce_pnl_pts'    : ce_pnl_pts,
            'ce_pnl_val'    : ce_pnl_val,
            'pe_entry_time' : t['pe_entry_time'],
            'pe_exit_time'  : hhmm,
            'pe_entry'      : t['pe_entry'],
            'pe_exit'       : pe_exit,
            'pe_pnl_pts'    : pe_pnl_pts,
            'pe_pnl_val'    : pe_pnl_val,
            'trade_pnl_pts' : ce_pnl_pts + pe_pnl_pts,
            'trade_pnl_val' : trade_val,
            'exit_reason'   : reason,
        }
        self.completed_trades.append(record)

        icon = "🎯" if reason == 'Target' else ("🔴" if reason == 'SL' else "🏁")
        note = "fixed pts" if reason in ('SL', 'Target') else "actual LTP"
        print(f"\n  {icon} TRADE #{t['trade_num']} CLOSED  "
              f"[{ts.strftime('%H:%M:%S')}]  {reason}  |  "
              f"CE exit {ce_exit:.2f}  PE exit {pe_exit:.2f}  |  "
              f"Trade PnL: ₹{trade_val:+.2f}  ({note})")

        return reason in ('SL', 'Target')

    def _process_tick(self, ce_ltp: float, pe_ltp: float, ts: datetime):
        tick_time = dt_time(ts.hour, ts.minute)
        in_window = self.entry_time <= tick_time < self.exit_time
        is_exit   = tick_time >= self.exit_time

        if not in_window and not is_exit:
            return

        # ── Time exit ─────────────────────────────────────────────────────────
        if is_exit and self.active is not None:
            self._close_trade(ce_ltp, pe_ltp, 'Time', ts)
            self.session_done = True
            return

        if not in_window:
            return

        # ── Entry / Re-entry ──────────────────────────────────────────────────
        if self.active is None:
            # Re-entry gate: wait for next minute boundary
            if self._reentry_after is not None:
                # Strip tzinfo for naive comparison
                ts_naive = ts.replace(tzinfo=None)
                if ts_naive < self._reentry_after:
                    return
            self._open_trade(ce_ltp, pe_ltp, ts)
            return  # don't check SL/Target on entry tick

        # ── SL check (tick-level) ─────────────────────────────────────────────
        t      = self.active
        sl_hit = (ce_ltp >= t['ce_sl'] or pe_ltp >= t['pe_sl'])

        # ── Target check (tick-level) ─────────────────────────────────────────
        combined_drop = (t['ce_entry'] - ce_ltp) + (t['pe_entry'] - pe_ltp)
        target_hit    = combined_drop >= TARGET_POINTS

        if sl_hit and target_hit:
            # Conflict on same tick — SL takes priority (conservative)
            reason = 'SL'
            print(f"  ⚡ Tick conflict [{ts.strftime('%H:%M:%S')}] "
                  f"SL and Target both hit → SL wins (conservative)")
        elif sl_hit:
            reason = 'SL'
        elif target_hit:
            reason = 'Target'
        else:
            return  # neither hit

        needs_reentry = self._close_trade(ce_ltp, pe_ltp, reason, ts)
        if needs_reentry:
            # Re-entry allowed from start of next minute boundary
            ts_naive = ts.replace(tzinfo=None)
            next_minute = (ts_naive + timedelta(minutes=1)).replace(
                second=0, microsecond=0)
            self._reentry_after = next_minute
            print(f"  ⏳ Re-entry allowed from {next_minute.strftime('%H:%M:%S')}")

    def _print_status(self):
        now        = get_ist_time().strftime('%H:%M:%S')
        closed_pnl = sum(t['trade_pnl_val'] for t in self.completed_trades)
        ce_ltp     = self._latest_ce
        pe_ltp     = self._latest_pe

        if self.active:
            t             = self.active
            combined_drop = (t['ce_entry'] - ce_ltp) + (t['pe_entry'] - pe_ltp)
            # Unrealised display uses actual LTP difference
            open_pnl      = combined_drop * LOT_SIZE
            total_pnl     = closed_pnl + open_pnl
            pos_str       = (f"Trade#{t['trade_num']} ACTIVE  "
                             f"CE {t['ce_strike']} LTP {ce_ltp:.2f} "
                             f"SL {t['ce_sl']:.2f}  |  "
                             f"PE {t['pe_strike']} LTP {pe_ltp:.2f} "
                             f"SL {t['pe_sl']:.2f}  |  "
                             f"Unrealised ₹{open_pnl:+.2f}")
        else:
            total_pnl = closed_pnl
            pos_str   = "No active position"

        print(f"  [{now}]  ATM {self._atm_strike}  |  "
              f"Trades {len(self.completed_trades)}  |  "
              f"Session PnL ₹{total_pnl:+.2f}  |  {pos_str}")

    def run(self, collector: DataCollector, collector_proc: subprocess.Popen):
        print("\n" + "=" * 65)
        print("  🚀  LIVE SHORT STRADDLE  (tick-based SL/Target)")
        print(f"  Entry  : {ENTRY_TIME}    Exit   : {EXIT_TIME}")
        print(f"  SL     : {SL_POINTS} pts/leg (fixed)    "
              f"Target : {TARGET_POINTS} pts combined (fixed)")
        print(f"  Lot    : {LOT_SIZE}")
        print("=" * 65)

        collector.set_tick_callback(self.on_tick)
        collector.start()

        print("\n⏳ Waiting for market data stream…")
        while not collector.is_ready:
            time.sleep(0.5)

        self._atm_strike   = collector.atm_strike
        self._spot_at_open = collector.spot_at_open
        print(f"✅ Stream active. ATM Strike: {self._atm_strike}. "
              f"Strategy running.\n")

        last_status_minute = None

        try:
            while not self.session_done:
                now      = get_ist_time()
                now_hhmm = now.strftime('%H:%M')

                if now.hour > 15 or (now.hour == 15 and now.minute >= 31):
                    print("\n🏁 Session complete.")
                    break

                # Drain tick queue → process each tick directly
                try:
                    while True:
                        ce_ltp, pe_ltp, ts = self._tick_queue.get_nowait()
                        self._latest_ce = ce_ltp
                        self._latest_pe = pe_ltp
                        self._process_tick(ce_ltp, pe_ltp, ts)
                        if self.session_done:
                            break
                except queue.Empty:
                    pass

                if self.session_done:
                    break

                # Print status once per minute
                if now_hhmm != last_status_minute:
                    last_status_minute = now_hhmm
                    self._print_status()

                time.sleep(0.05)  # tighter loop for tick responsiveness

        except KeyboardInterrupt:
            print("\n\n👋 Strategy stopped.")
        except Exception as e:
            print(f"\n❌ Strategy error: {e}")
            import traceback; traceback.print_exc()
        finally:
            collector.stop()
            if collector_proc and collector_proc.poll() is None:
                collector_proc.terminate()
                try:
                    collector_proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    collector_proc.kill()
                print("🛑 T10_CSV_Collector stopped.")
            print_trade_summary(self.completed_trades)


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    try:
        with open(TOKEN_FILE) as f:
            access_token = f.read().strip()
        if not access_token:
            raise ValueError("Empty token")
    except FileNotFoundError:
        print(f"❌ {TOKEN_FILE} not found.")
        raise SystemExit(1)
    except Exception as e:
        print(f"❌ Token error: {e}")
        raise SystemExit(1)

    if os.path.exists(SESSION_STATE):
        os.remove(SESSION_STATE)
        print(f"🗑️  Cleared stale {SESSION_STATE} — waiting for fresh write from T10.")

    collector_proc = launch_collector()
    collector      = DataCollector(access_token)
    strategy       = LiveStrategy()
    strategy.run(collector, collector_proc)