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
SL_POINTS     = 10
TARGET_POINTS = 10
LOT_SIZE      = 65
COST_PERCENT  = 0.0025

TOKEN_FILE            = 'upstox_token.txt'
CACHE_FILE            = 'option_chain_cache.pkl'
COLLECTOR_FILE        = 'T10_CSV_Collector.py'  # launched as subprocess at startup
SESSION_STATE         = 'session_state.json'    # written by T10 after ATM lock
SESSION_STATE_TIMEOUT = 60                      # max seconds to wait for T10's ATM lock
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

def _leg_pnl(entry, exit_price):
    cost = (entry + exit_price) * COST_PERCENT
    return (entry - exit_price) - cost


# ─────────────────────────────────────────────────────────────────────────────
# COLLECTOR LAUNCHER
# Starts T10_CSV_Collector.py as a background subprocess.
# Returns the Popen handle so it can be terminated on exit.
# ─────────────────────────────────────────────────────────────────────────────

def launch_collector() -> subprocess.Popen:
    if not os.path.exists(COLLECTOR_FILE):
        print(f"⚠️  {COLLECTOR_FILE} not found — CSV collection will not run.")
        return None
    env = os.environ.copy()
    env['PYTHONUTF8'] = '1'          # force UTF-8 on Windows (fixes emoji/arrow encode errors)
    proc = subprocess.Popen(
        [sys.executable, '-X', 'utf8', COLLECTOR_FILE],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        bufsize=1,
        text=True,
        encoding='utf-8',
        env=env,
    )
    # Forward collector output to console in a daemon thread
    def _forward():
        for line in proc.stdout:
            print(f"[T10] {line}", end='')
    threading.Thread(target=_forward, daemon=True, name="CollectorLog").start()
    print(f"🟢 T10_CSV_Collector started (PID {proc.pid})")
    return proc


# ─────────────────────────────────────────────────────────────────────────────
# LIVE CANDLE BUILDER
#
# KEY DESIGN — prev-close-as-open:
#   When a minute rolls over, the new candle OPEN = previous candle CLOSE,
#   not the first WebSocket tick of the new minute.
#
#   Why: The first tick of a new minute may arrive late or be stale.
#   The previous candle's close is a confirmed last-traded price, matching
#   what T10_CSV_Collector records as the candle open.
#   This makes entry price and SL levels stable and consistent with backtest.
# ─────────────────────────────────────────────────────────────────────────────

class LiveCandleBuilder:
    """
    Accumulates (ce_ltp, pe_ltp) ticks into 1-minute OHLC candles.
    Returns a completed candle dict when the minute rolls over.

    Fields: minute_dt, ce_open/high/low/close, pe_open/high/low/close
    """

    def __init__(self):
        self._lock   = threading.Lock()
        self._minute = None
        self._ce     = None
        self._pe     = None

    def tick(self, ce_ltp: float, pe_ltp: float, ts: datetime):
        candle_min = ts.replace(second=0, microsecond=0, tzinfo=None)

        with self._lock:
            if self._minute is None:
                self._open_candle(candle_min, ce_ltp, pe_ltp)
                return None

            if candle_min > self._minute:
                ce_prev_close = self._ce['close']
                pe_prev_close = self._pe['close']

                sealed = {
                    'minute_dt': self._minute,
                    'ce_open'  : self._ce['open'],
                    'ce_high'  : self._ce['high'],
                    'ce_low'   : self._ce['low'],
                    'ce_close' : ce_prev_close,
                    'pe_open'  : self._pe['open'],
                    'pe_high'  : self._pe['high'],
                    'pe_low'   : self._pe['low'],
                    'pe_close' : pe_prev_close,
                }
                # New candle opens at prev close — not at incoming ltp
                self._open_candle(candle_min, ce_prev_close, pe_prev_close)
                # Incoming tick updates high/low/close only
                self._ce['high']  = max(self._ce['high'],  ce_ltp)
                self._ce['low']   = min(self._ce['low'],   ce_ltp)
                self._ce['close'] = ce_ltp
                self._pe['high']  = max(self._pe['high'],  pe_ltp)
                self._pe['low']   = min(self._pe['low'],   pe_ltp)
                self._pe['close'] = pe_ltp
                return sealed

            self._ce['high']  = max(self._ce['high'],  ce_ltp)
            self._ce['low']   = min(self._ce['low'],   ce_ltp)
            self._ce['close'] = ce_ltp
            self._pe['high']  = max(self._pe['high'],  pe_ltp)
            self._pe['low']   = min(self._pe['low'],   pe_ltp)
            self._pe['close'] = pe_ltp
            return None

    def _open_candle(self, candle_min, ce_open, pe_open):
        self._minute = candle_min
        self._ce = {'open': ce_open, 'high': ce_open, 'low': ce_open, 'close': ce_open}
        self._pe = {'open': pe_open, 'high': pe_open, 'low': pe_open, 'close': pe_open}


# ─────────────────────────────────────────────────────────────────────────────
# OPTION CHAIN
# ─────────────────────────────────────────────────────────────────────────────

class UpstoxOptionChain:

    def __init__(self, access_token):
        self.access_token = access_token
        self.base_url     = "https://api.upstox.com/v2"
        self.headers = {
            'Content-Type' : 'application/json',
            'Accept'       : 'application/json',
            'Authorization': f'Bearer {access_token}',
        }

    def get_option_chain(self, instrument_key="NSE_INDEX|Nifty 50", expiry_date=None):
        if not expiry_date:
            expiry_date = self._get_nearest_expiry(instrument_key)
        resp = requests.get(
            f"{self.base_url}/option/chain",
            params={'instrument_key': instrument_key, 'expiry_date': expiry_date},
            headers=self.headers,
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get('status') == 'success':
            return data.get('data', []), expiry_date
        raise Exception(f"API Error: {data}")

    def _get_nearest_expiry(self, instrument_key):
        resp = requests.get(
            f"{self.base_url}/option/contract",
            params={'instrument_key': instrument_key},
            headers=self.headers,
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get('status') == 'success':
            contracts = data.get('data', [])
            if contracts:
                return sorted(set(c['expiry'] for c in contracts))[0]
        raise Exception("Could not determine nearest expiry")

    def get_atm_strike(self, chain_data):
        spot  = chain_data[0].get('underlying_spot_price', 0)
        all_k = sorted(e['strike_price'] for e in chain_data)
        atm   = min(all_k, key=lambda x: abs(x - spot))
        return atm, spot

    def get_atm_instrument_keys(self, chain_data, atm_strike):
        keys = {}
        for entry in chain_data:
            if entry['strike_price'] != atm_strike:
                continue
            ce_key = entry.get('call_options', {}).get('instrument_key')
            pe_key = entry.get('put_options',  {}).get('instrument_key')
            if ce_key: keys['CALL'] = ce_key
            if pe_key: keys['PUT']  = pe_key
        if len(keys) < 2:
            raise Exception(f"Could not find CE/PE keys for ATM {atm_strike}")
        return keys


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
        # ── Wait for T10_CSV_Collector to lock the ATM strike ─────────────────
        # T10 writes session_state.json within seconds of startup (immediately
        # after its own REST call completes). We wait for that file rather than
        # making an independent REST call — this guarantees both systems use the
        # exact same strike, expiry, and instrument keys.
        today_str  = get_ist_time().strftime('%Y-%m-%d')
        atm_strike = None
        expiry_date = None
        atm_keys   = None
        spot_price = 0.0

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
                except Exception as e:
                    pass   # file may be mid-write, retry next loop
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

        # Wait until the next full minute boundary before connecting.
        # This ensures T10's WebSocket is fully up and its first candle has
        # sealed before live strategy processes any candles — eliminating the
        # 1-candle head-start that caused live to enter one minute early.
        now         = get_ist_time()
        next_minute = (now + timedelta(minutes=1)).replace(second=0, microsecond=0)
        wait_secs   = (next_minute - now).total_seconds()
        print(f"⏳ Syncing to next minute boundary "
              f"({next_minute.strftime('%H:%M:%S')} IST) — "
              f"waiting {wait_secs:.1f}s…")
        self._stop_event.wait(wait_secs)
        if self._stop_event.is_set():
            return
        print(f"✅ Synced. Connecting WebSocket now.\n")

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
               "Entry ₹", "Exit ₹", "Leg PnL (₹)", "Trade PnL (₹)"]
    rows = []
    for i, t in enumerate(completed_trades, start=1):
        rows.append([i, "CE", t['ce_strike'],
                     t['ce_entry_time'], t['ce_exit_time'],
                     f"{t['ce_entry']:.2f}", f"{t['ce_exit']:.2f}",
                     f"{t['ce_pnl_val']:+.2f}", f"{t['trade_pnl_val']:+.2f}"])
        rows.append(["", "PE", t['pe_strike'],
                     t['pe_entry_time'], t['pe_exit_time'],
                     f"{t['pe_entry']:.2f}", f"{t['pe_exit']:.2f}",
                     f"{t['pe_pnl_val']:+.2f}", ""])

    print("\n" + "=" * 100)
    print(f"  SESSION TRADE SUMMARY  ({len(completed_trades)} completed trades)")
    print("=" * 100)
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
    print("=" * 100 + "\n")


# ─────────────────────────────────────────────────────────────────────────────
# LIVE STRATEGY
#
# Receives ticks from DataCollector, builds 1-min candles via LiveCandleBuilder,
# and applies strategy logic once per completed candle — identical to backtest.
#
#   Entry  : candle open (prev-close-as-open) of first eligible minute
#   Target : (ce_entry - ce_low) + (pe_entry - pe_low) >= TARGET_POINTS
#            → exit at ce_low / pe_low  (exact, no slippage)
#   SL     : ce_high >= ce_sl OR pe_high >= pe_sl
#            → exit at ce_sl / pe_sl   (exact, no slippage)
#   Conflict: open-proximity heuristic (identical to backtest)
#   Re-entry: start of next minute after SL/Target exit
#   Time   : EXIT_TIME candle close
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
        self._reentry_after   = None
        self._candle_builder  = LiveCandleBuilder()

    def on_tick(self, ce_ltp: float, pe_ltp: float, ts: datetime):
        self._tick_queue.put((ce_ltp, pe_ltp, ts))

    def _open_trade(self, ce_open: float, pe_open: float, minute_dt: datetime):
        self.trade_counter += 1
        hhmm   = minute_dt.strftime('%H:%M')
        strike = self._atm_strike
        self.active = {
            'trade_num'     : self.trade_counter,
            'ce_strike'     : strike,  'pe_strike'     : strike,
            'ce_entry'      : ce_open, 'pe_entry'      : pe_open,
            'ce_sl'         : ce_open + SL_POINTS,
            'pe_sl'         : pe_open + SL_POINTS,
            'ce_entry_time' : hhmm,    'pe_entry_time' : hhmm,
        }
        print(f"\n  ➤ TRADE #{self.trade_counter} OPEN  [{hhmm}]  Strike {strike}  |  "
              f"CE @ {ce_open:.2f}  SL {ce_open + SL_POINTS:.2f}  |  "
              f"PE @ {pe_open:.2f}  SL {pe_open + SL_POINTS:.2f}")

    def _close_trade(self, ce_exit: float, pe_exit: float,
                     reason: str, minute_dt: datetime):
        t    = self.active
        self.active = None
        hhmm = minute_dt.strftime('%H:%M')

        ce_pnl_pts = _leg_pnl(t['ce_entry'], ce_exit)
        pe_pnl_pts = _leg_pnl(t['pe_entry'], pe_exit)
        ce_pnl_val = ce_pnl_pts * LOT_SIZE
        pe_pnl_val = pe_pnl_pts * LOT_SIZE
        trade_val  = ce_pnl_val + pe_pnl_val

        record = {
            'trade_num'     : t['trade_num'],
            'ce_strike'     : t['ce_strike'],    'pe_strike'     : t['pe_strike'],
            'ce_entry_time' : t['ce_entry_time'],'ce_exit_time'  : hhmm,
            'ce_entry'      : t['ce_entry'],     'ce_exit'       : ce_exit,
            'ce_pnl_pts'    : ce_pnl_pts,        'ce_pnl_val'    : ce_pnl_val,
            'pe_entry_time' : t['pe_entry_time'],'pe_exit_time'  : hhmm,
            'pe_entry'      : t['pe_entry'],     'pe_exit'       : pe_exit,
            'pe_pnl_pts'    : pe_pnl_pts,        'pe_pnl_val'    : pe_pnl_val,
            'trade_pnl_pts' : ce_pnl_pts + pe_pnl_pts,
            'trade_pnl_val' : trade_val,
            'exit_reason'   : reason,
        }
        self.completed_trades.append(record)
        icon = "🎯" if reason == 'Target' else ("🔴" if reason == 'SL' else "🏁")
        print(f"\n  {icon} TRADE #{t['trade_num']} CLOSED  [{hhmm}]  {reason}  |  "
              f"CE exit {ce_exit:.2f}  PE exit {pe_exit:.2f}  |  "
              f"Trade PnL: ₹{trade_val:+.2f}")
        return reason in ('SL', 'Target')

    def _process_candle(self, candle: dict):
        minute_dt = candle['minute_dt']
        hhmm      = minute_dt.strftime('%H:%M')
        tick_time = dt_time(minute_dt.hour, minute_dt.minute)
        in_window = self.entry_time <= tick_time < self.exit_time
        is_exit   = tick_time >= self.exit_time

        if not in_window and not is_exit:
            return

        ce_open  = candle['ce_open']
        ce_high  = candle['ce_high']
        ce_low   = candle['ce_low']
        ce_close = candle['ce_close']
        pe_open  = candle['pe_open']
        pe_high  = candle['pe_high']
        pe_low   = candle['pe_low']
        pe_close = candle['pe_close']

        # ── Entry ──────────────────────────────────────────────────────────────
        if self.active is None and in_window:
            if self._reentry_after and minute_dt < self._reentry_after:
                return
            self._open_trade(ce_open, pe_open, minute_dt)
            return   # skip SL/Target on entry candle — same as backtest

        if self.active is None:
            return

        t           = self.active
        exit_reason = None
        ce_exit_px  = None
        pe_exit_px  = None
        do_reenter  = False

        # ── Time exit ──────────────────────────────────────────────────────────
        if is_exit:
            exit_reason = 'Time'
            ce_exit_px  = ce_close
            pe_exit_px  = pe_close

        else:
            target_pts = (t['ce_entry'] - ce_low) + (t['pe_entry'] - pe_low)
            target_hit = target_pts >= TARGET_POINTS
            sl_hit     = (ce_high >= t['ce_sl'] or pe_high >= t['pe_sl'])

            if target_hit and sl_hit:
                # Open-proximity conflict heuristic — identical to backtest
                sl_dists = []
                if ce_high >= t['ce_sl']:
                    sl_dists.append(t['ce_sl'] - ce_open)
                if pe_high >= t['pe_sl']:
                    sl_dists.append(t['pe_sl'] - pe_open)
                dist_to_sl     = min(sl_dists)
                gain_at_open   = (t['ce_entry'] - ce_open) + (t['pe_entry'] - pe_open)
                dist_to_target = max(0.0, TARGET_POINTS - gain_at_open)

                if dist_to_sl <= dist_to_target:
                    exit_reason = 'SL'
                    ce_exit_px  = t['ce_sl']
                    pe_exit_px  = t['pe_sl']
                else:
                    exit_reason = 'Target'
                    ce_exit_px  = ce_low
                    pe_exit_px  = pe_low

                print(f"  ⚡ Conflict [{hhmm}] dist_to_sl={dist_to_sl:.2f}  "
                      f"dist_to_target={dist_to_target:.2f}  → {exit_reason} wins")
                do_reenter = True

            elif target_hit:
                exit_reason = 'Target'
                ce_exit_px  = ce_low
                pe_exit_px  = pe_low
                do_reenter  = True

            elif sl_hit:
                exit_reason = 'SL'
                ce_exit_px  = t['ce_sl']
                pe_exit_px  = t['pe_sl']
                do_reenter  = True

        if exit_reason:
            needs_reentry = self._close_trade(ce_exit_px, pe_exit_px,
                                              exit_reason, minute_dt)
            if needs_reentry:
                self._reentry_after = minute_dt + timedelta(minutes=1)
            if not do_reenter:
                self.session_done = True

    def _print_status(self):
        now        = get_ist_time().strftime('%H:%M:%S')
        closed_pnl = sum(t['trade_pnl_val'] for t in self.completed_trades)
        ce_ltp     = self._latest_ce
        pe_ltp     = self._latest_pe

        if self.active:
            t         = self.active
            open_pnl  = (_leg_pnl(t['ce_entry'], ce_ltp or t['ce_entry']) +
                         _leg_pnl(t['pe_entry'], pe_ltp or t['pe_entry'])) * LOT_SIZE
            total_pnl = closed_pnl + open_pnl
            pos_str   = (f"Trade#{t['trade_num']} ACTIVE  "
                         f"CE {t['ce_strike']} LTP {ce_ltp:.2f}  "
                         f"PE {t['pe_strike']} LTP {pe_ltp:.2f}  "
                         f"Open PnL ₹{open_pnl:+.2f}")
        else:
            total_pnl = closed_pnl
            pos_str   = "No active position"

        print(f"  [{now}]  ATM {self._atm_strike}  |  "
              f"Trades {len(self.completed_trades)}  |  "
              f"Session PnL ₹{total_pnl:+.2f}  |  {pos_str}")

    def run(self, collector: DataCollector, collector_proc: subprocess.Popen):
        print("\n" + "=" * 65)
        print("  🚀  LIVE SHORT STRADDLE  (candle-aligned, backtest-matched)")
        print(f"  Entry  : {ENTRY_TIME}    Exit   : {EXIT_TIME}")
        print(f"  SL     : {SL_POINTS} pts/leg    Target : {TARGET_POINTS} pts combined")
        print(f"  Lot    : {LOT_SIZE}")
        print("=" * 65)

        collector.set_tick_callback(self.on_tick)
        collector.start()

        print("\n⏳ Waiting for market data stream…")
        while not collector.is_ready:
            time.sleep(0.5)

        self._atm_strike   = collector.atm_strike
        self._spot_at_open = collector.spot_at_open
        print(f"✅ Stream active. ATM Strike: {self._atm_strike}. Strategy running.\n")

        last_status_minute = None

        try:
            while not self.session_done:
                now      = get_ist_time()
                now_hhmm = now.strftime('%H:%M')

                if now.hour > 15 or (now.hour == 15 and now.minute >= 31):
                    print("\n🏁 Session complete.")
                    break

                # Drain tick queue → feed into candle builder
                try:
                    while True:
                        ce_ltp, pe_ltp, ts = self._tick_queue.get_nowait()
                        self._latest_ce = ce_ltp
                        self._latest_pe = pe_ltp
                        completed = self._candle_builder.tick(ce_ltp, pe_ltp, ts)
                        if completed:
                            self._process_candle(completed)
                        if self.session_done:
                            break
                except queue.Empty:
                    pass

                if self.session_done:
                    break

                if now_hhmm != last_status_minute:
                    last_status_minute = now_hhmm
                    self._print_status()

                time.sleep(0.1)

        except KeyboardInterrupt:
            print("\n\n👋 Strategy stopped.")
        except Exception as e:
            print(f"\n❌ Strategy error: {e}")
            import traceback; traceback.print_exc()
        finally:
            collector.stop()
            # Terminate T10_CSV_Collector subprocess
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

    # Launch T10_CSV_Collector as background subprocess — runs independently,
    # writes nifty_YYYY-MM-DD.csv for backtest use
    collector_proc = launch_collector()

    collector = DataCollector(access_token)
    strategy  = LiveStrategy()
    strategy.run(collector, collector_proc)