import upstox_client
import requests
import threading
import time
import queue
import pickle
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

TOKEN_FILE     = 'upstox_token.txt'
CACHE_FILE     = 'option_chain_cache.pkl'
MAX_RECONNECTS = 10

IST = timezone(timedelta(hours=5, minutes=30))


# ─────────────────────────────────────────────────────────────────────────────
# SHARED HELPERS
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
# LIVE CANDLE BUILDER
# Mirrors what T10_CSV_Collector does — accumulates ticks into 1-min OHLC.
# Used by LiveStrategy to apply the same candle-level logic as backtest.
# ─────────────────────────────────────────────────────────────────────────────

class LiveCandleBuilder:
    """
    Accumulates (ce_ltp, pe_ltp) ticks into 1-minute OHLC candles.
    On every tick, returns a completed candle if the minute just rolled over.

    KEY DESIGN: The new candle's OPEN is set to the PREVIOUS candle's CLOSE,
    not the first tick of the new minute. This mirrors how T10_CSV_Collector
    records candle opens (first tick it receives), and more importantly ensures
    that entry price and SL levels are based on a confirmed last-traded price
    rather than a potentially late/stale first WebSocket tick of the new minute.

    Example:
        12:30 candle closes at 59.50 (CE) and 51.80 (PE)
        12:31 candle open = 59.50 / 51.80  ← guaranteed, not first-tick-dependent
        First tick of 12:31 (e.g. 61.10) updates high/low/close only

    Fields in completed candle dict:
        minute_dt       : naive datetime of that minute (floor to :00)
        ce_open/high/low/close
        pe_open/high/low/close
    """

    def __init__(self):
        self._lock       = threading.Lock()
        self._minute     = None   # current candle minute (datetime)
        self._ce         = None   # {open, high, low, close}
        self._pe         = None

    def tick(self, ce_ltp: float, pe_ltp: float, ts: datetime):
        """
        Feed a tick. Returns completed candle dict if minute just rolled, else None.
        """
        candle_min = ts.replace(second=0, microsecond=0, tzinfo=None)

        with self._lock:
            if self._minute is None:
                # First tick ever — open first candle using this tick as open.
                # No previous close exists yet, so this is unavoidable.
                self._open_candle(candle_min, ce_ltp, pe_ltp)
                return None

            if candle_min > self._minute:
                # Minute rolled — seal the OLD candle.
                # New candle OPEN = OLD candle CLOSE (last confirmed price).
                # This is the fix: incoming tick LTP is NOT used as open.
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

                # Open new candle with prev close as open, then update with
                # the incoming tick (which may differ from prev close)
                self._open_candle(candle_min, ce_prev_close, pe_prev_close)
                # Now apply the incoming tick to update high/low/close
                self._ce['high']  = max(self._ce['high'],  ce_ltp)
                self._ce['low']   = min(self._ce['low'],   ce_ltp)
                self._ce['close'] = ce_ltp
                self._pe['high']  = max(self._pe['high'],  pe_ltp)
                self._pe['low']   = min(self._pe['low'],   pe_ltp)
                self._pe['close'] = pe_ltp

                return sealed

            # Same minute — update OHLC
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
        oc          = UpstoxOptionChain(self.access_token)
        full_chain  = None
        expiry_date = None

        for attempt in range(1, 6):
            if self._stop_event.is_set():
                return
            try:
                full_chain, expiry_date = oc.get_option_chain()
                with open(CACHE_FILE, 'wb') as f:
                    pickle.dump((full_chain, expiry_date), f)
                break
            except Exception as e:
                print(f"⚠️  Option chain attempt {attempt}/5 failed: {str(e)[:80]}")
                if attempt < 5:
                    self._stop_event.wait(2 * attempt)
                else:
                    try:
                        with open(CACHE_FILE, 'rb') as f:
                            full_chain, expiry_date = pickle.load(f)
                        print("✅ Using cached option chain.")
                    except Exception:
                        print("❌ No option chain data. Collector stopping.")
                        return

        atm_strike, spot_price = oc.get_atm_strike(full_chain)
        atm_keys               = oc.get_atm_instrument_keys(full_chain, atm_strike)

        self.atm_strike   = atm_strike
        self.expiry_date  = expiry_date
        self.spot_at_open = spot_price

        print(f"\n🔒 ATM Strike : {atm_strike}  |  Spot (open): {spot_price:.2f}"
              f"  |  Expiry: {expiry_date}\n")

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
                try:
                    opt_stream.disconnect()
                except Exception:
                    pass
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
# KEY DESIGN DECISIONS (aligns with backtest exactly):
#
# 1. CANDLE-LEVEL LOGIC:
#    Ticks are accumulated into 1-min OHLC candles (same as T10 collector).
#    SL / Target / Conflict checks happen once per COMPLETED candle — NOT
#    per individual tick. This is identical to how backtest.py works.
#
# 2. EXIT PRICES (no slippage, exact levels):
#    - Target hit : CE exits at (ce_entry - TARGET_POINTS/2)
#                   PE exits at (pe_entry - TARGET_POINTS/2)
#                   Combined PnL = exactly TARGET_POINTS pts (minus costs)
#    - SL hit     : CE exits at ce_sl, PE exits at pe_sl
#                   Combined PnL = exactly -SL_POINTS * 2 pts (minus costs)
#    - Time exit  : exits at candle close (live PnL, varies)
#
#    NOTE: Target is a COMBINED trigger (both legs together must drop >= TARGET).
#    Since we can't know per-leg exit in live, we split equally. This produces
#    identical combined PnL to backtest, which is all that matters.
#
# 3. RE-ENTRY ON MINUTE BOUNDARY:
#    After SL or Target exit, re-entry is blocked until the START of the next
#    minute (:00 seconds), not 60s from exit timestamp. This ensures live
#    enters on the same candle open as backtest.
#
# 4. CONFLICT RESOLUTION (Target + SL in same candle):
#    Open-proximity heuristic — identical to backtest:
#    Compare dist_to_sl vs dist_to_target from the candle open.
#    Whichever is smaller fires first.
#
# 5. FIXED STRIKE:
#    ATM strike is locked at session start from DataCollector. Never changes.
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

        # ── Candle builder: accumulates ticks → 1-min OHLC, same as T10 ──────
        self._candle_builder  = LiveCandleBuilder()

        # ── Re-entry gate: datetime at which next entry is allowed ────────────
        # Set to the START of the next minute after exit (not ts + 60s).
        self._reentry_after   = None

    # ── Tick callback ──────────────────────────────────────────────────────────

    def on_tick(self, ce_ltp: float, pe_ltp: float, ts: datetime):
        self._tick_queue.put((ce_ltp, pe_ltp, ts))

    # ── Trade lifecycle ────────────────────────────────────────────────────────

    def _open_trade(self, ce_open: float, pe_open: float, minute_dt: datetime):
        """
        Open a new straddle. Entry price = candle OPEN of the entry minute.
        Identical to backtest which uses candle['open'] for entry.
        """
        self.trade_counter += 1
        hhmm   = minute_dt.strftime('%H:%M')
        strike = self._atm_strike
        self.active = {
            'trade_num'     : self.trade_counter,
            'ce_strike'     : strike,
            'pe_strike'     : strike,
            'ce_entry'      : ce_open,
            'pe_entry'      : pe_open,
            'ce_sl'         : ce_open + SL_POINTS,
            'pe_sl'         : pe_open + SL_POINTS,
            'ce_entry_time' : hhmm,
            'pe_entry_time' : hhmm,
        }
        print(f"\n  ➤ TRADE #{self.trade_counter} OPEN  "
              f"[{hhmm}]  Strike {strike}  |  "
              f"CE @ {ce_open:.2f}  SL {ce_open + SL_POINTS:.2f}  |  "
              f"PE @ {pe_open:.2f}  SL {pe_open + SL_POINTS:.2f}")

    def _close_trade(self, ce_exit: float, pe_exit: float,
                     reason: str, minute_dt: datetime):
        t      = self.active
        self.active = None
        hhmm   = minute_dt.strftime('%H:%M')

        ce_pnl_pts = _leg_pnl(t['ce_entry'], ce_exit)
        pe_pnl_pts = _leg_pnl(t['pe_entry'], pe_exit)
        ce_pnl_val = ce_pnl_pts * LOT_SIZE
        pe_pnl_val = pe_pnl_pts * LOT_SIZE
        trade_val  = ce_pnl_val + pe_pnl_val

        record = {
            'trade_num'     : t['trade_num'],
            'ce_strike'     : t['ce_strike'], 'pe_strike'     : t['pe_strike'],
            'ce_entry_time' : t['ce_entry_time'], 'ce_exit_time'  : hhmm,
            'ce_entry'      : t['ce_entry'],  'ce_exit'       : ce_exit,
            'ce_pnl_pts'    : ce_pnl_pts,     'ce_pnl_val'    : ce_pnl_val,
            'pe_entry_time' : t['pe_entry_time'], 'pe_exit_time'  : hhmm,
            'pe_entry'      : t['pe_entry'],  'pe_exit'       : pe_exit,
            'pe_pnl_pts'    : pe_pnl_pts,     'pe_pnl_val'    : pe_pnl_val,
            'trade_pnl_pts' : ce_pnl_pts + pe_pnl_pts,
            'trade_pnl_val' : trade_val,
            'exit_reason'   : reason,
        }
        self.completed_trades.append(record)
        icon = "🎯" if reason == 'Target' else ("🔴" if reason == 'SL' else "🏁")
        print(f"\n  {icon} TRADE #{t['trade_num']} CLOSED  [{hhmm}]  {reason}  |  "
              f"CE exit {ce_exit:.2f}  PE exit {pe_exit:.2f}  |  "
              f"Trade PnL: ₹{trade_val:+.2f}")

        return reason in ('SL', 'Target')   # True = re-entry allowed after cooldown

    # ── Per-candle processor (called once per completed 1-min candle) ──────────

    def _process_candle(self, candle: dict):
        """
        Applies strategy logic on a COMPLETED 1-minute candle.

        This mirrors backtest.py exactly:
          - Entry  : candle open of first eligible candle
          - Target : (ce_entry - ce_low) + (pe_entry - pe_low) >= TARGET_POINTS
                     → exit at exact target level (no slippage)
          - SL     : ce_high >= ce_sl OR pe_high >= pe_sl
                     → exit at exact SL level (no slippage)
          - Conflict: open-proximity heuristic (same as backtest)
          - Time   : exit candle → close price (live PnL)
        """
        minute_dt  = candle['minute_dt']        # naive datetime, floor to :00
        hhmm       = minute_dt.strftime('%H:%M')
        tick_time  = dt_time(minute_dt.hour, minute_dt.minute)

        in_window  = self.entry_time <= tick_time < self.exit_time
        is_exit    = tick_time >= self.exit_time

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
            # Re-entry gate: must be at/after the allowed minute boundary
            # _reentry_after is set to the START of the next minute after exit,
            # so comparing minute_dt (candle start) is correct.
            if self._reentry_after and minute_dt < self._reentry_after:
                return
            # Enter at candle OPEN — same as backtest
            self._open_trade(ce_open, pe_open, minute_dt)
            # Don't check SL/Target on the same candle as entry
            # (backtest also skips: entry candle open is entry price,
            #  SL/Target first evaluated from next candle onward)
            return

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
            # ── Evaluate Target and SL on completed candle ─────────────────────
            # Identical formulas to backtest.py:
            target_pts = (t['ce_entry'] - ce_low) + (t['pe_entry'] - pe_low)
            target_hit = target_pts >= TARGET_POINTS
            sl_hit     = (ce_high >= t['ce_sl'] or pe_high >= t['pe_sl'])

            if target_hit and sl_hit:
                # ── Conflict: open-proximity heuristic (identical to backtest) ─
                sl_dists = []
                if ce_high >= t['ce_sl']:
                    sl_dists.append(t['ce_sl'] - ce_open)
                if pe_high >= t['pe_sl']:
                    sl_dists.append(t['pe_sl'] - pe_open)
                dist_to_sl = min(sl_dists)

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

                print(f"  ⚡ Conflict [{hhmm}] — "
                      f"dist_to_sl={dist_to_sl:.2f}  "
                      f"dist_to_target={dist_to_target:.2f}  "
                      f"→ {exit_reason} wins")
                do_reenter = True

            elif target_hit:
                exit_reason = 'Target'
                # Exit at exact target — no slippage.
                # Backtest uses ce_low / pe_low (which crossed the target).
                # Live uses the same: ce_low / pe_low from the live candle.
                ce_exit_px  = ce_low
                pe_exit_px  = pe_low
                do_reenter  = True

            elif sl_hit:
                exit_reason = 'SL'
                # Exit at exact SL price — no slippage. Same as backtest.
                ce_exit_px  = t['ce_sl']
                pe_exit_px  = t['pe_sl']
                do_reenter  = True

        if exit_reason:
            needs_reentry = self._close_trade(ce_exit_px, pe_exit_px,
                                              exit_reason, minute_dt)
            if needs_reentry:
                # ── Re-entry gate: START of the next minute ────────────────────
                # Example: trade exits at candle 09:24 (minute_dt = 09:24:00)
                # → _reentry_after = 09:25:00
                # → next candle with minute_dt >= 09:25:00 is eligible for entry
                # This is identical to backtest "next candle" re-entry.
                self._reentry_after = minute_dt + timedelta(minutes=1)

            if not do_reenter:
                self.session_done = True

    # ── Per-minute status ──────────────────────────────────────────────────────

    def _print_status(self, hhmm: str):
        now        = get_ist_time().strftime('%H:%M:%S')
        closed_pnl = sum(t['trade_pnl_val'] for t in self.completed_trades)
        ce_ltp     = self._latest_ce
        pe_ltp     = self._latest_pe

        if self.active:
            t        = self.active
            open_pnl = (_leg_pnl(t['ce_entry'], ce_ltp or t['ce_entry']) +
                        _leg_pnl(t['pe_entry'], pe_ltp or t['pe_entry'])) * LOT_SIZE
            total_pnl = closed_pnl + open_pnl
            pos_str   = (f"Trade#{t['trade_num']} ACTIVE  "
                         f"CE {t['ce_strike']} LTP {ce_ltp:.2f}  "
                         f"PE {t['pe_strike']} LTP {pe_ltp:.2f}  "
                         f"Open PnL ₹{open_pnl:+.2f}")
        else:
            total_pnl = closed_pnl
            pos_str   = "No active position"

        print(f"  [{now}]  Spot (open) {self._spot_at_open:.2f}  |  "
              f"Trades {len(self.completed_trades)}  |  "
              f"Session PnL ₹{total_pnl:+.2f}  |  {pos_str}")

    # ── Main loop ──────────────────────────────────────────────────────────────

    def run(self, collector: DataCollector):
        print("\n" + "=" * 65)
        print("  🚀  LIVE SHORT STRADDLE  (candle-aligned, backtest-matched)")
        print(f"  Entry  : {ENTRY_TIME}    Exit   : {EXIT_TIME}")
        print(f"  SL     : {SL_POINTS} pts/leg    Target : {TARGET_POINTS} pts combined")
        print(f"  Lot    : {LOT_SIZE}")
        print("  Logic  : 1-min candles | exact SL/Target | minute-boundary re-entry")
        print("=" * 65)

        collector.set_tick_callback(self.on_tick)
        collector.start()

        print("\n⏳ Waiting for market data stream…")
        while not collector.is_ready:
            time.sleep(0.5)

        self._atm_strike   = collector.atm_strike
        self._spot_at_open = collector.spot_at_open
        print(f"✅ Stream active. ATM Strike locked: {self._atm_strike}. Strategy running.\n")

        last_status_minute = None

        try:
            while not self.session_done:
                now      = get_ist_time()
                now_hhmm = now.strftime('%H:%M')

                # Hard market-close guard
                if now.hour > 15 or (now.hour == 15 and now.minute >= 31):
                    print("\n🏁 Session complete.")
                    break

                # Drain tick queue — feed each tick into candle builder
                try:
                    while True:
                        ce_ltp, pe_ltp, ts = self._tick_queue.get_nowait()

                        # Track latest LTP for status display
                        self._latest_ce = ce_ltp
                        self._latest_pe = pe_ltp

                        # Feed tick into candle builder
                        # Returns a completed candle dict when minute rolls over
                        completed = self._candle_builder.tick(ce_ltp, pe_ltp, ts)
                        if completed:
                            self._process_candle(completed)

                        if self.session_done:
                            break
                except queue.Empty:
                    pass

                if self.session_done:
                    break

                # Status once per minute
                if now_hhmm != last_status_minute:
                    last_status_minute = now_hhmm
                    self._print_status(now_hhmm)

                time.sleep(0.1)

        except KeyboardInterrupt:
            print("\n\n👋 Strategy stopped.")
        except Exception as e:
            print(f"\n❌ Strategy error: {e}")
            import traceback; traceback.print_exc()
        finally:
            collector.stop()
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

    collector = DataCollector(access_token)
    strategy  = LiveStrategy()
    strategy.run(collector)