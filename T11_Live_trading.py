import upstox_client
import requests
import threading
import time
import queue
import pickle
from datetime import datetime, timezone, timedelta, time as dt_time
from tabulate import tabulate

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION  (unchanged)
# ─────────────────────────────────────────────────────────────────────────────

# Strategy
ENTRY_TIME    = "09:16"
EXIT_TIME     = "15:15"
SL_POINTS     = 3
TARGET_POINTS = 5
LOT_SIZE      = 65
COST_PERCENT  = 0.0025

# Collector
TOKEN_FILE     = 'upstox_token.txt'
CACHE_FILE     = 'option_chain_cache.pkl'
MAX_RECONNECTS = 10

IST = timezone(timedelta(hours=5, minutes=30))


# ─────────────────────────────────────────────────────────────────────────────
# SHARED HELPERS  (unchanged)
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
# OPTION CHAIN  (unchanged)
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

    def get_option_chain(self, instrument_key="NSE_INDEX|Nifty 50",
                         expiry_date=None):
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
# OPTION STREAMER  — real-time tick callback, no candle building
# ─────────────────────────────────────────────────────────────────────────────

class OptionStreamer:
    """
    Subscribes to ATM CALL + PUT.
    On every incoming tick, fires _tick_cb(ce_ltp, pe_ltp, ts)
    with the latest known LTP for each leg.
    No candle aggregation, no CSV.
    """

    def __init__(self, access_token: str, atm_strike: float,
                 atm_keys: dict, expiry_date: str):
        self.access_token = access_token
        self.atm_strike   = atm_strike
        self.atm_keys     = atm_keys
        self.expiry_date  = expiry_date
        self.streamer     = None
        self.is_connected = False

        self._key_to_type = {v: k for k, v in atm_keys.items()}
        self._latest      = {}          # {'CALL': ltp, 'PUT': ltp}
        self._tick_cb     = None        # set by DataCollector
        self._snapshots   = []

    def set_tick_callback(self, cb):
        """Register callback(ce_ltp, pe_ltp, ts) called on every tick."""
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

        # Fire callback only when we have a price for both legs
        if (len(self._latest) == 2 and self._tick_cb is not None):
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
# DATA COLLECTOR  — background thread, no spot stream, no CSV
# ─────────────────────────────────────────────────────────────────────────────

class DataCollector:
    """
    Fetches ATM strike once via REST, then streams real-time option ticks.
    Fires the registered tick callback on every CALL+PUT update.
    """

    def __init__(self, access_token: str):
        self.access_token = access_token
        self._stop_event  = threading.Event()
        self._main_thread = None
        self._tick_cb     = None        # registered by LiveStrategy
        self.atm_strike   = None
        self.expiry_date  = None
        self.spot_at_open = None
        self.is_ready     = False

    def set_tick_callback(self, cb):
        """Register callback(ce_ltp, pe_ltp, ts) before calling start()."""
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
        # ── Fetch option chain (REST, once) ───────────────────────────────────
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
              f"  |  Expiry: {expiry_date}")
        print(f"   Spot will NOT be streamed — shown above only once.\n")

        # ── Setup option streamer ─────────────────────────────────────────────
        opt_stream = OptionStreamer(self.access_token, atm_strike,
                                    atm_keys, expiry_date)
        opt_stream.set_tick_callback(self._tick_cb)
        opt_stream.setup()
        threading.Thread(target=opt_stream.connect, daemon=True).start()

        opt_reconnects = 0
        self.is_ready  = True           # signal strategy to start

        # ── Streaming loop ────────────────────────────────────────────────────
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
                    print(f"⏳ Waiting {wait}s before reconnect…")
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
                else:
                    print("⚠️  Option connection failed to establish.")

            self._stop_event.wait(1)

        # ── Cleanup ───────────────────────────────────────────────────────────
        opt_stream.disconnect()


# ─────────────────────────────────────────────────────────────────────────────
# TRADE SUMMARY  (unchanged)
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
# LIVE STRATEGY  — tick-driven (no CSV, no candle polling)
# ─────────────────────────────────────────────────────────────────────────────

# Re-entry cooldown after SL/Target exit (mirrors original "next candle" gap)
REENTRY_COOLDOWN_SECS = 60

class LiveStrategy:

    def __init__(self):
        self.completed_trades = []
        self.trade_counter    = 0
        self.active           = None
        self.session_done     = False
        self.entry_time       = _parse_time(ENTRY_TIME)
        self.exit_time        = _parse_time(EXIT_TIME)

        # Real-time state (populated by tick callback)
        self._tick_queue      = queue.Queue()
        self._latest_ce       = 0.0
        self._latest_pe       = 0.0
        self._spot_at_open    = 0.0     # filled once from collector
        self._reentry_after   = None    # datetime: earliest next re-entry

    # ── Tick callback (called from WebSocket thread → pushed to queue) ─────────

    def on_tick(self, ce_ltp: float, pe_ltp: float, ts: datetime):
        """Enqueue tick for processing on the main strategy thread."""
        self._tick_queue.put((ce_ltp, pe_ltp, ts))

    # ── Trade lifecycle  (format unchanged) ──────────────────────────────────

    def _open_trade(self, ce_ltp: float, pe_ltp: float, ts: datetime):
        self.trade_counter += 1
        hhmm = ts.strftime('%H:%M')
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
        print(f"\n  ➤ TRADE #{self.trade_counter} OPEN  "
              f"[{hhmm}]  Strike {strike}  |  "
              f"CE @ {ce_ltp:.2f}  SL {ce_ltp + SL_POINTS:.2f}  |  "
              f"PE @ {pe_ltp:.2f}  SL {pe_ltp + SL_POINTS:.2f}")

    def _close_trade(self, ce_exit: float, pe_exit: float,
                     reason: str, hhmm: str):
        t  = self.active
        self.active = None
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

    # ── Per-tick processor ────────────────────────────────────────────────────

    def _process_tick(self, ce_ltp: float, pe_ltp: float, ts: datetime):
        """
        Core strategy logic — mirrors original candle logic but per-tick:
          • SL   : either leg LTP >= its SL price
          • Target: combined gain in points >= TARGET_POINTS
          • Time  : first tick at or after EXIT_TIME
        Re-entry after SL/Target is gated by REENTRY_COOLDOWN_SECS
        to match the original "next candle" behaviour.
        """
        self._latest_ce = ce_ltp
        self._latest_pe = pe_ltp

        hhmm       = ts.strftime('%H:%M')
        tick_time  = _parse_time(hhmm)
        in_window  = self.entry_time <= tick_time < self.exit_time
        is_exit    = tick_time >= self.exit_time

        if not in_window and not is_exit:
            return

        # ── Entry ─────────────────────────────────────────────────────────────
        if self.active is None and in_window:
            # Respect re-entry cooldown after SL / Target exit
            if self._reentry_after and ts < self._reentry_after:
                return
            self._open_trade(ce_ltp, pe_ltp, ts)
            return          # skip exit checks on the same entry tick

        if self.active is None:
            return          # nothing open, past exit time → do nothing

        t           = self.active
        exit_reason = None
        ce_exit_px  = pe_exit_px = None

        # ── Time exit ─────────────────────────────────────────────────────────
        if is_exit:
            exit_reason = 'Time'
            ce_exit_px  = ce_ltp
            pe_exit_px  = pe_ltp

        # ── Target (combined points gained, same formula as original) ─────────
        elif (t['ce_entry'] - ce_ltp) + (t['pe_entry'] - pe_ltp) >= TARGET_POINTS:
            exit_reason = 'Target'
            ce_exit_px  = ce_ltp
            pe_exit_px  = pe_ltp

        # ── SL (either leg breaches its stop) ─────────────────────────────────
        elif ce_ltp >= t['ce_sl'] or pe_ltp >= t['pe_sl']:
            exit_reason = 'SL'
            ce_exit_px  = t['ce_sl']    # exit at SL price (same as original)
            pe_exit_px  = t['pe_sl']

        if exit_reason:
            self._close_trade(ce_exit_px, pe_exit_px, exit_reason, hhmm)
            if exit_reason in ('SL', 'Target'):
                # Gate re-entry: wait REENTRY_COOLDOWN_SECS (≈ 1 candle gap)
                self._reentry_after = ts + timedelta(seconds=REENTRY_COOLDOWN_SECS)
            if is_exit:
                self.session_done = True

    # ── Per-minute status (format unchanged) ──────────────────────────────────

    def _print_status(self):
        now        = get_ist_time().strftime('%H:%M:%S')
        closed_pnl = sum(t['trade_pnl_val'] for t in self.completed_trades)
        num_trades = len(self.completed_trades)
        spot_str   = f"{self._spot_at_open:.2f}"
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

        print(f"  [{now}]  Spot {spot_str}  |  Trades {num_trades}  |  "
              f"Session PnL ₹{total_pnl:+.2f}  |  {pos_str}")

    # ── Main loop ─────────────────────────────────────────────────────────────

    def run(self, collector: DataCollector):
        print("\n" + "=" * 65)
        print("  🚀  LIVE SHORT STRADDLE  (real-time ticks)")
        print(f"  Entry  : {ENTRY_TIME}    Exit   : {EXIT_TIME}")
        print(f"  SL     : {SL_POINTS} pts/leg    Target : {TARGET_POINTS} pts combined")
        print(f"  Lot    : {LOT_SIZE}")
        print("=" * 65)

        # Register callback BEFORE starting collector
        collector.set_tick_callback(self.on_tick)
        collector.start()

        # Wait for collector to lock ATM strike and connect streams
        print("\n⏳ Waiting for market data stream…")
        while not collector.is_ready:
            time.sleep(0.5)

        self._atm_strike   = collector.atm_strike
        self._spot_at_open = collector.spot_at_open
        print(f"✅ Stream active. Strategy running.\n")

        last_status_minute = None

        try:
            while not self.session_done:
                now      = get_ist_time()
                now_hhmm = now.strftime('%H:%M')

                # Hard market-close guard
                if now.hour > 15 or (now.hour == 15 and now.minute >= 16):
                    print("\n🏁 Session complete.")
                    break

                # Drain tick queue — process all ticks that arrived since last loop
                try:
                    while True:
                        ce_ltp, pe_ltp, ts = self._tick_queue.get_nowait()
                        self._process_tick(ce_ltp, pe_ltp, ts)
                        if self.session_done:
                            break
                except queue.Empty:
                    pass

                if self.session_done:
                    break

                # Status once per minute (same cadence as original)
                if now_hhmm != last_status_minute:
                    last_status_minute = now_hhmm
                    self._print_status()

                time.sleep(0.1)     # tight loop — ticks arrive frequently

        except KeyboardInterrupt:
            print("\n\n👋 Strategy stopped.")
        except Exception as e:
            print(f"\n❌ Strategy error: {e}")
            import traceback; traceback.print_exc()
        finally:
            collector.stop()
            print_trade_summary(self.completed_trades)


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT  (unchanged)
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