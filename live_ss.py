import upstox_client
import requests
import threading
import time
import csv
import os
import pickle
from datetime import datetime, timezone, timedelta, time as dt_time
from tabulate import tabulate

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────

# Strategy
ENTRY_TIME    = "09:16"
EXIT_TIME     = "15:15"
SL_POINTS     = 3
TARGET_POINTS = 5
LOT_SIZE      = 65
COST_PERCENT  = 0.0025

# Collector
TOKEN_FILE    = 'upstox_token.txt'
CACHE_FILE    = 'option_chain_cache.pkl'
CSV_PREFIX    = 'nifty_'
MAX_RECONNECTS = 10

# Strategy polling
POLL_INTERVAL = 5   # seconds between CSV checks

IST = timezone(timedelta(hours=5, minutes=30))


# ─────────────────────────────────────────────────────────────────────────────
# SHARED HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def get_ist_time() -> datetime:
    return datetime.now(IST)

def _to_float(v, default=0.0):
    try: return float(v) if v else default
    except: return default

def _today_csv() -> str:
    return f"{CSV_PREFIX}{get_ist_time().strftime('%Y-%m-%d')}.csv"

def _parse_time(hhmm: str) -> dt_time:
    h, m = map(int, hhmm.split(':'))
    return dt_time(h, m)

def _leg_pnl(entry, exit_price):
    cost = (entry + exit_price) * COST_PERCENT
    return (entry - exit_price) - cost


# ─────────────────────────────────────────────────────────────────────────────
# CANDLE BUILDER
# ─────────────────────────────────────────────────────────────────────────────

class CandleBuilder:
    """Accumulates ticks into 1-minute OHLC candles. Thread-safe."""

    def __init__(self):
        self._lock       = threading.Lock()
        self._candle_min = None
        self._open = self._high = self._low = self._close = None
        self.last_closed = None

    def tick(self, ltp: float, ts: datetime) -> bool:
        if ltp <= 0:
            return False
        candle_min = ts.replace(second=0, microsecond=0)
        closed = False
        with self._lock:
            if self._candle_min is None:
                self._start(candle_min, ltp)
            elif candle_min > self._candle_min:
                self.last_closed = {
                    'minute_dt': self._candle_min,
                    'open': self._open, 'high': self._high,
                    'low': self._low,   'close': self._close,
                }
                self._start(candle_min, ltp)
                closed = True
            else:
                self._high  = max(self._high, ltp)
                self._low   = min(self._low,  ltp)
                self._close = ltp
        return closed

    def _start(self, candle_min, ltp):
        self._candle_min = candle_min
        self._open = self._high = self._low = self._close = ltp

    def get_last_closed(self):
        with self._lock:
            return dict(self.last_closed) if self.last_closed else None


# ─────────────────────────────────────────────────────────────────────────────
# SHARED STATE  (between spot thread and option thread)
# ─────────────────────────────────────────────────────────────────────────────

class SharedState:
    def __init__(self):
        self._lock        = threading.Lock()
        self.spot_opens   = {}
        self.pending_rows = []

    def set_spot_open(self, minute_dt: datetime, spot_open: float):
        key = minute_dt.replace(tzinfo=None)
        with self._lock:
            self.spot_opens[key] = spot_open

    def get_spot_open(self, minute_dt: datetime):
        key = minute_dt.replace(tzinfo=None)
        with self._lock:
            if key in self.spot_opens:
                return self.spot_opens[key]
            return self.spot_opens.get(key - timedelta(minutes=1))

    def add_rows(self, rows: list):
        with self._lock:
            self.pending_rows.extend(rows)

    def drain_rows(self) -> list:
        with self._lock:
            rows, self.pending_rows = self.pending_rows, []
            return rows


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
# SPOT STREAMER
# ─────────────────────────────────────────────────────────────────────────────

class SpotStreamer:

    INSTRUMENT_KEY = "NSE_INDEX|Nifty 50"

    def __init__(self, access_token: str, shared: SharedState):
        self.access_token = access_token
        self.shared       = shared
        self.streamer     = None
        self.is_connected = False
        self.candle       = CandleBuilder()
        self._snapshots   = []

    def setup(self):
        cfg = upstox_client.Configuration()
        cfg.access_token = self.access_token
        self.streamer = upstox_client.MarketDataStreamerV3(
            upstox_client.ApiClient(cfg), [self.INSTRUMENT_KEY], "full")
        self.streamer.on("message", self._on_message)
        self.streamer.on("open",    lambda: setattr(self, 'is_connected', True))
        self.streamer.on("close",   lambda *a: setattr(self, 'is_connected', False))
        self.streamer.on("error",   lambda e: print(f"❌ Spot error: {e}"))

    def connect(self):
        if self.streamer: self.streamer.connect()

    def disconnect(self):
        if self.streamer: self.streamer.disconnect()

    def reset_snapshots(self):
        self._snapshots.clear()

    def _on_message(self, message):
        if not isinstance(message, dict) or 'feeds' not in message:
            return
        for _, data in message['feeds'].items():
            ltp = 0.0
            if 'ltpc' in data:
                ltp = _to_float(data['ltpc'].get('ltp', 0))
            elif 'fullFeed' in data and 'indexFF' in data['fullFeed']:
                ltp = _to_float(data['fullFeed']['indexFF'].get('ltpc', {}).get('ltp', 0))
            if ltp > 0 and self.candle.tick(ltp, get_ist_time()):
                closed = self.candle.get_last_closed()
                if closed:
                    self.shared.set_spot_open(closed['minute_dt'], closed['open'])

    def is_stale(self) -> bool:
        # Don't check for staleness if not connected
        if not self.is_connected:
            self._snapshots.clear()
            return False
            
        ltp = self.candle._close or 0
        self._snapshots.append(ltp)
        
        # Increase threshold: 15 seconds of same value (15 snapshots @ 1/sec)
        if len(self._snapshots) > 15: 
            self._snapshots.pop(0)
            
        # Only return True if we have a full window of identical values
        return len(self._snapshots) == 15 and len(set(self._snapshots)) == 1


# ─────────────────────────────────────────────────────────────────────────────
# OPTION STREAMER
# ─────────────────────────────────────────────────────────────────────────────

CSV_COLUMNS = ['datetime', 'date', 'spot_open', 'expiry_date',
               'strike_price', 'option_type', 'open', 'high', 'low', 'close']

class OptionStreamer:

    def __init__(self, access_token: str, shared: SharedState,
                 atm_strike: float, atm_keys: dict, expiry_date: str):
        self.access_token = access_token
        self.shared       = shared
        self.atm_strike   = atm_strike
        self.atm_keys     = atm_keys
        self.expiry_date  = expiry_date
        self.streamer     = None
        self.is_connected = False
        self._candles     = {'CALL': CandleBuilder(), 'PUT': CandleBuilder()}
        self._key_to_type = {v: k for k, v in atm_keys.items()}
        self._market_ltp  = {}
        self._snapshots   = []

    def setup(self):
        cfg = upstox_client.Configuration()
        cfg.access_token = self.access_token
        self.streamer = upstox_client.MarketDataStreamerV3(
            upstox_client.ApiClient(cfg), list(self.atm_keys.values()), "full")
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
                ltp = _to_float(data['fullFeed']['marketFF'].get('ltpc', {}).get('ltp', 0))
            elif 'ltpc' in data:
                ltp = _to_float(data['ltpc'].get('ltp', 0))
            if ltp > 0:
                self._market_ltp[ikey] = ltp
                if self._candles[otype].tick(ltp, ts):
                    closed = self._candles[otype].get_last_closed()
                    if closed:
                        row = self._make_row(otype, closed)
                        if row:
                            self.shared.add_rows([row])

    def _make_row(self, otype, candle):
        minute_dt = candle['minute_dt']
        spot_open = self.shared.get_spot_open(minute_dt)
        if spot_open is None:
            return None
        date_str = minute_dt.strftime('%Y-%m-%d')
        return {
            'datetime'    : minute_dt.strftime('%Y-%m-%d %H:%M:%S'),
            'date'        : date_str,
            'spot_open'   : f"{spot_open:.2f}",
            'expiry_date' : self.expiry_date,
            'strike_price': f"{self.atm_strike:.1f}",
            'option_type' : otype,
            'open'        : f"{candle['open']:.2f}",
            'high'        : f"{candle['high']:.2f}",
            'low'         : f"{candle['low']:.2f}",
            'close'       : f"{candle['close']:.2f}",
        }

    def is_stale(self) -> bool:
        # Don't check for staleness if not connected or no data received yet
        if not self.is_connected or not self._market_ltp:
            self._snapshots.clear()
            return False
            
        snap = dict(self._market_ltp)
        self._snapshots.append(snap)
        
        # Increase threshold: 15 seconds of same value (15 snapshots @ 1/sec)
        if len(self._snapshots) > 15: 
            self._snapshots.pop(0)
            
        # Only return True if we have a full window of identical values
        return (len(self._snapshots) == 15 and
                all(s == self._snapshots[0] for s in self._snapshots))


# ─────────────────────────────────────────────────────────────────────────────
# CSV WRITER
# ─────────────────────────────────────────────────────────────────────────────

class CSVWriter:
    """
    Writes option candle rows to CSV.
    Tracks (datetime, option_type) keys already written and silently
    drops duplicates — prevents double-writes caused by reconnects.
    First write wins: original candle data is preserved, reconnect
    re-seals are discarded.
    """

    def __init__(self):
        self._lock       = threading.Lock()
        self._csv_path   = None
        self._date_str   = None
        self._written    = set()   # {(datetime_str, option_type)} already flushed

    def _get_path(self) -> str:
        today = get_ist_time().strftime('%Y-%m-%d')
        if today != self._date_str:
            self._date_str = today
            self._csv_path = f"nifty_{today}.csv"
            self._written.clear()
            if not os.path.exists(self._csv_path):
                with open(self._csv_path, 'w', newline='') as f:
                    csv.DictWriter(f, fieldnames=CSV_COLUMNS).writeheader()
        return self._csv_path

    def flush(self, rows: list):
        if not rows:
            return
        path = self._get_path()
        with self._lock:
            new_rows = []
            for row in rows:
                key = (row['datetime'], row['option_type'])
                if key in self._written:
                    continue   # duplicate — discard silently
                self._written.add(key)
                new_rows.append(row)
            if new_rows:
                with open(path, 'a', newline='') as f:
                    csv.DictWriter(f, fieldnames=CSV_COLUMNS,
                                   extrasaction='ignore').writerows(new_rows)


# ─────────────────────────────────────────────────────────────────────────────
# DATA COLLECTOR  (runs as background thread)
# ─────────────────────────────────────────────────────────────────────────────

class DataCollector:
    """
    Wraps T10 collection logic. Runs entirely in background threads.
    Call start() to begin, stop() to cleanly shut down.
    """

    def __init__(self, access_token: str):
        self.access_token  = access_token
        self._stop_event   = threading.Event()
        self._main_thread  = None
        self.atm_strike    = None
        self.expiry_date   = None
        self.is_ready      = False   # True once streams are connected

    def start(self):
        """Launch collector in background. Returns immediately."""
        self._main_thread = threading.Thread(
            target=self._run, daemon=True, name="DataCollector")
        self._main_thread.start()

    def stop(self):
        """Signal collector to stop and wait for clean shutdown."""
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
        # ── Fetch option chain ────────────────────────────────────────────────
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

        self.atm_strike  = atm_strike
        self.expiry_date = expiry_date

        print(f"\n🔒 ATM Strike: {atm_strike}  |  Spot: {spot_price:.2f}  "
              f"|  Expiry: {expiry_date}")

        # ── Setup streamers ───────────────────────────────────────────────────
        shared     = SharedState()
        csv_writer = CSVWriter()

        spot_stream = SpotStreamer(self.access_token, shared)
        opt_stream  = OptionStreamer(self.access_token, shared,
                                     atm_strike, atm_keys, expiry_date)

        spot_stream.setup()
        threading.Thread(target=spot_stream.connect, daemon=True).start()

        opt_stream.setup()
        threading.Thread(target=opt_stream.connect, daemon=True).start()

        spot_reconnects = 0
        opt_reconnects  = 0

        # Signal ready as soon as streams are started (strategy will wait for CSV)
        self.is_ready = True

        # ── Collection loop ───────────────────────────────────────────────────
        while not self._stop_event.is_set():
            # Flush rows to CSV
            rows = shared.drain_rows()
            if rows:
                csv_writer.flush(rows)

            # Stale: spot
            if spot_stream.is_stale():
                spot_reconnects += 1
                if spot_reconnects > MAX_RECONNECTS:
                    print(f"❌ Max spot reconnects ({MAX_RECONNECTS}) reached.")
                    break
                print(f"🔄 Spot reconnect #{spot_reconnects}…")
                try:
                    spot_stream.disconnect()
                except:
                    pass
                spot_stream.reset_snapshots()
                
                # Backoff delay
                if spot_reconnects > 1:
                    wait = min(2 * (2 ** (spot_reconnects - 2)), 60)
                    print(f"⏳ Waiting {wait} seconds before spot reconnect...")
                    self._stop_event.wait(wait)
                
                spot_stream.setup()
                threading.Thread(target=spot_stream.connect, daemon=True).start()
                
                # IMPORTANT: Wait for connection to establish before checking again
                print("⏳ Waiting for spot connection to establish…")
                for _ in range(10): # Max 10 seconds
                    if spot_stream.is_connected or self._stop_event.is_set():
                        break
                    time.sleep(1)
                
                if spot_stream.is_connected:
                    print("✅ Spot connection restored.")
                    spot_reconnects = 0
                else:
                    print("⚠️  Spot connection failed to establish.")

            # Stale: options
            if opt_stream.is_stale():
                opt_reconnects += 1
                if opt_reconnects > MAX_RECONNECTS:
                    print(f"❌ Max option reconnects ({MAX_RECONNECTS}) reached.")
                    break
                print(f"🔄 Option reconnect #{opt_reconnects}…")
                try:
                    opt_stream.disconnect()
                except:
                    pass
                opt_stream.reset_snapshots()
                
                # Backoff delay
                if opt_reconnects > 1:
                    wait = min(2 * (2 ** (opt_reconnects - 2)), 60)
                    print(f"⏳ Waiting {wait} seconds before option reconnect...")
                    self._stop_event.wait(wait)
                    
                opt_stream.setup()
                threading.Thread(target=opt_stream.connect, daemon=True).start()
                
                # IMPORTANT: Wait for connection to establish before checking again
                print("⏳ Waiting for option connection to establish…")
                for _ in range(10): # Max 10 seconds
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
        spot_stream.disconnect()
        opt_stream.disconnect()
        rows = shared.drain_rows()
        if rows:
            csv_writer.flush(rows)


# ─────────────────────────────────────────────────────────────────────────────
# FEED READER  (strategy side — reads CSV written by DataCollector)
# ─────────────────────────────────────────────────────────────────────────────

class FeedReader:

    def __init__(self, csv_path: str):
        self.csv_path      = csv_path
        self._skip_minutes = set()
        self._seen_minutes = set()
        self._initialized  = False
        self.fixed_strike  = None
        self.expiry        = None

    def initialize(self):
        if not os.path.exists(self.csv_path):
            self._initialized = True
            return
        with open(self.csv_path, 'r', newline='') as f:
            for row in csv.DictReader(f):
                self._skip_minutes.add(row['datetime'][:16])
        self._initialized = True

    def poll(self) -> list:
        if not self._initialized:
            self.initialize()
        if not os.path.exists(self.csv_path):
            return []

        buckets = {}
        try:
            with open(self.csv_path, 'r', newline='') as f:
                for row in csv.DictReader(f):
                    minute = row['datetime'][:16]
                    if minute in self._skip_minutes:
                        continue
                    if minute not in buckets:
                        buckets[minute] = {
                            'spot_open': float(row['spot_open']),
                            'strike'   : float(row['strike_price']),
                            'expiry'   : row['expiry_date'],
                        }
                    buckets[minute][row['option_type']] = {
                        'open' : float(row['open']),
                        'high' : float(row['high']),
                        'low'  : float(row['low']),
                        'close': float(row['close']),
                    }
        except Exception:
            return []

        if not buckets:
            return []

        # Only return minutes that have a newer minute after them (fully sealed)
        all_minutes = sorted(buckets.keys())
        results = []
        for minute in all_minutes[:-1]:
            if minute in self._seen_minutes:
                continue
            b = buckets[minute]
            if 'CALL' not in b or 'PUT' not in b:
                continue
            self._seen_minutes.add(minute)
            if self.fixed_strike is None:
                self.fixed_strike = b['strike']
                self.expiry       = b['expiry']
            results.append({
                'datetime'  : minute,
                'spot_open' : b['spot_open'],
                'strike'    : b['strike'],
                'ce'        : b['CALL'],
                'pe'        : b['PUT'],
            })
        return results

    def latest_tick(self):
        """Most recent (possibly partial) candle close — for status display only."""
        if not os.path.exists(self.csv_path):
            return None
        try:
            latest = {}
            with open(self.csv_path, 'r', newline='') as f:
                for row in csv.DictReader(f):
                    m = row['datetime'][:16]
                    if m not in latest:
                        latest[m] = {'spot': float(row['spot_open'])}
                    latest[m][row['option_type']] = float(row['close'])
            if not latest:
                return None
            d = latest[sorted(latest.keys())[-1]]
            return d.get('spot', 0), d.get('CALL', 0), d.get('PUT', 0)
        except Exception:
            return None


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
    # Print line-by-line to prevent terminal buffering from swallowing rows
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
# ─────────────────────────────────────────────────────────────────────────────

class LiveStrategy:

    def __init__(self):
        self.completed_trades = []
        self.trade_counter    = 0
        self.active           = None
        self.session_done     = False
        self.entry_time       = _parse_time(ENTRY_TIME)
        self.exit_time        = _parse_time(EXIT_TIME)
        self.feed             = FeedReader(_today_csv())

    # ── Trade lifecycle ───────────────────────────────────────────────────────

    def _open_trade(self, candle):
        self.trade_counter += 1
        ce     = candle['ce']
        pe     = candle['pe']
        strike = self.feed.fixed_strike
        self.active = {
            'trade_num'     : self.trade_counter,
            'ce_strike'     : strike,
            'pe_strike'     : strike,
            'ce_entry'      : ce['open'],
            'pe_entry'      : pe['open'],
            'ce_sl'         : ce['open'] + SL_POINTS,
            'pe_sl'         : pe['open'] + SL_POINTS,
            'ce_entry_time' : candle['datetime'][11:16],
            'pe_entry_time' : candle['datetime'][11:16],
        }
        print(f"\n  ➤ TRADE #{self.trade_counter} OPEN  "
              f"[{candle['datetime'][11:16]}]  Strike {strike}  |  "
              f"CE @ {ce['open']:.2f}  SL {ce['open']+SL_POINTS:.2f}  |  "
              f"PE @ {pe['open']:.2f}  SL {pe['open']+SL_POINTS:.2f}")

    def _close_trade(self, ce_exit, pe_exit, reason, hhmm):
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

    # ── Candle processor ──────────────────────────────────────────────────────

    def _process_candle(self, candle) -> bool:
        """Returns True if session should end (time exit)."""
        hhmm        = candle['datetime'][11:16]
        candle_time = _parse_time(hhmm)
        ce          = candle['ce']
        pe          = candle['pe']
        in_window   = self.entry_time <= candle_time < self.exit_time
        is_exit     = (candle_time == self.exit_time)

        if not in_window and not is_exit:
            return False

        if self.active is None and not is_exit:
            self._open_trade(candle)

        if self.active is None:
            return is_exit

        t           = self.active
        exit_reason = None
        ce_exit_px  = pe_exit_px = None
        do_reenter  = False

        if is_exit:
            exit_reason = 'Time'
            ce_exit_px  = ce['close']
            pe_exit_px  = pe['close']
        else:
            target_pts = (t['ce_entry'] - ce['low']) + (t['pe_entry'] - pe['low'])
            if target_pts >= TARGET_POINTS:
                exit_reason = 'Target'
                ce_exit_px  = ce['low']
                pe_exit_px  = pe['low']
                do_reenter  = True
            elif ce['high'] >= t['ce_sl'] or pe['high'] >= t['pe_sl']:
                exit_reason = 'SL'
                ce_exit_px  = t['ce_sl']
                pe_exit_px  = t['pe_sl']
                do_reenter  = True

        if exit_reason:
            self._close_trade(ce_exit_px, pe_exit_px, exit_reason, hhmm)
            # Re-entry opens on NEXT candle — self.active already None

        return is_exit

    # ── Per-minute status ─────────────────────────────────────────────────────

    def _print_status(self):
        now        = get_ist_time().strftime('%H:%M:%S')
        tick       = self.feed.latest_tick()
        closed_pnl = sum(t['trade_pnl_val'] for t in self.completed_trades)
        num_trades = len(self.completed_trades)
        spot_str   = f"{tick[0]:.2f}" if tick else "—"
        ce_ltp     = tick[1] if tick else None
        pe_ltp     = tick[2] if tick else None

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

        print(f"  [{now}]  Spot {spot_str}  |  Trades {num_trades}  |  "
              f"Session PnL ₹{total_pnl:+.2f}  |  {pos_str}")

    # ── Main loop ─────────────────────────────────────────────────────────────

    def run(self, collector: DataCollector):
        print("\n" + "=" * 65)
        print("  🚀  LIVE SHORT STRADDLE")
        print(f"  Entry  : {ENTRY_TIME}    Exit   : {EXIT_TIME}")
        print(f"  SL     : {SL_POINTS} pts/leg    Target : {TARGET_POINTS} pts combined")
        print(f"  Lot    : {LOT_SIZE}")
        print("=" * 65)

        # Wait for CSV to appear (collector writes it as soon as first candle seals)
        csv_path = _today_csv()
        print(f"\n⏳ Initialising market data…")
        while not os.path.exists(csv_path):
            time.sleep(1)

        self.feed.initialize()
        print(f"✅ Market data active. Strategy running.\n")

        last_status_minute = None

        try:
            while not self.session_done:
                now      = get_ist_time()
                now_hhmm = now.strftime('%H:%M')

                if now.hour > 15 or (now.hour == 15 and now.minute >= 16):
                    print("\n🏁 Session complete.")
                    break

                for candle in self.feed.poll():
                    if self._process_candle(candle):
                        self.session_done = True
                        break

                if self.session_done:
                    break

                if now_hhmm != last_status_minute:
                    last_status_minute = now_hhmm
                    self._print_status()

                time.sleep(POLL_INTERVAL)

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
    # Load token
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

    # Start data collector in background
    collector = DataCollector(access_token)
    collector.start()

    # Run strategy (blocks until session ends or Ctrl+C)
    strategy = LiveStrategy()
    strategy.run(collector)