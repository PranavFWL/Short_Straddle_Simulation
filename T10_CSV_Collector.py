"""
T10_CSV_Collector.py
====================
Combines T8 (Nifty50 spot) + T9 (Option Chain) real-time streams into a
single CSV file: nifty_YYYY-MM-DD.csv

ATM strike is calculated ONCE at session start and fixed for the entire session.
Only 2 rows are written per completed TF-minute candle: one CALL + one PUT for
that fixed ATM strike.

CSV Format (matches backtest.py exactly):
  datetime, date, spot_open, expiry_date, strike_price, option_type,
  open, high, low, close

Timeframe:
  Set TF_SECONDS below. The candle grid is anchored at 09:15:00 IST.
    TF_SECONDS = 30  → 30s candles  (09:15:00, 09:15:30, 09:16:00 …)
    TF_SECONDS = 60  → 1-min candles (09:15:00, 09:16:00, 09:17:00 …)
    TF_SECONDS = 180 → 3-min candles (09:15:00, 09:18:00, 09:21:00 …)

Run:
    python T10_CSV_Collector.py

Stop:
    Ctrl+C  (or auto-stops at 15:30 IST)
"""

import upstox_client
import requests
import threading
import time
import csv
import os
import json
import pickle
from datetime import datetime, timezone, timedelta
from urllib.parse import urlencode, parse_qs, urlparse

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────

TOKEN_FILE    = 'upstox_token.txt'
CACHE_FILE    = 'option_chain_cache.pkl'
MAX_RECONNECTS = 10

IST = timezone(timedelta(hours=5, minutes=30))

# ── Timeframe ─────────────────────────────────────────────────────────────────
# Controls candle width in SECONDS. The bar grid is anchored at 09:15:00 IST.
#
#   TF_SECONDS = 30   → 30-sec candles  (bars: 09:15:00, 09:15:30, 09:16:00 …)
#   TF_SECONDS = 60   → 1-min candles   (bars: 09:15:00, 09:16:00, 09:17:00 …)
#   TF_SECONDS = 180  → 3-min candles   (bars: 09:15:00, 09:18:00, 09:21:00 …)
#   TF_SECONDS = 300  → 5-min candles   (bars: 09:15:00, 09:20:00, 09:25:00 …)
#
# Must match TF_SECONDS in live_strategy.py.
# ─────────────────────────────────────────────────────────────────────────────
TF_SECONDS = 30

# Anchor for the bar grid — market open (09:15:00 IST), never changes
_CANDLE_ANCHOR_H = 9
_CANDLE_ANCHOR_M = 15
_CANDLE_ANCHOR_S = 0


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def get_ist_time() -> datetime:
    return datetime.now(IST)

def is_weekend() -> bool:
    return get_ist_time().weekday() in (5, 6)

def is_market_hours() -> bool:
    now = get_ist_time()
    return (now.replace(hour=9, minute=15, second=0, microsecond=0)
            <= now <=
            now.replace(hour=15, minute=30, second=0, microsecond=0))

def wait_for_market_open():
    now       = get_ist_time()
    open_time = now.replace(hour=9, minute=15, second=0, microsecond=0)
    if now < open_time:
        wait = (open_time - now).total_seconds()
        print(f"\n⏰ Current time : {now.strftime('%H:%M:%S')} IST")
        print(f"⏰ Market opens : 09:15:00 IST")
        print(f"⏳ Waiting {int(wait)}s ({int(wait/60)} min)…")
        time.sleep(wait)
        print("🔔 Market open — starting collection…\n")

def _to_float(v, default=0.0):
    try: return float(v) if v else default
    except: return default


def _candle_start(ts: datetime, tf_seconds: int) -> datetime:
    """
    Return the bar-open datetime that *ts* belongs to, anchored at
    09:15:00 IST and repeating every tf_seconds seconds.

    Result is naive (tzinfo stripped) for use as a dict key.

    Examples (TF_SECONDS=30, anchor=09:15:00):
        09:15:00 → 09:15:00   09:15:29 → 09:15:00   09:15:30 → 09:15:30
        09:16:59 → 09:16:30   09:18:00 → 09:18:00

    Examples (TF_SECONDS=60):
        09:15:00 → 09:15:00   09:15:59 → 09:15:00   09:16:00 → 09:16:00

    Examples (TF_SECONDS=180, i.e. 3 min):
        09:15:00 → 09:15:00   09:17:59 → 09:15:00   09:18:00 → 09:18:00
    """
    ts_naive = ts.replace(tzinfo=None)
    anchor   = ts_naive.replace(
        hour=_CANDLE_ANCHOR_H, minute=_CANDLE_ANCHOR_M,
        second=_CANDLE_ANCHOR_S, microsecond=0)

    if ts_naive < anchor:
        return anchor                          # before market open — first bar

    elapsed = int((ts_naive - anchor).total_seconds())
    offset  = (elapsed // tf_seconds) * tf_seconds
    return anchor + timedelta(seconds=offset)


# ─────────────────────────────────────────────────────────────────────────────
# CANDLE BUILDER  (TF-minute OHLC)
# ─────────────────────────────────────────────────────────────────────────────

class CandleBuilder:
    """
    Accumulates tick data and closes a TF_SECONDS OHLC candle.
    Only seals completed candles — no partial candle data is ever written.

    The bar boundary is computed via _candle_start(), so changing TF_SECONDS
    automatically adjusts candle width with no other code changes.
    """

    def __init__(self, tf_seconds: int = TF_SECONDS):
        self._tf         = tf_seconds
        self._lock       = threading.Lock()
        self._candle_bar = None   # naive datetime of current bar open
        self._open       = None
        self._high       = None
        self._low        = None
        self._close      = None
        self.last_closed = None   # {minute_dt, open, high, low, close}

    def tick(self, ltp: float, ts: datetime) -> bool:
        """
        Feed a tick. Returns True when a candle just sealed.
        A candle seals when the first tick of a NEW bar arrives.
        """
        if ltp <= 0:
            return False

        bar_dt = _candle_start(ts, self._tf)
        closed = False

        with self._lock:
            if self._candle_bar is None:
                self._open_candle(bar_dt, ltp)

            elif bar_dt > self._candle_bar:
                # Seal the completed bar
                self.last_closed = {
                    'minute_dt': self._candle_bar,   # key name kept for compatibility
                    'open'     : self._open,
                    'high'     : self._high,
                    'low'      : self._low,
                    'close'    : self._close,
                }
                self._open_candle(bar_dt, ltp)
                closed = True

            else:
                # Same bar — update OHLC
                self._high  = max(self._high, ltp)
                self._low   = min(self._low,  ltp)
                self._close = ltp

        return closed

    def _open_candle(self, bar_dt, ltp):
        self._candle_bar = bar_dt
        self._open  = ltp
        self._high  = ltp
        self._low   = ltp
        self._close = ltp

    def get_last_closed(self):
        with self._lock:
            return dict(self.last_closed) if self.last_closed else None


# ─────────────────────────────────────────────────────────────────────────────
# SHARED STATE
# ─────────────────────────────────────────────────────────────────────────────

class SharedState:
    """Thread-safe store for spot opens and pending CSV rows."""

    def __init__(self):
        self._lock        = threading.Lock()
        self.spot_opens   = {}   # {naive bar_dt: spot_open float}
        self.pending_rows = []

    def set_spot_open(self, bar_dt: datetime, spot_open: float):
        """Record spot open price for the bar that just closed."""
        key = bar_dt.replace(tzinfo=None)
        with self._lock:
            self.spot_opens[key] = spot_open

    def get_spot_open(self, bar_dt: datetime):
        """
        Look up the spot open for *bar_dt*.
        Falls back to the most recent earlier bar if needed — handles the race
        where an option candle seals fractionally before the spot candle on the
        same boundary (extremely rare but possible).
        """
        key = bar_dt.replace(tzinfo=None)
        with self._lock:
            if key in self.spot_opens:
                return self.spot_opens[key]
            # Scan backwards through all stored bars to find the nearest earlier one
            earlier = [k for k in self.spot_opens if k < key]
            if earlier:
                return self.spot_opens[max(earlier)]
            return None

    def add_rows(self, rows: list):
        with self._lock:
            self.pending_rows.extend(rows)

    def drain_rows(self) -> list:
        with self._lock:
            rows, self.pending_rows = self.pending_rows, []
            return rows


# ─────────────────────────────────────────────────────────────────────────────
# UPSTOX AUTH
# ─────────────────────────────────────────────────────────────────────────────

class UpstoxAuth:

    def __init__(self, api_key, api_secret, redirect_uri="https://www.google.com"):
        self.api_key      = api_key
        self.api_secret   = api_secret
        self.redirect_uri = redirect_uri
        self.base_url     = "https://api.upstox.com/v2"

    def get_access_token(self, auth_code):
        resp = requests.post(
            f"{self.base_url}/login/authorization/token",
            headers={'accept': 'application/json',
                     'Content-Type': 'application/x-www-form-urlencoded'},
            data={'code': auth_code, 'client_id': self.api_key,
                  'client_secret': self.api_secret,
                  'redirect_uri': self.redirect_uri,
                  'grant_type': 'authorization_code'},
        )
        resp.raise_for_status()
        return resp.json().get('access_token')


# ─────────────────────────────────────────────────────────────────────────────
# OPTION CHAIN FETCHER
# ─────────────────────────────────────────────────────────────────────────────

class UpstoxOptionChain:

    def __init__(self, access_token):
        self.access_token = access_token
        self.base_url     = "https://api.upstox.com/v2"
        self.headers = {
            'Content-Type'  : 'application/json',
            'Accept'        : 'application/json',
            'Authorization' : f'Bearer {access_token}',
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
                nearest = sorted(set(c['expiry'] for c in contracts))[0]
                print(f"✅ Nearest expiry: {nearest}")
                return nearest
        raise Exception("Could not determine nearest expiry")

    def get_atm_strike(self, chain_data):
        if not chain_data:
            raise Exception("Empty option chain data")
        spot  = chain_data[0].get('underlying_spot_price', 0)
        all_k = sorted(e['strike_price'] for e in chain_data)
        atm   = min(all_k, key=lambda x: abs(x - spot))
        print(f"📊 Spot: {spot:.2f}  ATM Strike (fixed): {atm}")
        return atm, spot

    def get_atm_instrument_keys(self, chain_data, atm_strike):
        keys = {}
        for entry in chain_data:
            if entry['strike_price'] != atm_strike:
                continue
            ce_key = entry.get('call_options', {}).get('instrument_key')
            pe_key = entry.get('put_options',  {}).get('instrument_key')
            if ce_key:
                keys['CALL'] = ce_key
            if pe_key:
                keys['PUT'] = pe_key
        if len(keys) < 2:
            raise Exception(
                f"Could not find both CE and PE keys for ATM {atm_strike}")
        print(f"🔑 ATM instrument keys: CALL={keys['CALL']}  PUT={keys['PUT']}")
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
        # CandleBuilder uses the global TF_SECONDS automatically
        self.candle       = CandleBuilder()
        self._snapshots   = []

    def setup(self):
        cfg = upstox_client.Configuration()
        cfg.access_token = self.access_token
        self.streamer = upstox_client.MarketDataStreamerV3(
            upstox_client.ApiClient(cfg),
            [self.INSTRUMENT_KEY],
            "full",
        )
        self.streamer.on("message", self._on_message)
        self.streamer.on("open",    self._on_open)
        self.streamer.on("error",   self._on_error)
        self.streamer.on("close",   self._on_close)
        print("📡 Spot streamer configured")

    def connect(self):
        if self.streamer:
            self.streamer.connect()

    def disconnect(self):
        if self.streamer:
            self.streamer.disconnect()

    def reset_snapshots(self):
        self._snapshots.clear()

    def _on_open(self):
        self.is_connected = True
        print("✅ Spot WebSocket connected")

    def _on_close(self, code=None, msg=None):
        self.is_connected = False
        print(f"🔌 Spot WebSocket disconnected (code={code})")

    def _on_error(self, error):
        print(f"❌ Spot WebSocket error: {error}")

    def _on_message(self, message):
        if not isinstance(message, dict) or 'feeds' not in message:
            return
        for _, data in message['feeds'].items():
            ltp = 0.0
            if 'ltpc' in data:
                ltp = _to_float(data['ltpc'].get('ltp', 0))
            elif 'fullFeed' in data and 'indexFF' in data['fullFeed']:
                ltpc = data['fullFeed']['indexFF'].get('ltpc', {})
                ltp  = _to_float(ltpc.get('ltp', 0))
            if ltp > 0:
                ts     = get_ist_time()
                sealed = self.candle.tick(ltp, ts)

                # Always keep the current bar's spot open stored so that when
                # an option candle seals at the same boundary the lookup never
                # returns None — even if the spot candle hasn't formally sealed.
                cur_bar = _candle_start(ts, TF_SECONDS)
                if self.candle._open is not None:
                    self.shared.set_spot_open(cur_bar, self.candle._open)

                if sealed:
                    closed = self.candle.get_last_closed()
                    if closed:
                        # Explicitly store the just-sealed bar too (belt & braces)
                        self.shared.set_spot_open(closed['minute_dt'],
                                                   closed['open'])
                        print(f"  📈 Spot candle sealed: "
                              f"{closed['minute_dt'].strftime('%H:%M')}  "
                              f"O={closed['open']:.2f}  "
                              f"(TF={TF_SECONDS}s)")

    def is_stale(self) -> bool:
        if not self.is_connected:
            self._snapshots.clear()
            return False
        ltp = self.candle._close or 0
        self._snapshots.append(ltp)
        if len(self._snapshots) > 15:
            self._snapshots.pop(0)
        return len(self._snapshots) == 15 and len(set(self._snapshots)) == 1


# ─────────────────────────────────────────────────────────────────────────────
# OPTION STREAMER  — ATM strike only
# ─────────────────────────────────────────────────────────────────────────────

class OptionStreamer:
    """
    Subscribes to exactly 2 instruments: ATM CALL and ATM PUT.
    ATM strike is fixed at session start and never changes.
    Writes exactly 2 CSV rows per completed TF-minute candle.
    """

    def __init__(self, access_token: str, shared: SharedState,
                 atm_strike: float, atm_keys: dict, expiry_date: str):
        self.access_token = access_token
        self.shared       = shared
        self.atm_strike   = atm_strike
        self.atm_keys     = atm_keys
        self.expiry_date  = expiry_date
        self.streamer     = None
        self.is_connected = False

        # One CandleBuilder per option leg — both use the global TF_SECONDS
        self._candles = {
            'CALL': CandleBuilder(),
            'PUT' : CandleBuilder(),
        }
        self._key_to_type = {v: k for k, v in atm_keys.items()}
        self._market_ltp  = {}
        self._snapshots   = []

    def setup(self):
        cfg = upstox_client.Configuration()
        cfg.access_token = self.access_token
        self.streamer = upstox_client.MarketDataStreamerV3(
            upstox_client.ApiClient(cfg),
            list(self.atm_keys.values()),
            "full",
        )
        self.streamer.on("message", self._on_message)
        self.streamer.on("open",    self._on_open)
        self.streamer.on("error",   self._on_error)
        self.streamer.on("close",   self._on_close)
        print(f"📡 Option streamer configured — ATM {self.atm_strike} "
              f"(CALL + PUT only)  TF={TF_SECONDS}s")

    def connect(self):
        if self.streamer:
            self.streamer.connect()

    def disconnect(self):
        if self.streamer:
            self.streamer.disconnect()

    def reset_snapshots(self):
        self._snapshots.clear()

    def _on_open(self):
        self.is_connected = True
        print("✅ Option WebSocket connected")

    def _on_close(self, code=None, msg=None):
        self.is_connected = False
        print(f"🔌 Option WebSocket disconnected (code={code})")

    def _on_error(self, error):
        print(f"❌ Option WebSocket error: {error}")

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
                mff  = data['fullFeed']['marketFF']
                ltpc = mff.get('ltpc', {})
                ltp  = _to_float(ltpc.get('ltp', 0))
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
                            print(f"  📉 Option candle sealed: "
                                  f"{closed['minute_dt'].strftime('%H:%M')}  "
                                  f"{otype}  "
                                  f"O={closed['open']:.2f} "
                                  f"H={closed['high']:.2f} "
                                  f"L={closed['low']:.2f} "
                                  f"C={closed['close']:.2f}  "
                                  f"(TF={TF_SECONDS}s)")

    def _make_row(self, otype: str, candle: dict):
        """Build one CSV row dict for a completed option candle."""
        bar_dt = candle['minute_dt']   # naive datetime of bar open
        dt_str = bar_dt.strftime('%Y-%m-%d %H:%M:%S')

        return {
            'datetime'    : dt_str,
            'strike_price': f"{self.atm_strike:.1f}",
            'option_type' : otype,
            'open'        : f"{candle['open']:.2f}",
            'high'        : f"{candle['high']:.2f}",
            'low'         : f"{candle['low']:.2f}",
            'close'       : f"{candle['close']:.2f}",
        }

    def is_stale(self) -> bool:
        if not self.is_connected or not self._market_ltp:
            self._snapshots.clear()
            return False
        snap = dict(self._market_ltp)
        self._snapshots.append(snap)
        if len(self._snapshots) > 15:
            self._snapshots.pop(0)
        return (len(self._snapshots) == 15 and
                all(s == self._snapshots[0] for s in self._snapshots))


# ─────────────────────────────────────────────────────────────────────────────
# CSV WRITER
# ─────────────────────────────────────────────────────────────────────────────

CSV_COLUMNS = [
    'datetime', 'strike_price', 'option_type',
    'open', 'high', 'low', 'close',
]

class CSVWriter:
    """
    Writes option candle rows to CSV.
    Tracks (datetime, option_type) keys already written — first write wins,
    duplicates from reconnects are silently dropped.
    """

    def __init__(self):
        self._lock     = threading.Lock()
        self._csv_path = None
        self._date_str = None
        self._written  = set()   # {(datetime_str, option_type)}

    def _get_path(self) -> str:
        today = get_ist_time().strftime('%Y-%m-%d')
        if today != self._date_str:
            self._date_str = today
            self._csv_path = f"nifty_{today}.csv"
            self._written.clear()
            if not os.path.exists(self._csv_path):
                with open(self._csv_path, 'w', newline='') as f:
                    writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
                    writer.writeheader()
                print(f"📄 Created {self._csv_path}")
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
                    continue
                self._written.add(key)
                new_rows.append(row)
            if new_rows:
                with open(path, 'a', newline='') as f:
                    csv.DictWriter(f, fieldnames=CSV_COLUMNS,
                                   extrasaction='ignore').writerows(new_rows)
                print(f"💾 Wrote {len(new_rows)} rows → {path}")


# ─────────────────────────────────────────────────────────────────────────────
# RECONNECT HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _reconnect_spot(spot_stream: SpotStreamer,
                    stop_event: threading.Event, attempt: int):
    print(f"🔄 Spot reconnect attempt {attempt}…")
    try:
        spot_stream.disconnect()
    except Exception:
        pass
    spot_stream.reset_snapshots()
    if attempt > 1:
        delay = min(2 * (2 ** (attempt - 2)), 60)
        print(f"⏳ Waiting {delay}s…")
        stop_event.wait(delay)
    spot_stream.setup()
    threading.Thread(target=spot_stream.connect, daemon=True).start()
    for _ in range(10):
        if spot_stream.is_connected or stop_event.is_set():
            break
        time.sleep(1)


def _reconnect_option(opt_stream: OptionStreamer,
                      stop_event: threading.Event, attempt: int):
    print(f"🔄 Option reconnect attempt {attempt}…")
    try:
        opt_stream.disconnect()
    except Exception:
        pass
    opt_stream.reset_snapshots()
    if attempt > 1:
        delay = min(2 * (2 ** (attempt - 2)), 60)
        print(f"⏳ Waiting {delay}s…")
        stop_event.wait(delay)
    opt_stream.setup()
    threading.Thread(target=opt_stream.connect, daemon=True).start()
    for _ in range(10):
        if opt_stream.is_connected or stop_event.is_set():
            break
        time.sleep(1)


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    print("\n" + "=" * 70)
    print(f"  T10 CSV COLLECTOR — Fixed ATM Strike | "
          f"{TF_SECONDS}s Candles → CSV")
    print("=" * 70)

    # ── Load access token ─────────────────────────────────────────────────────
    try:
        with open(TOKEN_FILE) as f:
            access_token = f.read().strip()
        if not access_token:
            raise ValueError("Empty token file")
        print(f"✅ Access token loaded from {TOKEN_FILE}")
    except FileNotFoundError:
        print(f"❌ {TOKEN_FILE} not found.")
        return
    except Exception as e:
        print(f"❌ Token error: {e}")
        return

    # ── Weekend / market time guard ───────────────────────────────────────────
    if is_weekend():
        print("📅 Today is a weekend — market is closed.")
        return

    wait_for_market_open()

    # ── Fetch option chain to determine ATM strike ────────────────────────────
    print("\n" + "=" * 70)
    print("  FETCHING OPTION CHAIN — calculating ATM strike")
    print("=" * 70)

    oc          = UpstoxOptionChain(access_token)
    full_chain  = None
    expiry_date = None

    for attempt in range(1, 6):
        try:
            print(f"📡 Attempt {attempt}/5…")
            full_chain, expiry_date = oc.get_option_chain()
            with open(CACHE_FILE, 'wb') as f:
                pickle.dump((full_chain, expiry_date), f)
            print("✅ Option chain fetched.")
            break
        except Exception as e:
            print(f"⚠️  Attempt {attempt} failed: {str(e)[:100]}")
            if attempt < 5:
                wait = 2 * attempt
                print(f"⏳ Retrying in {wait}s…")
                time.sleep(wait)
            else:
                print("⚠️  Trying cache…")
                try:
                    with open(CACHE_FILE, 'rb') as f:
                        full_chain, expiry_date = pickle.load(f)
                    print("✅ Using cached option chain.")
                except Exception:
                    print("❌ No cache available. Exiting.")
                    return

    # ── Calculate ATM once — fixed for entire session ─────────────────────────
    atm_strike, spot_price = oc.get_atm_strike(full_chain)
    atm_keys               = oc.get_atm_instrument_keys(full_chain, atm_strike)

    _session_state = {
        'date'        : get_ist_time().strftime('%Y-%m-%d'),
        'atm_strike'  : atm_strike,
        'expiry_date' : expiry_date,
        'atm_ce_key'  : atm_keys['CALL'],
        'atm_pe_key'  : atm_keys['PUT'],
        'spot_at_open': spot_price,
        'tf_seconds'  : TF_SECONDS,            # written for strategy reference
    }
    with open('session_state.json', 'w') as _f:
        json.dump(_session_state, _f, indent=2)

    print(f"\n🔒 ATM Strike LOCKED: {atm_strike}  "
          f"(Spot at open: {spot_price:.2f})")
    print(f"   This strike will NOT change during the session.")
    print(f"   Expiry  : {expiry_date}")
    print(f"   TF      : {TF_SECONDS}s  "
          f"(bars anchored at 09:15 IST)")
    print(f"   ✅ session_state.json written — live_strategy will pick this up.")

    # ── Shared state & CSV writer ─────────────────────────────────────────────
    shared     = SharedState()
    csv_writer = CSVWriter()

    # ── Setup streamers ───────────────────────────────────────────────────────
    spot_stream = SpotStreamer(access_token, shared)
    opt_stream  = OptionStreamer(access_token, shared,
                                 atm_strike, atm_keys, expiry_date)

    print("\n" + "=" * 70)
    print("  STARTING STREAMS")
    print("=" * 70)

    spot_stream.setup()
    threading.Thread(target=spot_stream.connect, daemon=True).start()

    opt_stream.setup()
    threading.Thread(target=opt_stream.connect, daemon=True).start()

    print("⏳ Waiting 10s for initial connection…")
    time.sleep(10)

    spot_reconnects = 0
    opt_reconnects  = 0
    rows_written    = 0
    last_log        = time.time()
    stop_event      = threading.Event()

    try:
        while not stop_event.is_set():
            # ── Market-close guard ─────────────────────────────────────────────
            now = get_ist_time()
            if now.hour > 15 or (now.hour == 15 and now.minute >= 30):
                print("\n🔔 Market closed at 15:30 IST — stopping collection.")
                break

            time.sleep(1)

            # ── Flush pending rows to CSV ──────────────────────────────────────
            rows = shared.drain_rows()
            if rows:
                csv_writer.flush(rows)
                rows_written += len(rows)

            # ── Periodic status ────────────────────────────────────────────────
            if time.time() - last_log >= 30:
                print(f"📊 {now.strftime('%H:%M:%S')} | "
                      f"TF={TF_SECONDS}s | "
                      f"Rows: {rows_written} | "
                      f"Spot: {spot_stream.is_connected} | "
                      f"Options: {opt_stream.is_connected}")
                last_log = time.time()

            # ── Stale: spot ───────────────────────────────────────────────────
            if spot_stream.is_stale():
                spot_reconnects += 1
                if spot_reconnects > MAX_RECONNECTS:
                    print(f"❌ Max spot reconnects ({MAX_RECONNECTS}) reached.")
                    break
                _reconnect_spot(spot_stream, stop_event, spot_reconnects)
                if spot_stream.is_connected:
                    print("✅ Spot connection restored.")
                    spot_reconnects = 0
                else:
                    print("⚠️  Spot connection failed to establish.")

            # ── Stale: options ────────────────────────────────────────────────
            if opt_stream.is_stale():
                opt_reconnects += 1
                if opt_reconnects > MAX_RECONNECTS:
                    print(f"❌ Max option reconnects ({MAX_RECONNECTS}) reached.")
                    break
                _reconnect_option(opt_stream, stop_event, opt_reconnects)
                if opt_stream.is_connected:
                    print("✅ Option connection restored.")
                    opt_reconnects = 0
                else:
                    print("⚠️  Option connection failed to establish.")

    except KeyboardInterrupt:
        print("\n\n👋 Ctrl+C — stopping…")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback; traceback.print_exc()
    finally:
        spot_stream.disconnect()
        opt_stream.disconnect()
        rows = shared.drain_rows()
        if rows:
            csv_writer.flush(rows)
        print(f"\n✅ Session ended.")
        print(f"   ATM Strike  : {atm_strike}")
        print(f"   TF          : {TF_SECONDS}s")
        print(f"   Rows written: {rows_written}")
        if csv_writer._csv_path:
            print(f"   CSV saved   : {csv_writer._csv_path}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Fatal error: {e}")
        import traceback
        traceback.print_exc()