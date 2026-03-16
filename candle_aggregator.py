"""
candle_aggregator.py
====================
Tails the live tick CSV written by T10_CSV_Collector.py and aggregates
raw ticks into OHLC candles of any timeframe, writing results to a new CSV.

Input CSV  (written live by T10):
    timestamp, date, strike_price, option_type, option_ltp

Output CSV (written by this script):
    candle_start, date, strike_price, option_type, open, high, low, close

Candle grid is anchored at 09:15:00 IST — identical logic to T10.
Only COMPLETED candles are written (no partial/live candle output).

Usage:
    # Default 60-second candles, auto-detects today's T10 CSV
    python candle_aggregator.py

    # 30-second candles
    python candle_aggregator.py --tf 30

    # Explicit input file, custom timeframe
    python candle_aggregator.py --input nifty_2026-03-16.csv --tf 180

    # Custom output file
    python candle_aggregator.py --tf 60 --output my_candles.csv

Stop:
    Ctrl+C  (or auto-stops at 15:30 IST)
"""

import csv
import os
import time
import argparse
from datetime import datetime, timezone, timedelta
from collections import defaultdict

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────

DEFAULT_TF_SECONDS = 60          # default candle width in seconds
POLL_INTERVAL      = 0.25        # how often to re-read the CSV (seconds)

IST = timezone(timedelta(hours=5, minutes=30))

# Candle grid anchor — must match T10
_ANCHOR_H = 9
_ANCHOR_M = 15
_ANCHOR_S = 0

INPUT_COLUMNS  = ['timestamp', 'date', 'strike_price', 'option_type', 'option_ltp']
OUTPUT_COLUMNS = ['candle_start', 'date', 'strike_price', 'option_type',
                  'open', 'high', 'low', 'close']


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def get_ist_time() -> datetime:
    return datetime.now(IST)


def is_market_closed() -> bool:
    now = get_ist_time()
    return now.hour > 15 or (now.hour == 15 and now.minute >= 30)


def candle_start(ts: datetime, tf_seconds: int) -> datetime:
    """
    Return the bar-open datetime that *ts* belongs to, anchored at
    09:15:00 IST, repeating every tf_seconds seconds.
    Result is a naive datetime (no tzinfo) for use as a dict key.

    Examples (tf=30, anchor=09:15:00):
        09:15:00 → 09:15:00   09:15:29 → 09:15:00   09:15:30 → 09:15:30
        09:16:59 → 09:16:30   09:18:00 → 09:18:00

    Examples (tf=60):
        09:15:00 → 09:15:00   09:15:59 → 09:15:00   09:16:00 → 09:16:00
    """
    ts_naive = ts.replace(tzinfo=None, microsecond=0)
    anchor   = ts_naive.replace(
        hour=_ANCHOR_H, minute=_ANCHOR_M,
        second=_ANCHOR_S, microsecond=0)

    if ts_naive < anchor:
        return anchor

    elapsed = int((ts_naive - anchor).total_seconds())
    offset  = (elapsed // tf_seconds) * tf_seconds
    return anchor + timedelta(seconds=offset)


def parse_timestamp(ts_str: str) -> datetime:
    """Parse T10 timestamp string (with or without microseconds)."""
    for fmt in ('%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S'):
        try:
            return datetime.strptime(ts_str, fmt)
        except ValueError:
            continue
    raise ValueError(f"Unrecognised timestamp format: {ts_str!r}")


def today_input_path() -> str:
    date_str = get_ist_time().strftime('%Y-%m-%d')
    return f"nifty_{date_str}.csv"


def default_output_path(input_path: str, tf_seconds: int) -> str:
    base = os.path.splitext(input_path)[0]
    return f"{base}_candles_{tf_seconds}s.csv"


# ─────────────────────────────────────────────────────────────────────────────
# CANDLE STATE
# ─────────────────────────────────────────────────────────────────────────────

class CandleState:
    """
    Holds the in-progress OHLC candle for a single (strike, option_type) series.
    Candles are identified by their bar-open datetime (naive).
    """

    def __init__(self):
        # keyed by (strike_price_str, option_type)
        # value: {'bar': naive_dt, 'open': f, 'high': f, 'low': f, 'close': f}
        self._live: dict = {}

    def feed(self, strike: str, otype: str, ltp: float,
             bar_dt: datetime) -> dict | None:
        """
        Feed one tick. Returns a completed candle dict if this tick belongs
        to a NEW bar (i.e. the previous bar just closed), else None.
        """
        key    = (strike, otype)
        closed = None

        if key in self._live:
            existing = self._live[key]
            if bar_dt > existing['bar']:
                # Seal the completed bar
                closed = {
                    'candle_start': existing['bar'].strftime('%Y-%m-%d %H:%M:%S'),
                    'date'        : existing['bar'].strftime('%Y-%m-%d'),
                    'strike_price': strike,
                    'option_type' : otype,
                    'open'        : f"{existing['open']:.2f}",
                    'high'        : f"{existing['high']:.2f}",
                    'low'         : f"{existing['low']:.2f}",
                    'close'       : f"{existing['close']:.2f}",
                }
                self._live[key] = self._new_bar(bar_dt, ltp)
            else:
                # Same bar — update OHLC
                existing['high']  = max(existing['high'], ltp)
                existing['low']   = min(existing['low'],  ltp)
                existing['close'] = ltp
        else:
            self._live[key] = self._new_bar(bar_dt, ltp)

        return closed

    @staticmethod
    def _new_bar(bar_dt: datetime, ltp: float) -> dict:
        return {'bar': bar_dt, 'open': ltp, 'high': ltp, 'low': ltp, 'close': ltp}

    def all_live_keys(self):
        return list(self._live.keys())


# ─────────────────────────────────────────────────────────────────────────────
# CSV TAIL READER
# ─────────────────────────────────────────────────────────────────────────────

class TailReader:
    """
    Reads new rows appended to a CSV file since the last read.
    Tracks byte offset so only genuinely new data is processed.
    Skips the header row automatically.
    """

    def __init__(self, path: str):
        self.path        = path
        self._offset     = 0
        self._header_skipped = False

    def read_new_rows(self) -> list[dict]:
        """Return list of new row dicts since last call. Empty list if none."""
        if not os.path.exists(self.path):
            return []

        rows = []
        with open(self.path, 'r', newline='') as f:
            # Skip header on first open
            if not self._header_skipped:
                f.readline()
                self._offset = f.tell()
                self._header_skipped = True

            f.seek(self._offset)
            reader = csv.DictReader(f, fieldnames=INPUT_COLUMNS)
            for row in reader:
                # Skip blank or malformed rows
                if not row.get('timestamp') or not row.get('option_ltp'):
                    continue
                rows.append(row)
            self._offset = f.tell()

        return rows


# ─────────────────────────────────────────────────────────────────────────────
# CANDLE CSV WRITER
# ─────────────────────────────────────────────────────────────────────────────

class CandleWriter:
    """Appends completed candle rows to the output CSV."""

    def __init__(self, path: str):
        self.path = path
        # Write header if file doesn't exist yet
        if not os.path.exists(path):
            with open(path, 'w', newline='') as f:
                csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS).writeheader()
            print(f"📄 Created output file: {path}")

    def write(self, rows: list[dict]):
        if not rows:
            return
        with open(self.path, 'a', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS,
                                    extrasaction='ignore')
            writer.writerows(rows)
        print(f"💾 Wrote {len(rows)} candle(s) → {self.path}")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN AGGREGATOR LOOP
# ─────────────────────────────────────────────────────────────────────────────

def run(input_path: str, output_path: str, tf_seconds: int):
    print("\n" + "=" * 70)
    print(f"  CANDLE AGGREGATOR")
    print(f"  Input  : {input_path}")
    print(f"  Output : {output_path}")
    print(f"  TF     : {tf_seconds}s  (anchor 09:15:00 IST)")
    print("=" * 70)

    tail         = TailReader(input_path)
    writer       = CandleWriter(output_path)
    state        = CandleState()
    ticks_seen   = 0
    candles_written = 0

    # Wait for input file to appear (T10 may not have started yet)
    if not os.path.exists(input_path):
        print(f"⏳ Waiting for {input_path} to appear…")
        while not os.path.exists(input_path):
            time.sleep(1)
        print(f"✅ Found {input_path} — starting aggregation…\n")

    print("▶️  Tailing live data… (Ctrl+C to stop)\n")

    try:
        while True:
            new_rows = tail.read_new_rows()
            completed = []

            for row in new_rows:
                try:
                    ts     = parse_timestamp(row['timestamp'])
                    ltp    = float(row['option_ltp'])
                    strike = row['strike_price'].strip()
                    otype  = row['option_type'].strip()
                except (ValueError, KeyError):
                    continue   # malformed row — skip silently

                if ltp <= 0:
                    continue

                ticks_seen += 1
                bar_dt = candle_start(ts, tf_seconds)
                closed = state.feed(strike, otype, ltp, bar_dt)
                if closed:
                    completed.append(closed)

            if completed:
                writer.write(completed)
                candles_written += len(completed)

            # Stop gracefully at market close
            if is_market_closed():
                print("\n🔔 Market closed at 15:30 IST — stopping aggregator.")
                break

            time.sleep(POLL_INTERVAL)

    except KeyboardInterrupt:
        print("\n\n👋 Ctrl+C — stopping…")

    print(f"\n✅ Done.")
    print(f"   Ticks processed : {ticks_seen}")
    print(f"   Candles written : {candles_written}")
    print(f"   Output file     : {output_path}")


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Aggregate T10 raw tick CSV into OHLC candles of any timeframe."
    )
    parser.add_argument(
        '--input', '-i',
        default=None,
        help="Path to T10 tick CSV (default: nifty_YYYY-MM-DD.csv for today)"
    )
    parser.add_argument(
        '--tf', '-t',
        type=int,
        default=DEFAULT_TF_SECONDS,
        help=f"Candle width in seconds (default: {DEFAULT_TF_SECONDS})"
    )
    parser.add_argument(
        '--output', '-o',
        default=None,
        help="Output candle CSV path (default: <input>_candles_<tf>s.csv)"
    )
    args = parser.parse_args()

    if args.tf < 1:
        print("❌ --tf must be >= 1 second.")
        return

    input_path  = args.input  or today_input_path()
    output_path = args.output or default_output_path(input_path, args.tf)

    run(input_path, output_path, args.tf)


if __name__ == "__main__":
    main()