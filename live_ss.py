import time
import csv
import os
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
CSV_PREFIX    = 'nifty_'

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

    def run(self):
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
            print_trade_summary(self.completed_trades)


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Run strategy (blocks until session ends or Ctrl+C)
    strategy = LiveStrategy()
    strategy.run()