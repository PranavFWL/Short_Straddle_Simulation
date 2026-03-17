"""
backtest.py  —  Short Straddle Backtest + Live Upstox Fetch (single file)

─────────────────────────────────────────────────────────────────────────────
MODE 1 — Live fetch + backtest  (all four flags required, no CSV written)
─────────────────────────────────────────────────────────────────────────────
    python backtest.py --date 2026-03-17 --strike 23450 --start 11:36 --end 13:06
    python backtest.py --date 2026-03-17 --strike 23450 --start 09:16 --end 15:15 --sl 40 --target 35

─────────────────────────────────────────────────────────────────────────────
MODE 2 — Classic backtest from CSV / DuckDB  (original behaviour, unchanged)
─────────────────────────────────────────────────────────────────────────────
    python backtest.py                          # all CSV/DB dates
    python backtest.py --date 2026-03-17        # one date from CSV/DB
    python backtest.py --time 09:20 --sl 40     # custom params, all dates
"""

import duckdb
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import time, datetime, timedelta
from tabulate import tabulate
import argparse
import os
import sys
import glob
import requests


# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────

SL_POINTS     = 15
TARGET_POINTS = 15
LOT_SIZE      = 65
COST_PERCENT  = 0.0025

TF_SECONDS    = 60
CSV_COMBINED  = 'nifty_{date}.csv'
DB_FILE       = 'gdfl_data.duckdb'

TOKEN_FILE      = "upstox_token.txt"
BASE_URL        = "https://api.upstox.com/v2"
NIFTY_INDEX_KEY = "NSE_INDEX|Nifty 50"


# ─────────────────────────────────────────────────────────────────────────────
# UPSTOX FETCH HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _load_token() -> str:
    if not os.path.exists(TOKEN_FILE):
        sys.exit(f"[ERROR] Token file '{TOKEN_FILE}' not found in current directory.")
    token = open(TOKEN_FILE).read().strip()
    if not token:
        sys.exit(f"[ERROR] Token file '{TOKEN_FILE}' is empty.")
    return token


def _api_headers(token: str) -> dict:
    return {
        "Content-Type":  "application/json",
        "Accept":        "application/json",
        "Authorization": f"Bearer {token}",
    }


def _nearest_expiry(token: str, trade_date: str) -> str:
    """Return the nearest expiry on or after trade_date."""
    resp = requests.get(
        f"{BASE_URL}/option/contract",
        headers=_api_headers(token),
        params={"instrument_key": NIFTY_INDEX_KEY},
        timeout=10,
    )
    resp.raise_for_status()
    contracts = resp.json().get("data", [])
    if not contracts:
        sys.exit("[ERROR] No expiry dates returned from Upstox.")

    trade_dt = datetime.strptime(trade_date, "%Y-%m-%d").date()
    expiry_dates = set()
    for c in contracts:
        exp = c.get("expiry") if isinstance(c, dict) else c
        if exp:
            expiry_dates.add(exp)

    future = sorted(
        e for e in expiry_dates
        if datetime.strptime(e, "%Y-%m-%d").date() >= trade_dt
    )
    if not future:
        sys.exit(f"[ERROR] No expiry on or after {trade_date}.")
    print(f"[FETCH] Nearest expiry: {future[0]}")
    return future[0]


def _option_chain(token: str, expiry: str) -> list:
    resp = requests.get(
        f"{BASE_URL}/option/chain",
        headers=_api_headers(token),
        params={"instrument_key": NIFTY_INDEX_KEY, "expiry_date": expiry},
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json().get("data", [])


def _strike_keys(chain: list, strike: float) -> dict:
    for item in chain:
        if float(item.get("strike_price", -1)) == strike:
            ce_key = (item.get("call_options") or {}).get("instrument_key")
            pe_key = (item.get("put_options")  or {}).get("instrument_key")
            if not ce_key or not pe_key:
                sys.exit(f"[ERROR] CE or PE instrument_key missing for strike {strike}.")
            return {"CE": ce_key, "PE": pe_key}
    sys.exit(
        f"[ERROR] Strike {int(strike)} not found in option chain. "
        "Verify the strike is valid for this expiry."
    )


def _intraday_candles(token: str, instrument_key: str) -> list:
    encoded = requests.utils.quote(instrument_key, safe="")
    resp = requests.get(
        f"{BASE_URL}/historical-candle/intraday/{encoded}/1minute",
        headers=_api_headers(token),
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json().get("data", {}).get("candles", [])


def _candles_to_df(candles: list, strike: float, opt_type: str) -> pd.DataFrame:
    """API candle: [timestamp, open, high, low, close, volume, oi]"""
    rows = []
    for c in candles:
        dt = datetime.fromisoformat(c[0]).replace(tzinfo=None)
        rows.append({
            "datetime":     dt,
            "strike_price": strike,
            "option_type":  "CALL" if opt_type == "CE" else "PUT",
            "open":         float(c[1]),
            "high":         float(c[2]),
            "low":          float(c[3]),
            "close":        float(c[4]),
        })
    df = pd.DataFrame(rows)
    return df.sort_values("datetime").reset_index(drop=True)


def fetch_data_in_memory(trade_date: str, strike: float,
                         start_time: str, end_time: str) -> pd.DataFrame:
    """
    Fetch CE + PE 1-min candles from Upstox, filter to [start_time, end_time],
    return a combined DataFrame — nothing written to disk.
    """
    print(f"\n[FETCH] Date={trade_date}  Strike={int(strike)}  "
          f"Window={start_time}–{end_time}")

    token  = _load_token()
    expiry = _nearest_expiry(token, trade_date)
    chain  = _option_chain(token, expiry)
    if not chain:
        sys.exit("[ERROR] Empty option chain returned.")

    keys = _strike_keys(chain, strike)
    print(f"[FETCH] CE key : {keys['CE']}")
    print(f"[FETCH] PE key : {keys['PE']}")

    print("[FETCH] Downloading CE candles ...")
    ce_candles = _intraday_candles(token, keys["CE"])
    print(f"[FETCH] CE candles received: {len(ce_candles)}")

    print("[FETCH] Downloading PE candles ...")
    pe_candles = _intraday_candles(token, keys["PE"])
    print(f"[FETCH] PE candles received: {len(pe_candles)}")

    if not ce_candles and not pe_candles:
        sys.exit("[ERROR] No candle data returned for CE or PE.")

    df_all = pd.concat(
        [_candles_to_df(ce_candles, strike, "CE"),
         _candles_to_df(pe_candles, strike, "PE")],
        ignore_index=True,
    )
    df_all["datetime"] = pd.to_datetime(df_all["datetime"])

    t_start = datetime.strptime(start_time, "%H:%M").time()
    t_end   = datetime.strptime(end_time,   "%H:%M").time()
    mask    = (df_all["datetime"].dt.time >= t_start) & \
              (df_all["datetime"].dt.time <= t_end)
    df_all  = df_all[mask].sort_values("datetime").reset_index(drop=True)

    if df_all.empty:
        sys.exit(f"[ERROR] No data in window {start_time}–{end_time} "
                 f"for {trade_date} strike {int(strike)}.")

    n_ce = (df_all["option_type"] == "CALL").sum()
    n_pe = (df_all["option_type"] == "PUT").sum()
    print(f"[FETCH] Rows after time filter: {len(df_all)}  "
          f"(CE: {n_ce}, PE: {n_pe})\n")
    return df_all


# ─────────────────────────────────────────────────────────────────────────────
# PnL HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _leg_cost(entry, exit_price):
    return (entry + exit_price) * COST_PERCENT


def _leg_pnl_time(entry, exit_price):
    return (entry - exit_price) - _leg_cost(entry, exit_price)


def _atm(spot):
    return round(spot / 50) * 50


# ─────────────────────────────────────────────────────────────────────────────
# BAR-ALIGNMENT HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _bar_open_for(ts, entry_h, entry_m, tf_seconds):
    anchor  = ts.replace(hour=entry_h, minute=entry_m, second=0, microsecond=0)
    if ts < anchor:
        return anchor
    elapsed = int((ts - anchor).total_seconds())
    return anchor + timedelta(seconds=(elapsed // tf_seconds) * tf_seconds)


def _next_bar_open(ts, entry_h, entry_m, tf_seconds):
    return _bar_open_for(ts, entry_h, entry_m, tf_seconds) + timedelta(seconds=tf_seconds)


# ─────────────────────────────────────────────────────────────────────────────
# SUMMARY TABLE
# ─────────────────────────────────────────────────────────────────────────────

def print_trade_summary(completed_trades):
    if not completed_trades:
        print("\nNo completed trades to display.")
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
    print("  BACKTEST TRADE SUMMARY  (completed trades only)")
    print("=" * 110)
    print(tabulate(rows, headers=headers, tablefmt="rounded_outline",
                   stralign="center", numalign="center"))

    total  = len(completed_trades)
    wins   = sum(1 for t in completed_trades if t['trade_pnl_val'] > 0)
    total_pnl = sum(t['trade_pnl_val'] for t in completed_trades)
    best   = max(completed_trades, key=lambda t: t['trade_pnl_val'])
    worst  = min(completed_trades, key=lambda t: t['trade_pnl_val'])

    print(f"\n  Total Trades  : {total}")
    print(f"  Winners       : {wins}  ({wins/total*100:.1f}%)")
    print(f"  Losers        : {total-wins}  ({(total-wins)/total*100:.1f}%)")
    print(f"  Best Trade    : ₹{best['trade_pnl_val']:+.2f}  (entered {best['ce_entry_time']})")
    print(f"  Worst Trade   : ₹{worst['trade_pnl_val']:+.2f}  (entered {worst['ce_entry_time']})")
    print(f"  Total Net PnL : ₹{total_pnl:+,.2f}")
    print("=" * 110 + "\n")


# ─────────────────────────────────────────────────────────────────────────────
# DATA LOADING  (CSV / DuckDB — original paths, used in Mode 2)
# ─────────────────────────────────────────────────────────────────────────────

def _load_from_combined_csv(date_str, entry_sql_time):
    csv_file = CSV_COMBINED.format(date=date_str)
    if not os.path.exists(csv_file):
        return None, None

    df = pd.read_csv(csv_file, parse_dates=['datetime'])
    df['datetime'] = pd.to_datetime(df['datetime'])

    required = {'datetime', 'strike_price', 'option_type', 'open', 'high', 'low', 'close'}
    missing  = required - set(df.columns)
    if missing:
        print(f"  ❌ CSV missing columns: {missing}")
        return None, None

    entry_time_obj = datetime.strptime(entry_sql_time, '%H:%M:%S').time()
    df = df[
        (df['datetime'].dt.time >= entry_time_obj) &
        (df['datetime'].dt.time <= time(15, 15))
    ].sort_values('datetime').reset_index(drop=True)

    if df.empty:
        return None, None

    spot_df = df[['datetime']].drop_duplicates(subset='datetime').reset_index(drop=True)
    opts_df = df[['datetime', 'strike_price', 'option_type',
                  'open', 'high', 'low', 'close']].copy()
    return spot_df, opts_df


def _load_from_db(con, date_str, entry_sql_time):
    expiry_res = con.execute(f"""
        SELECT MIN(expiry_date) FROM options_data
        WHERE date = '{date_str}' AND expiry_date >= '{date_str}'
    """).fetchone()
    if not expiry_res or not expiry_res[0]:
        return None, None
    expiry = expiry_res[0]

    spot_df = con.execute(f"""
        SELECT datetime FROM spot_data
        WHERE date = '{date_str}'
          AND cast(datetime as time) >= '{entry_sql_time}'
          AND cast(datetime as time) <= '15:30:00'
        ORDER BY datetime
    """).fetchdf()

    opts_df = con.execute(f"""
        SELECT datetime, strike_price, option_type, open, high, low, close
        FROM options_data
        WHERE date = '{date_str}' AND expiry_date = '{expiry}'
          AND cast(datetime as time) >= '{entry_sql_time}'
          AND cast(datetime as time) <= '15:30:00'
        ORDER BY datetime
    """).fetchdf()

    spot_df['datetime'] = pd.to_datetime(spot_df['datetime'])
    opts_df['datetime'] = pd.to_datetime(opts_df['datetime'])
    return spot_df, opts_df


def _get_csv_dates():
    dates = []
    for f in sorted(glob.glob(CSV_COMBINED.replace('{date}', '*'))):
        base     = os.path.basename(f)
        date_str = base.replace('nifty_', '').replace('.csv', '')
        try:
            datetime.strptime(date_str, '%Y-%m-%d')
            dates.append(date_str)
        except ValueError:
            continue
    return dates


def _get_db_dates(con):
    return [
        d.strftime('%Y-%m-%d') if hasattr(d, 'strftime') else str(d)
        for d in con.execute(
            "SELECT DISTINCT date FROM spot_data ORDER BY date"
        ).fetchdf()['date'].tolist()
    ]


def _prepare_injected(df: pd.DataFrame, entry_sql_time: str):
    """Convert in-memory live DataFrame into (spot_df, opts_df)."""
    df = df.copy()
    df['datetime'] = pd.to_datetime(df['datetime'])

    entry_time_obj = datetime.strptime(entry_sql_time, '%H:%M:%S').time()
    df = df[
        (df['datetime'].dt.time >= entry_time_obj) &
        (df['datetime'].dt.time <= time(15, 15))
    ].sort_values('datetime').reset_index(drop=True)

    if df.empty:
        return None, None

    spot_df = df[['datetime']].drop_duplicates(subset='datetime').reset_index(drop=True)
    opts_df = df[['datetime', 'strike_price', 'option_type',
                  'open', 'high', 'low', 'close']].copy()
    return spot_df, opts_df


# ─────────────────────────────────────────────────────────────────────────────
# MAIN BACKTEST ENGINE
# ─────────────────────────────────────────────────────────────────────────────

def run_backtest(entry_time_str='09:16', sl_points=SL_POINTS,
                 target_points=TARGET_POINTS, date_filter=None,
                 tf_seconds=TF_SECONDS, quiet=False,
                 injected_df: pd.DataFrame = None):
    """
    injected_df : pre-built in-memory DataFrame from live Upstox fetch.
                  When supplied, all CSV / DuckDB loading is skipped.
                  Required columns: datetime, strike_price, option_type,
                                    open, high, low, close
    """
    try:
        entry_h, entry_m = map(int, entry_time_str.split(':'))
        entry_sql_time   = f"{entry_time_str}:00"
    except ValueError:
        print("Invalid time format. Use HH:MM")
        return pd.DataFrame(), []

    if date_filter:
        try:
            datetime.strptime(date_filter, '%Y-%m-%d')
        except ValueError:
            print("Invalid date format. Use YYYY-MM-DD")
            return pd.DataFrame(), []

    # ── Build date→source list ─────────────────────────────────────────────────
    if injected_df is not None:
        date_sources = [(date_filter, 'injected')]
    else:
        date_sources = []
        if date_filter:
            csv_file = CSV_COMBINED.format(date=date_filter)
            if os.path.exists(csv_file):
                date_sources.append((date_filter, 'csv'))
                if not quiet:
                    print(f"  📄 {date_filter} → {csv_file}")
            else:
                date_sources.append((date_filter, 'db'))
                if not quiet:
                    print(f"  🗄️  {date_filter} → DuckDB")
        else:
            csv_dates = set(_get_csv_dates())
            db_dates  = set()
            if os.path.exists(DB_FILE):
                _con = duckdb.connect(DB_FILE, read_only=True)
                db_dates = set(_get_db_dates(_con))
                _con.close()
            for d in sorted(csv_dates | db_dates):
                date_sources.append((d, 'csv' if d in csv_dates else 'db'))

        if not date_sources:
            print("No data found. Check CSV files or DuckDB.")
            return pd.DataFrame(), []

    con = None
    if any(s == 'db' for _, s in date_sources):
        if os.path.exists(DB_FILE):
            con = duckdb.connect(DB_FILE, read_only=True)
        else:
            print(f"⚠️  DuckDB '{DB_FILE}' not found — skipping DB dates.")
            date_sources = [(d, s) for d, s in date_sources if s == 'csv']

    if not quiet:
        if injected_df is not None:
            src_label = f"Live API  ({date_filter}  strike auto-detected)"
        else:
            src_label = date_filter if date_filter else f"ALL ({len(date_sources)} days)"
        print(f"\nFound {len(date_sources)} trading day(s).")
        print(f"Parameters : Date   = {src_label}")
        print(f"           : Entry  = {entry_time_str}")
        print(f"           : SL     = {sl_points} pts/leg (fixed deduction)")
        print(f"           : Target = {target_points} pts/leg (fixed)")
        print(f"           : TF     = {tf_seconds}s (re-entry grid)")
        print(f"           : Lot    = {LOT_SIZE}\n")

    daily_results    = []
    completed_trades = []
    trade_counter    = 0

    for date_str, source in date_sources:
        try:
            # ── Load data ──────────────────────────────────────────────────────
            if source == 'injected':
                spot_day, opts_day = _prepare_injected(injected_df, entry_sql_time)
            elif source == 'csv':
                spot_day, opts_day = _load_from_combined_csv(date_str, entry_sql_time)
            else:
                spot_day, opts_day = _load_from_db(con, date_str, entry_sql_time)

            if spot_day is None or spot_day.empty:
                if not quiet:
                    print(f"  ⚠️  No spot data for {date_str} from {entry_time_str}")
                continue
            if opts_day is None or opts_day.empty:
                if not quiet:
                    print(f"  ⚠️  No options data for {date_str} from {entry_time_str}")
                continue

            opts_day['dt'] = pd.to_datetime(opts_day['datetime'])
            opts_day = opts_day.drop_duplicates(
                subset=['dt', 'strike_price', 'option_type'], keep='last'
            )
            opts_idx = opts_day.set_index(['dt', 'strike_price', 'option_type'])

            # ── Strike resolution ──────────────────────────────────────────────
            unique_strikes = opts_day['strike_price'].unique()
            if source in ('injected', 'csv') and len(unique_strikes) == 1:
                fixed_strike = float(unique_strikes[0])
                label = "live fetch" if source == 'injected' else "CSV"
                if not quiet:
                    print(f"  🔒 Fixed strike from {label}: {fixed_strike}")
            else:
                fixed_strike     = None
                day_first_strike = None

            def get_candle(candle_dt, strike, opt_type):
                try:
                    return opts_idx.loc[(candle_dt, float(strike), opt_type)]
                except KeyError:
                    return None

            # ── Per-day state ──────────────────────────────────────────────────
            active        = None
            day_trades    = []
            reentry_after = None

            for _, spot_row in spot_day.iterrows():
                candle_dt      = pd.to_datetime(spot_row['datetime'])
                candle_time    = candle_dt.time()
                is_exit_candle = (candle_time == time(15, 15))

                # Strike for this candle
                if source in ('injected', 'csv'):
                    strike_to_use = fixed_strike
                else:
                    if day_first_strike is None:
                        day_first_strike = _atm(spot_open)
                        if not quiet:
                            print(f"  🔒 Fixed strike from DB (first candle): {day_first_strike}")
                    strike_to_use = day_first_strike

                # ── Open trade ─────────────────────────────────────────────────
                if active is None:
                    if reentry_after is not None:
                        if candle_dt.replace(tzinfo=None) < reentry_after:
                            continue
                    reentry_after = None

                    ce_c = get_candle(candle_dt, strike_to_use, 'CALL')
                    pe_c = get_candle(candle_dt, strike_to_use, 'PUT')
                    if ce_c is None or pe_c is None:
                        if not quiet:
                            print(f"  ⚠️  No candle for strike {strike_to_use} at "
                                  f"{candle_dt.strftime('%H:%M')} — skipping")
                        continue

                    trade_counter += 1
                    active = {
                        'trade_num'     : trade_counter,
                        'ce_strike'     : strike_to_use,
                        'pe_strike'     : strike_to_use,
                        'ce_entry'      : float(ce_c['open']),
                        'pe_entry'      : float(pe_c['open']),
                        'ce_entry_time' : candle_dt.strftime('%H:%M'),
                        'pe_entry_time' : candle_dt.strftime('%H:%M'),
                    }
                    active['ce_sl']     = active['ce_entry'] + sl_points
                    active['pe_sl']     = active['pe_entry'] + sl_points
                    active['ce_target'] = active['ce_entry'] - target_points
                    active['pe_target'] = active['pe_entry'] - target_points

                    if not quiet:
                        print(f"  ➤ TRADE #{trade_counter} OPEN  "
                              f"[{date_str} {candle_dt.strftime('%H:%M')}] "
                              f"CE {strike_to_use} @ {active['ce_entry']:.2f}  "
                              f"SL {active['ce_sl']:.2f}  Tgt {active['ce_target']:.2f}  |  "
                              f"PE {strike_to_use} @ {active['pe_entry']:.2f}  "
                              f"SL {active['pe_sl']:.2f}  Tgt {active['pe_target']:.2f}")

                # ── Fetch current candle data ───────────────────────────────────
                ce_c = get_candle(candle_dt, active['ce_strike'], 'CALL')
                pe_c = get_candle(candle_dt, active['pe_strike'], 'PUT')
                if ce_c is None or pe_c is None:
                    continue

                ce_open_c = float(ce_c['open'])
                ce_high   = float(ce_c['high'])
                ce_low    = float(ce_c['low'])
                ce_close  = float(ce_c['close'])
                pe_open_c = float(pe_c['open'])
                pe_high   = float(pe_c['high'])
                pe_low    = float(pe_c['low'])
                pe_close  = float(pe_c['close'])

                # ── Per-candle status ──────────────────────────────────────────
                if not quiet:
                    ce_live_pts    = active['ce_entry'] - ce_close
                    pe_live_pts    = active['pe_entry'] - pe_close
                    trade_live_val = (ce_live_pts + pe_live_pts) * LOT_SIZE
                    closed_pnl     = sum(t['trade_pnl_val'] for t in day_trades)
                    print(
                        f"  [{date_str} {candle_dt.strftime('%H:%M')}]  "
                        f"Trade#{active['trade_num']}  "
                        f"CE({active['ce_strike']}): C={ce_close:.2f} | "
                        f"PE({active['pe_strike']}): C={pe_close:.2f} | "
                        f"Unrealised PnL: ₹{trade_live_val:+.2f} | "
                        f"Day PnL: ₹{closed_pnl + trade_live_val:+.2f}"
                    )

                exit_reason = None
                ce_exit_px  = ce_pnl_pts = None
                pe_exit_px  = pe_pnl_pts = None
                do_reenter  = False

                # ── Time exit ─────────────────────────────────────────────────
                if is_exit_candle:
                    exit_reason = 'Time'
                    ce_exit_px  = ce_close
                    pe_exit_px  = pe_close
                    ce_pnl_pts  = _leg_pnl_time(active['ce_entry'], ce_exit_px)
                    pe_pnl_pts  = _leg_pnl_time(active['pe_entry'], pe_exit_px)

                else:
                    sl_hit_ce  = ce_high >= active['ce_sl']
                    sl_hit_pe  = pe_high >= active['pe_sl']
                    sl_hit     = sl_hit_ce or sl_hit_pe
                    tgt_hit_ce = ce_low <= active['ce_target']
                    tgt_hit_pe = pe_low <= active['pe_target']
                    target_hit = tgt_hit_ce or tgt_hit_pe

                    if target_hit and sl_hit:
                        same_leg = (sl_hit_ce and tgt_hit_ce) or (sl_hit_pe and tgt_hit_pe)
                        if same_leg:
                            if sl_hit_ce and tgt_hit_ce:
                                dist_sl  = active['ce_sl']     - ce_open_c
                                dist_tgt = ce_open_c - active['ce_target']
                            else:
                                dist_sl  = active['pe_sl']     - pe_open_c
                                dist_tgt = pe_open_c - active['pe_target']
                            exit_reason = 'SL' if dist_sl <= dist_tgt else 'Target'
                            if not quiet:
                                print(f"  ⚡ Conflict at {candle_dt.strftime('%H:%M')} — "
                                      f"dist_sl={dist_sl:.2f}  dist_tgt={dist_tgt:.2f}  "
                                      f"→ {exit_reason} wins")
                        else:
                            exit_reason = 'SL'
                            if not quiet:
                                print(f"  ⚡ Cross-leg at {candle_dt.strftime('%H:%M')} — "
                                      "SL on one leg, Target on other → SL wins")
                        do_reenter = True

                    elif target_hit:
                        exit_reason = 'Target'
                        do_reenter  = True
                    elif sl_hit:
                        exit_reason = 'SL'
                        do_reenter  = True

                    if exit_reason == 'Target':
                        ce_exit_px = active['ce_entry'] - target_points
                        pe_exit_px = active['pe_entry'] - target_points
                        ce_pnl_pts = target_points - _leg_cost(active['ce_entry'], ce_exit_px)
                        pe_pnl_pts = target_points - _leg_cost(active['pe_entry'], pe_exit_px)
                    elif exit_reason == 'SL':
                        ce_exit_px = active['ce_sl']
                        pe_exit_px = active['pe_sl']
                        ce_pnl_pts = -sl_points - _leg_cost(active['ce_entry'], ce_exit_px)
                        pe_pnl_pts = -sl_points - _leg_cost(active['pe_entry'], pe_exit_px)

                if exit_reason:
                    ce_pnl_val = ce_pnl_pts * LOT_SIZE
                    pe_pnl_val = pe_pnl_pts * LOT_SIZE
                    trade_val  = ce_pnl_val + pe_pnl_val

                    record = {
                        'trade_num'     : active['trade_num'],
                        'date'          : date_str,
                        'ce_strike'     : active['ce_strike'],
                        'ce_entry_time' : active['ce_entry_time'],
                        'ce_exit_time'  : candle_dt.strftime('%H:%M'),
                        'ce_entry'      : active['ce_entry'],
                        'ce_exit'       : ce_exit_px,
                        'ce_pnl_pts'    : ce_pnl_pts,
                        'ce_pnl_val'    : ce_pnl_val,
                        'pe_strike'     : active['pe_strike'],
                        'pe_entry_time' : active['pe_entry_time'],
                        'pe_exit_time'  : candle_dt.strftime('%H:%M'),
                        'pe_entry'      : active['pe_entry'],
                        'pe_exit'       : pe_exit_px,
                        'pe_pnl_pts'    : pe_pnl_pts,
                        'pe_pnl_val'    : pe_pnl_val,
                        'trade_pnl_pts' : ce_pnl_pts + pe_pnl_pts,
                        'trade_pnl_val' : trade_val,
                        'exit_reason'   : exit_reason,
                    }
                    day_trades.append(record)
                    completed_trades.append(record)

                    prev_active = active
                    active = None

                    if not quiet:
                        icon = "🎯" if exit_reason == 'Target' else \
                               "🔴" if exit_reason == 'SL' else "🏁"
                        print(f"  {icon} TRADE #{record['trade_num']} CLOSED | "
                              f"{exit_reason} | "
                              f"CE entry {prev_active['ce_entry']:.2f} exit {ce_exit_px:.2f}  "
                              f"PE entry {record['pe_entry']:.2f} exit {pe_exit_px:.2f} | "
                              f"Trade PnL: ₹{trade_val:+.2f} "
                              f"({'fixed ±pts' if exit_reason != 'Time' else 'actual prices'})")

                    if not do_reenter:
                        break
                    reentry_after = _next_bar_open(
                        candle_dt.replace(tzinfo=None), entry_h, entry_m, tf_seconds)
                    continue

            # ── Daily PnL ──────────────────────────────────────────────────────
            day_pnl = sum(t['trade_pnl_val'] for t in day_trades)
            if day_trades:
                daily_results.append({
                    'date'       : date_str,
                    'pnl_value'  : day_pnl,
                    'num_trades' : len(day_trades),
                })

        except Exception as e:
            print(f"Error on {date_str}: {e}")
            import traceback; traceback.print_exc()
            continue

    if con:
        con.close()

    # ── Equity curve & reporting ───────────────────────────────────────────────
    res_df = pd.DataFrame(daily_results)

    if not res_df.empty:
        res_df['cumulative_pnl'] = res_df['pnl_value'].cumsum()
        res_df['peak_pnl']       = res_df['cumulative_pnl'].cummax()
        res_df['drawdown']       = res_df['cumulative_pnl'] - res_df['peak_pnl']
        max_drawdown             = res_df['drawdown'].min()

        if not quiet:
            print("\nBacktest Summary:")
            print(f"  Total Days Traded  : {len(res_df)}")
            print(f"  Total PnL          : ₹{res_df['pnl_value'].sum():+,.2f}")
            print(f"  Win Rate (days)    : {(res_df['pnl_value'] > 0).mean()*100:.2f}%")
            print(f"  Avg PnL per Day    : ₹{res_df['pnl_value'].mean():+,.2f}")
            print(f"  Max Profit (daily) : ₹{res_df['pnl_value'].max():+,.2f}")
            print(f"  Max Loss (daily)   : ₹{res_df['pnl_value'].min():+,.2f}")
            print(f"  Max Drawdown       : ₹{max_drawdown:+,.2f}")

            res_df.to_csv('backtest_results.csv', index=False)
            print("\n  Results → backtest_results.csv")

            if len(res_df) > 1:
                res_df['date'] = pd.to_datetime(res_df['date'])
                monthly = (res_df.set_index('date')
                                 .resample('ME')['pnl_value'].sum().reset_index())
                monthly['Year']  = monthly['date'].dt.year
                monthly['Month'] = monthly['date'].dt.strftime('%b')
                pivot = monthly.pivot(index='Year', columns='Month', values='pnl_value')
                month_order = ['Jan','Feb','Mar','Apr','May','Jun',
                               'Jul','Aug','Sep','Oct','Nov','Dec']
                pivot = pivot.reindex(columns=month_order)
                base_capital = 300000
                yearly_pnl   = res_df.groupby(res_df['date'].dt.year)['pnl_value'].sum()
                yearly_ret   = (yearly_pnl / base_capital) * 100
                yearly_mdd   = {}
                for yr, grp in res_df.groupby(res_df['date'].dt.year):
                    cp = grp['pnl_value'].cumsum()
                    yearly_mdd[yr] = (cp - cp.cummax()).min()
                pivot['Total PnL'] = yearly_pnl
                pivot['Return %']  = yearly_ret.map('{:.2f}%'.format)
                pivot['MDD']       = pd.Series(yearly_mdd)
                pivot = pivot.fillna(0)
                print("\nMonthly PnL Table (Base Capital: ₹3L):")
                pd.set_option('display.max_columns', None)
                pd.set_option('display.width', 1200)
                print(pivot)

                pivot_pct = pivot.copy()
                for m in month_order:
                    if m in pivot_pct.columns:
                        pivot_pct[m] = pivot_pct[m].apply(
                            lambda v: f"{v:.2f} ({(v/base_capital)*100:.2f}%)"
                            if isinstance(v, (int, float)) else v)
                pivot_pct['MDD'] = pivot_pct['MDD'].apply(
                    lambda v: f"{v:.2f} ({(v/base_capital)*100:.2f}%)"
                    if isinstance(v, (int, float)) else v)
                pivot_pct.to_csv('monthly_pnl_matrix.csv')
                print("  Monthly matrix → monthly_pnl_matrix.csv")

                plt.figure(figsize=(12, 8))
                plt.subplot(2, 1, 1)
                plt.plot(res_df['date'], res_df['cumulative_pnl'],
                         label='Equity Curve', color='blue')
                plt.title('Equity Curve (Fixed 1 Lot | SL + Target Re-Entry)')
                plt.ylabel('Cumulative PnL (₹)')
                plt.grid(True); plt.legend()
                plt.subplot(2, 1, 2)
                plt.fill_between(res_df['date'], res_df['drawdown'], 0,
                                 color='red', alpha=0.3, label='Drawdown')
                plt.plot(res_df['date'], res_df['drawdown'], color='red', alpha=0.6)
                plt.title('Drawdown Curve')
                plt.ylabel('Drawdown (₹)'); plt.xlabel('Date')
                plt.grid(True); plt.legend()
                plt.tight_layout()
                plt.savefig('backtest_performance.png')
                print("  Chart → backtest_performance.png")

            print_trade_summary(completed_trades)
    else:
        if not quiet:
            print("No trades generated.")

    return res_df, completed_trades


# ─────────────────────────────────────────────────────────────────────────────
# CLI  —  smart arg parser handles both modes in one file
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "Short Straddle Backtest\n\n"
            "MODE 1 — Live fetch + backtest (--strike and --start and --end all required):\n"
            "  python backtest.py --date 2026-03-17 --strike 23450 --start 11:36 --end 13:06\n\n"
            "MODE 2 — Classic CSV/DB backtest (original behaviour):\n"
            "  python backtest.py --date 2026-03-17\n"
            "  python backtest.py --time 09:20 --sl 40\n"
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )

    # Live-fetch flags (Mode 1)
    parser.add_argument('--strike', type=float, default=None,
                        help='[Mode 1] Strike price  e.g. 23450')
    parser.add_argument('--start',  type=str,   default=None,
                        help='[Mode 1] Data/entry start time  HH:MM')
    parser.add_argument('--end',    type=str,   default=None,
                        help='[Mode 1] Data end time  HH:MM')

    # Shared flags
    parser.add_argument('--date',   type=str,   default=None,
                        help='Trading date YYYY-MM-DD')
    parser.add_argument('--sl',     type=int,   default=SL_POINTS,
                        help=f'SL points per leg (default {SL_POINTS})')
    parser.add_argument('--target', type=int,   default=TARGET_POINTS,
                        help=f'Target points per leg (default {TARGET_POINTS})')
    parser.add_argument('--tf',     type=int,   default=TF_SECONDS,
                        help=f'Re-entry bar size seconds (default {TF_SECONDS})')
    parser.add_argument('--quiet',  action='store_true',
                        help='Suppress per-candle log output')

    # Mode 2 only
    parser.add_argument('--time',   type=str,   default=None,
                        help='[Mode 2] Entry time HH:MM (default 09:16); '
                             'in Mode 1 --start is used instead')

    args = parser.parse_args()

    # ── Detect mode ────────────────────────────────────────────────────────────
    live_flags = [args.strike, args.start, args.end]
    any_live   = any(f is not None for f in live_flags)
    all_live   = all(f is not None for f in live_flags)

    if any_live:
        # ── MODE 1 — Live fetch ────────────────────────────────────────────────
        if not all_live:
            missing = [n for n, v in [('--strike', args.strike),
                                       ('--start',  args.start),
                                       ('--end',    args.end)] if v is None]
            parser.error(
                f"Live fetch mode requires --strike, --start, and --end.\n"
                f"Missing: {', '.join(missing)}"
            )
        if args.date is None:
            parser.error("--date is required in live fetch mode.")

        # Validate formats
        try:
            datetime.strptime(args.date,  '%Y-%m-%d')
        except ValueError:
            parser.error("--date must be YYYY-MM-DD")
        for flag, val in [('--start', args.start), ('--end', args.end)]:
            try:
                datetime.strptime(val, '%H:%M')
            except ValueError:
                parser.error(f"{flag} must be HH:MM")
        if datetime.strptime(args.start, '%H:%M') >= datetime.strptime(args.end, '%H:%M'):
            parser.error("--start must be earlier than --end")

        live_df = fetch_data_in_memory(
            trade_date=args.date,
            strike=args.strike,
            start_time=args.start,
            end_time=args.end,
        )
        run_backtest(
            entry_time_str=args.start,
            sl_points=args.sl,
            target_points=args.target,
            date_filter=args.date,
            tf_seconds=args.tf,
            quiet=args.quiet,
            injected_df=live_df,
        )

    else:
        # ── MODE 2 — Classic CSV / DuckDB ─────────────────────────────────────
        entry_time = args.time if args.time else '09:16'
        run_backtest(
            entry_time_str=entry_time,
            sl_points=args.sl,
            target_points=args.target,
            date_filter=args.date,
            tf_seconds=args.tf,
            quiet=args.quiet,
        )