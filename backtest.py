import duckdb
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import time, datetime
from tabulate import tabulate
import argparse
import os
import glob

# ─────────────────────────────────────────────────────────────────────────────
# COMBINED CSV FORMAT
# ─────────────────────────────────────────────────────────────────────────────
#
#   Filename : nifty_YYYY-MM-DD.csv
#   One row per strike per option_type per minute.
#   Spot open is repeated on every row for that minute (denormalized).
#
#   REQUIRED COLUMNS (exact names, case-sensitive):
#
#   datetime      YYYY-MM-DD HH:MM:SS   Candle timestamp
#   date          YYYY-MM-DD            Trading date
#   spot_open     float                 Nifty50 spot candle open (e.g. 25463.35)
#   expiry_date   YYYY-MM-DD            Option expiry date (e.g. 2026-03-02)
#   strike_price  float                 Strike (e.g. 25450.0)
#   option_type   string                Exactly 'CALL' or 'PUT'
#   open          float                 Option candle open   → entry price
#   high          float                 Option candle high   → SL check
#   low           float                 Option candle low    → target check
#   close         float                 Option candle close  → time exit
#
#   SAMPLE:
#   datetime,date,spot_open,expiry_date,strike_price,option_type,open,high,low,close
#   2026-02-25 09:16:00,2026-02-25,25463.35,2026-03-02,25450.0,CALL,166.85,167.20,166.45,166.90
#   2026-02-25 09:16:00,2026-02-25,25463.35,2026-03-02,25450.0,PUT,136.55,137.25,136.45,137.25
#
# ─────────────────────────────────────────────────────────────────────────────

SL_POINTS     = 3     # per leg: exit if candle HIGH >= entry + SL_POINTS
TARGET_POINTS = 5     # combined: exit if (ce_entry-low) + (pe_entry-low) >= TARGET_POINTS
LOT_SIZE      = 75
COST_PERCENT  = 0.0025

CSV_COMBINED  = 'nifty_{date}.csv'
DB_FILE       = 'gdfl_data.duckdb'


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _leg_pnl(entry, exit_price):
    """Net PnL points for one short leg after transaction costs."""
    cost = (entry + exit_price) * COST_PERCENT
    return (entry - exit_price) - cost


def _atm(spot):
    return round(spot / 50) * 50


# ─────────────────────────────────────────────────────────────────────────────
# SUMMARY TABLE
# ─────────────────────────────────────────────────────────────────────────────

def print_trade_summary(completed_trades):
    """
    Print final summary table after all dates are processed.
    Two rows per trade (CE + PE) with sequential Index.
    Columns: Index, Leg, Strike, Entry Time, Exit Time,
             Entry ₹, Exit ₹, Leg PnL (₹), Trade PnL (₹)
    """
    if not completed_trades:
        print("\nNo completed trades to display.")
        return

    headers = [
        "#", "Leg", "Strike",
        "Entry Time", "Exit Time",
        "Entry ₹", "Exit ₹",
        "Leg PnL (₹)", "Trade PnL (₹)"
    ]

    rows = []
    for i, t in enumerate(completed_trades, start=1):
        # CE row — Trade PnL shown here
        rows.append([
            i, "CE", t['ce_strike'],
            t['ce_entry_time'], t['ce_exit_time'],
            f"{t['ce_entry']:.2f}", f"{t['ce_exit']:.2f}",
            f"{t['ce_pnl_val']:+.2f}",
            f"{t['trade_pnl_val']:+.2f}",
        ])
        # PE row — Trade PnL blank, # blank for clean look
        rows.append([
            "", "PE", t['pe_strike'],
            t['pe_entry_time'], t['pe_exit_time'],
            f"{t['pe_entry']:.2f}", f"{t['pe_exit']:.2f}",
            f"{t['pe_pnl_val']:+.2f}",
            "",
        ])

    print("\n" + "=" * 100)
    print("  BACKTEST TRADE SUMMARY  (completed trades only)")
    print("=" * 100)
    print(tabulate(rows, headers=headers, tablefmt="rounded_outline",
                   stralign="center", numalign="center"))

    # Aggregate stats
    total_trades = len(completed_trades)
    wins         = sum(1 for t in completed_trades if t['trade_pnl_val'] > 0)
    losses       = total_trades - wins
    total_pnl    = sum(t['trade_pnl_val'] for t in completed_trades)
    best         = max(completed_trades, key=lambda t: t['trade_pnl_val'])
    worst        = min(completed_trades, key=lambda t: t['trade_pnl_val'])

    print(f"\n  Total Trades  : {total_trades}")
    print(f"  Winners       : {wins}  ({wins / total_trades * 100:.1f}%)")
    print(f"  Losers        : {losses}  ({losses / total_trades * 100:.1f}%)")
    print(f"  Best Trade    : ₹{best['trade_pnl_val']:+.2f}  "
          f"(entered {best['ce_entry_time']})")
    print(f"  Worst Trade   : ₹{worst['trade_pnl_val']:+.2f}  "
          f"(entered {worst['ce_entry_time']})")
    print(f"  Total Net PnL : ₹{total_pnl:+,.2f}")
    print("=" * 100 + "\n")


# ─────────────────────────────────────────────────────────────────────────────
# DATA LOADING
# ─────────────────────────────────────────────────────────────────────────────

def _load_from_combined_csv(date_str, entry_sql_time):
    """
    Load nifty_YYYY-MM-DD.csv and split into spot_df and opts_df.
    Returns (spot_df, opts_df) or (None, None) if file not found.
    """
    csv_file = CSV_COMBINED.format(date=date_str)
    if not os.path.exists(csv_file):
        return None, None

    df = pd.read_csv(csv_file, parse_dates=['datetime'])
    df['datetime'] = pd.to_datetime(df['datetime'])

    required = {'datetime', 'date', 'spot_open', 'expiry_date',
                'strike_price', 'option_type', 'open', 'high', 'low', 'close'}
    missing = required - set(df.columns)
    if missing:
        print(f"  ❌ CSV missing columns: {missing}")
        return None, None

    entry_time_obj = datetime.strptime(entry_sql_time, '%H:%M:%S').time()
    df = df[
        (df['datetime'].dt.time >= entry_time_obj) &
        (df['datetime'].dt.time <= time(15, 15))
    ].sort_values('datetime').reset_index(drop=True)

    # Drop first and last minute — T10 collector always produces an incomplete
    # first candle (partial open minute) and the 15:15 candle is time-exit only.
    if not df.empty:
        all_minutes = sorted(df['datetime'].dt.strftime('%Y-%m-%d %H:%M').unique())
        if len(all_minutes) > 2:
            drop_minutes = {all_minutes[0], all_minutes[-1]}
            df = df[~df['datetime'].dt.strftime('%Y-%m-%d %H:%M').isin(drop_minutes)]
            df = df.reset_index(drop=True)

    if df.empty:
        return None, None

    # Spot: one row per minute
    spot_df = (df[['datetime', 'spot_open']]
               .drop_duplicates(subset='datetime')
               .rename(columns={'spot_open': 'open'})
               .reset_index(drop=True))

    # Options: all rows
    opts_df = df[['datetime', 'expiry_date', 'strike_price',
                  'option_type', 'open', 'high', 'low', 'close']].copy()

    return spot_df, opts_df


def _load_from_db(con, date_str, entry_sql_time):
    """
    Load from DuckDB (fallback). Returns (spot_df, opts_df) or (None, None).
    """
    expiry_res = con.execute(f"""
        SELECT MIN(expiry_date) FROM options_data
        WHERE date = '{date_str}' AND expiry_date >= '{date_str}'
    """).fetchone()
    if not expiry_res or not expiry_res[0]:
        return None, None
    expiry = expiry_res[0]

    spot_df = con.execute(f"""
        SELECT datetime, open FROM spot_data
        WHERE date = '{date_str}'
        AND cast(datetime as time) >= '{entry_sql_time}'
        AND cast(datetime as time) <= '15:15:00'
        ORDER BY datetime
    """).fetchdf()

    opts_df = con.execute(f"""
        SELECT datetime, strike_price, option_type, open, high, low, close
        FROM options_data
        WHERE date = '{date_str}' AND expiry_date = '{expiry}'
        AND cast(datetime as time) >= '{entry_sql_time}'
        AND cast(datetime as time) <= '15:15:00'
        ORDER BY datetime
    """).fetchdf()

    spot_df['datetime'] = pd.to_datetime(spot_df['datetime'])
    opts_df['datetime'] = pd.to_datetime(opts_df['datetime'])
    return spot_df, opts_df


def _get_csv_dates():
    files = glob.glob(CSV_COMBINED.replace('{date}', '*'))
    dates = []
    for f in sorted(files):
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


# ─────────────────────────────────────────────────────────────────────────────
# MAIN BACKTEST
# ─────────────────────────────────────────────────────────────────────────────

def run_backtest(entry_time_str='09:16', sl_points=SL_POINTS,
                 target_points=TARGET_POINTS, date_filter=None, quiet=False):
    """
    Strategy: Short ATM Straddle with SL, Target, and Re-entry.

    Entry   : OPEN of first candle at or after entry_time_str
    Target  : Checked on candle LOW first (priority)
              (ce_entry - low) + (pe_entry - low) >= target_points
              Both legs exit at LOW
    SL      : Checked on candle HIGH second
              Either HIGH >= leg_entry + sl_points → both exit at SL price
    Re-entry: Next candle open immediately after SL or Target
    Time    : 15:15 candle CLOSE, no re-entry

    Data    : nifty_YYYY-MM-DD.csv (combined) → DuckDB fallback
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

    # ── Build date → source list ──────────────────────────────────────────────
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

    # Open DB only if needed
    con = None
    if any(s == 'db' for _, s in date_sources):
        if os.path.exists(DB_FILE):
            con = duckdb.connect(DB_FILE, read_only=True)
        else:
            print(f"⚠️  DuckDB '{DB_FILE}' not found — skipping DB dates.")
            date_sources = [(d, s) for d, s in date_sources if s == 'csv']

    if not quiet:
        date_label = date_filter if date_filter else f"ALL ({len(date_sources)} days)"
        print(f"\nFound {len(date_sources)} trading day(s).")
        print(f"Parameters : Date   = {date_label}")
        print(f"           : Entry  = {entry_time_str}")
        print(f"           : SL     = {sl_points} pts/leg")
        print(f"           : Target = {target_points} pts combined")
        print(f"           : Lot    = {LOT_SIZE}\n")

    daily_results    = []
    completed_trades = []   # one dict per closed straddle
    trade_counter    = 0

    for date_str, source in date_sources:
        try:
            # ── Load data ─────────────────────────────────────────────────────
            if source == 'csv':
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

            # ── Build options lookup index ────────────────────────────────────
            opts_day['dt'] = pd.to_datetime(opts_day['datetime'])
            # Deduplicate: CSV may have duplicate rows from collector reconnects.
            # Keep last occurrence so index always returns a single row, not Series.
            opts_day = opts_day.drop_duplicates(
                subset=['dt', 'strike_price', 'option_type'], keep='last'
            )
            opts_idx = opts_day.set_index(['dt', 'strike_price', 'option_type'])

            # ── Determine strike mode ─────────────────────────────────────────
            # CSV from T10 has one fixed strike for the whole session.
            # DB data may have multiple strikes — use ATM calc per candle.
            unique_strikes = opts_day['strike_price'].unique()
            if source == 'csv' and len(unique_strikes) == 1:
                fixed_strike = float(unique_strikes[0])
                if not quiet:
                    print(f"  🔒 Fixed strike from CSV: {fixed_strike}")
            else:
                fixed_strike = None   # calculate ATM per candle (DB path)

            def get_candle(candle_dt, strike, opt_type):
                try:
                    return opts_idx.loc[(candle_dt, float(strike), opt_type)]
                except KeyError:
                    return None

            # ── Per-day state ─────────────────────────────────────────────────
            active     = None   # open straddle dict, None when flat
            day_trades = []

            for _, spot_row in spot_day.iterrows():
                candle_dt      = pd.to_datetime(spot_row['datetime'])
                spot_open      = float(spot_row['open'])
                candle_time    = candle_dt.time()
                is_exit_candle = (candle_time == time(15, 15))

                # ── Open trade (initial or re-entry) ──────────────────────────
                if active is None:
                    # Fixed strike from CSV, or ATM calc for DB
                    strike = fixed_strike if fixed_strike is not None else _atm(spot_open)
                    ce_c   = get_candle(candle_dt, strike, 'CALL')
                    pe_c   = get_candle(candle_dt, strike, 'PUT')
                    if ce_c is None or pe_c is None:
                        if not quiet:
                            print(f"  ⚠️  No candle for strike {strike} at "
                                  f"{candle_dt.strftime('%H:%M')} — skipping")
                        continue

                    trade_counter += 1
                    active = {
                        'trade_num'     : trade_counter,
                        'ce_strike'     : strike,
                        'pe_strike'     : strike,
                        'ce_entry'      : float(ce_c['open']),
                        'pe_entry'      : float(pe_c['open']),
                        'ce_entry_time' : candle_dt.strftime('%H:%M'),
                        'pe_entry_time' : candle_dt.strftime('%H:%M'),
                    }
                    active['ce_sl'] = active['ce_entry'] + sl_points
                    active['pe_sl'] = active['pe_entry'] + sl_points

                    if not quiet:
                        print(f"  ➤ TRADE #{trade_counter} OPEN  "
                              f"[{date_str} {candle_dt.strftime('%H:%M')}] "
                              f"CE {strike} @ {active['ce_entry']:.2f}  "
                              f"PE {strike} @ {active['pe_entry']:.2f}")

                # ── Fetch current candles for active trade ────────────────────
                ce_c = get_candle(candle_dt, active['ce_strike'], 'CALL')
                pe_c = get_candle(candle_dt, active['pe_strike'], 'PUT')
                if ce_c is None or pe_c is None:
                    continue

                ce_high  = float(ce_c['high'])
                ce_low   = float(ce_c['low'])
                ce_close = float(ce_c['close'])
                pe_high  = float(pe_c['high'])
                pe_low   = float(pe_c['low'])
                pe_close = float(pe_c['close'])

                # ── Per-minute display ────────────────────────────────────────
                if not quiet:
                    ce_pts         = _leg_pnl(active['ce_entry'], ce_close)
                    pe_pts         = _leg_pnl(active['pe_entry'], pe_close)
                    trade_live_val = (ce_pts + pe_pts) * LOT_SIZE
                    closed_pnl     = sum(t['trade_pnl_val'] for t in day_trades)
                    print(
                        f"  [{date_str} {candle_dt.strftime('%H:%M')}]  "
                        f"Spot: {spot_open:.0f} | "
                        f"Trade#{active['trade_num']}  "
                        f"CE({active['ce_strike']}): {ce_close:.2f} | "
                        f"PE({active['pe_strike']}): {pe_close:.2f} | "
                        f"Trade PnL: ₹{trade_live_val:+.2f} | "
                        f"Day PnL: ₹{closed_pnl + trade_live_val:+.2f}"
                    )

                exit_reason = None
                ce_exit_px  = None
                pe_exit_px  = None
                do_reenter  = False

                if is_exit_candle:
                    # Time exit — both legs at candle close
                    exit_reason = 'Time'
                    ce_exit_px  = ce_close
                    pe_exit_px  = pe_close

                else:
                    # ── Target first (candle LOW) ─────────────────────────────
                    target_pts = (
                        (active['ce_entry'] - ce_low) +
                        (active['pe_entry'] - pe_low)
                    )
                    if target_pts >= target_points:
                        exit_reason = 'Target'
                        ce_exit_px  = ce_low
                        pe_exit_px  = pe_low
                        do_reenter  = True

                    else:
                        # ── SL second (candle HIGH) ───────────────────────────
                        if (ce_high >= active['ce_sl'] or
                                pe_high >= active['pe_sl']):
                            exit_reason = 'SL'
                            ce_exit_px  = active['ce_sl']
                            pe_exit_px  = active['pe_sl']
                            do_reenter  = True

                # ── Close trade ───────────────────────────────────────────────
                if exit_reason:
                    ce_pnl_pts = _leg_pnl(active['ce_entry'], ce_exit_px)
                    pe_pnl_pts = _leg_pnl(active['pe_entry'], pe_exit_px)
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
                    active = None

                    if not quiet:
                        icon = "🎯" if exit_reason == 'Target' else (
                               "🔴" if exit_reason == 'SL' else "🏁")
                        print(f"  {icon} TRADE #{record['trade_num']} CLOSED | "
                              f"{exit_reason} | "
                              f"CE exit {ce_exit_px:.2f}  PE exit {pe_exit_px:.2f} | "
                              f"Trade PnL: ₹{trade_val:+.2f}")

                    if not do_reenter:
                        break   # time exit — done for this day
                    # SL/Target: active=None, loop continues to next candle

            # ── Daily PnL ─────────────────────────────────────────────────────
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

    # ── Equity curve & reporting ──────────────────────────────────────────────
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

            # Monthly PnL table (only for multi-day runs)
            if len(res_df) > 1:
                res_df['date'] = pd.to_datetime(res_df['date'])
                monthly = (res_df.set_index('date')
                                 .resample('ME')['pnl_value']
                                 .sum()
                                 .reset_index())
                monthly['Year']  = monthly['date'].dt.year
                monthly['Month'] = monthly['date'].dt.strftime('%b')
                pivot = monthly.pivot(index='Year', columns='Month',
                                      values='pnl_value')
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
                            if isinstance(v, (int, float)) else v
                        )
                pivot_pct['MDD'] = pivot_pct['MDD'].apply(
                    lambda v: f"{v:.2f} ({(v/base_capital)*100:.2f}%)"
                    if isinstance(v, (int, float)) else v
                )
                pivot_pct.to_csv('monthly_pnl_matrix.csv')
                print("  Monthly matrix → monthly_pnl_matrix.csv")

                # Equity & drawdown chart
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
                plt.plot(res_df['date'], res_df['drawdown'],
                         color='red', alpha=0.6)
                plt.title('Drawdown Curve')
                plt.ylabel('Drawdown (₹)'); plt.xlabel('Date')
                plt.grid(True); plt.legend()
                plt.tight_layout()
                plt.savefig('backtest_performance.png')
                print("  Chart → backtest_performance.png")

            # ── Final trade summary table (printed last) ──────────────────────
            print_trade_summary(completed_trades)

    else:
        if not quiet:
            print("No trades generated.")

    return res_df, completed_trades


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Short Straddle Backtest (SL + Target + Re-entry)',
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        '--time', type=str, default='09:16',
        help='Entry time HH:MM (default 09:16)\n'
             'Also acts as session start — candles before this are ignored\n'
             'Example: --time 14:55'
    )
    parser.add_argument(
        '--sl', type=int, default=SL_POINTS,
        help=f'SL points per leg (default {SL_POINTS})'
    )
    parser.add_argument(
        '--target', type=int, default=TARGET_POINTS,
        help=f'Target combined pts (default {TARGET_POINTS})'
    )
    parser.add_argument(
        '--date', type=str, default=None,
        help='Filter to single date YYYY-MM-DD (default: all dates)\n'
             'Combined CSV checked first, DuckDB used as fallback\n'
             'Example: --date 2026-02-25'
    )
    args = parser.parse_args()
    run_backtest(
        entry_time_str=args.time,
        sl_points=args.sl,
        target_points=args.target,
        date_filter=args.date,
    )