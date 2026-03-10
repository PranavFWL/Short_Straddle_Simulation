import duckdb
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import time, datetime
from tabulate import tabulate
import argparse
import os
import glob


SL_POINTS     = 10
TARGET_POINTS = 10
LOT_SIZE      = 65
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
        rows.append([
            i, "CE", t['ce_strike'],
            t['ce_entry_time'], t['ce_exit_time'],
            f"{t['ce_entry']:.2f}", f"{t['ce_exit']:.2f}",
            f"{t['ce_pnl_val']:+.2f}",
            f"{t['trade_pnl_val']:+.2f}",
        ])
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

    if not df.empty:
        all_minutes = sorted(df['datetime'].dt.strftime('%Y-%m-%d %H:%M').unique())
        if len(all_minutes) > 2:
            drop_minutes = {all_minutes[0], all_minutes[-1]}
            df = df[~df['datetime'].dt.strftime('%Y-%m-%d %H:%M').isin(drop_minutes)]
            df = df.reset_index(drop=True)

    if df.empty:
        return None, None

    spot_df = (df[['datetime', 'spot_open']]
               .drop_duplicates(subset='datetime')
               .rename(columns={'spot_open': 'open'})
               .reset_index(drop=True))

    opts_df = df[['datetime', 'expiry_date', 'strike_price',
                  'option_type', 'open', 'high', 'low', 'close']].copy()

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
#
# KEY DESIGN DECISIONS (aligned with live_strategy.py):
#
# 1. FIXED STRIKE per day:
#    CSV path  — uses the single strike present in the CSV (from T10 collector)
#    DB path   — locks to ATM of the FIRST candle of the day, never recalculates
#    Both match live which locks ATM once at session start.
#
# 2. EXIT PRICES (no slippage):
#    Target → ce_low, pe_low  (exact level crossed, same as live)
#    SL     → ce_sl, pe_sl    (exact level, same as live)
#    Time   → candle close    (live PnL, same as live)
#
# 3. CONFLICT RESOLUTION:
#    Open-proximity heuristic — identical to live_strategy.py
#
# 4. RE-ENTRY:
#    Immediate next candle after SL/Target exit.
#    Since candles are 1-minute, this equals "next minute boundary" —
#    same as live which gates re-entry to (exit_candle_minute + 1 min).
# ─────────────────────────────────────────────────────────────────────────────

def run_backtest(entry_time_str='09:16', sl_points=SL_POINTS,
                 target_points=TARGET_POINTS, date_filter=None, quiet=False):

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

    # ── Build date → source list ───────────────────────────────────────────────
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
        date_label = date_filter if date_filter else f"ALL ({len(date_sources)} days)"
        print(f"\nFound {len(date_sources)} trading day(s).")
        print(f"Parameters : Date   = {date_label}")
        print(f"           : Entry  = {entry_time_str}")
        print(f"           : SL     = {sl_points} pts/leg")
        print(f"           : Target = {target_points} pts combined")
        print(f"           : Lot    = {LOT_SIZE}\n")

    daily_results    = []
    completed_trades = []
    trade_counter    = 0

    for date_str, source in date_sources:
        try:
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

            opts_day['dt'] = pd.to_datetime(opts_day['datetime'])
            opts_day = opts_day.drop_duplicates(
                subset=['dt', 'strike_price', 'option_type'], keep='last'
            )
            opts_idx = opts_day.set_index(['dt', 'strike_price', 'option_type'])

            # ── Strike mode ────────────────────────────────────────────────────
            # CSV: one fixed strike for whole day (from T10 collector)
            # DB : lock to ATM of first candle — never recalculate mid-session.
            #      This matches live which locks ATM once at session start.
            unique_strikes = opts_day['strike_price'].unique()
            if source == 'csv' and len(unique_strikes) == 1:
                fixed_strike = float(unique_strikes[0])
                if not quiet:
                    print(f"  🔒 Fixed strike from CSV: {fixed_strike}")
            else:
                # DB path: will be set from the first candle's spot open
                fixed_strike     = None
                day_first_strike = None  # locked once, never changed

            def get_candle(candle_dt, strike, opt_type):
                try:
                    return opts_idx.loc[(candle_dt, float(strike), opt_type)]
                except KeyError:
                    return None

            # ── Per-day state ──────────────────────────────────────────────────
            active     = None
            day_trades = []

            for _, spot_row in spot_day.iterrows():
                candle_dt      = pd.to_datetime(spot_row['datetime'])
                spot_open      = float(spot_row['open'])
                candle_time    = candle_dt.time()
                is_exit_candle = (candle_time == time(15, 15))

                # ── Determine strike to use ────────────────────────────────────
                # CSV: always fixed_strike
                # DB : lock on first candle (matches live locking ATM at open)
                if source == 'csv':
                    strike_to_use = fixed_strike
                else:
                    if day_first_strike is None:
                        # Lock strike from first candle's spot — never changes
                        day_first_strike = _atm(spot_open)
                        if not quiet:
                            print(f"  🔒 Fixed strike from DB (first candle): "
                                  f"{day_first_strike}")
                    strike_to_use = day_first_strike

                # ── Open trade (initial or re-entry) ──────────────────────────
                if active is None:
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
                    active['ce_sl'] = active['ce_entry'] + sl_points
                    active['pe_sl'] = active['pe_entry'] + sl_points

                    if not quiet:
                        print(f"  ➤ TRADE #{trade_counter} OPEN  "
                              f"[{date_str} {candle_dt.strftime('%H:%M')}] "
                              f"CE {strike_to_use} @ {active['ce_entry']:.2f}  "
                              f"PE {strike_to_use} @ {active['pe_entry']:.2f}")

                    # Skip SL/Target check on entry candle
                    # (same behaviour as live: entry candle open → don't check
                    #  SL/Target until the NEXT candle completes)
                    continue

                # ── Fetch current candles for active trade ─────────────────────
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

                # ── Per-minute display ─────────────────────────────────────────
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
                    exit_reason = 'Time'
                    ce_exit_px  = ce_close
                    pe_exit_px  = pe_close

                else:
                    # ── Target and SL on completed candle ──────────────────────
                    target_pts = (
                        (active['ce_entry'] - ce_low) +
                        (active['pe_entry'] - pe_low)
                    )
                    target_hit = target_pts >= target_points
                    sl_hit     = (ce_high >= active['ce_sl'] or
                                  pe_high >= active['pe_sl'])

                    if target_hit and sl_hit:
                        # ── Conflict: open-proximity heuristic ─────────────────
                        sl_dists = []
                        if ce_high >= active['ce_sl']:
                            sl_dists.append(active['ce_sl'] - ce_open_c)
                        if pe_high >= active['pe_sl']:
                            sl_dists.append(active['pe_sl'] - pe_open_c)
                        dist_to_sl = min(sl_dists)

                        gain_at_open   = ((active['ce_entry'] - ce_open_c) +
                                          (active['pe_entry'] - pe_open_c))
                        dist_to_target = max(0.0, target_points - gain_at_open)

                        if dist_to_sl <= dist_to_target:
                            exit_reason = 'SL'
                            ce_exit_px  = active['ce_sl']
                            pe_exit_px  = active['pe_sl']
                        else:
                            exit_reason = 'Target'
                            ce_exit_px  = ce_low
                            pe_exit_px  = pe_low

                        if not quiet:
                            print(f"  ⚡ Conflict at {candle_dt.strftime('%H:%M')} — "
                                  f"dist_to_sl={dist_to_sl:.2f}  "
                                  f"dist_to_target={dist_to_target:.2f}  "
                                  f"→ {exit_reason} wins")
                        do_reenter = True

                    elif target_hit:
                        exit_reason = 'Target'
                        ce_exit_px  = ce_low
                        pe_exit_px  = pe_low
                        do_reenter  = True

                    elif sl_hit:
                        exit_reason = 'SL'
                        ce_exit_px  = active['ce_sl']
                        pe_exit_px  = active['pe_sl']
                        do_reenter  = True

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
                    # SL/Target: active=None, loop continues to NEXT candle (re-entry)

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
    parser.add_argument('--time', type=str, default='09:16',
                        help='Entry time HH:MM (default 09:16)')
    parser.add_argument('--sl', type=int, default=SL_POINTS,
                        help=f'SL points per leg (default {SL_POINTS})')
    parser.add_argument('--target', type=int, default=TARGET_POINTS,
                        help=f'Target combined pts (default {TARGET_POINTS})')
    parser.add_argument('--date', type=str, default=None,
                        help='Filter to single date YYYY-MM-DD (default: all dates)')
    args = parser.parse_args()
    run_backtest(
        entry_time_str=args.time,
        sl_points=args.sl,
        target_points=args.target,
        date_filter=args.date,
    )