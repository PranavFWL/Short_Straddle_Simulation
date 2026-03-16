"""
fetch_options_ohlc.py
---------------------
Fetches current day's 1-minute OHLC data for a given NIFTY options strike
(both CE and PE) using the Upstox API.

Usage:
    python fetch_options_ohlc.py --strike 23400

Output:
    CSV file named  API_csv_YYYY-MM-DD.csv  in the current directory.

Requirements:
    pip install requests pandas
"""

import argparse
import json
import os
import sys
from datetime import datetime, date

import pandas as pd
import requests

# ──────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────
TOKEN_FILE = "upstox_token.txt"
INSTRUMENTS_URL = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"
NIFTY_INDEX_KEY = "NSE_INDEX|Nifty 50"
BASE_URL = "https://api.upstox.com/v2"


# ──────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────

def load_token() -> str:
    """Read Bearer token from upstox_token.txt."""
    if not os.path.exists(TOKEN_FILE):
        sys.exit(f"[ERROR] Token file '{TOKEN_FILE}' not found in current directory.")
    with open(TOKEN_FILE, "r") as f:
        token = f.read().strip()
    if not token:
        sys.exit(f"[ERROR] Token file '{TOKEN_FILE}' is empty.")
    return token


def get_headers(token: str) -> dict:
    return {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {token}",
    }


def fetch_nifty_ltp(token: str) -> float:
    """Fetch current LTP of NIFTY 50 index."""
    url = f"{BASE_URL}/market-quote/ltp"
    params = {"instrument_key": NIFTY_INDEX_KEY}
    resp = requests.get(url, headers=get_headers(token), params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    # Response key is like "NSE_INDEX:Nifty 50"
    for key, val in data["data"].items():
        if "Nifty 50" in key or "NIFTY" in key.upper():
            return float(val["last_price"])
    sys.exit("[ERROR] Could not parse NIFTY LTP from response.")


def fetch_option_contracts(token: str) -> list:
    """
    Fetch all available option expiry dates for NIFTY,
    then return full option chain data for the nearest expiry.
    """
    # Step 1: get list of expiries
    url_expiry = f"{BASE_URL}/option/contract"
    params = {"instrument_key": NIFTY_INDEX_KEY}
    resp = requests.get(url_expiry, headers=get_headers(token), params=params, timeout=10)
    resp.raise_for_status()
    expiries_raw = resp.json().get("data", [])

    if not expiries_raw:
        sys.exit("[ERROR] No expiry dates returned from Upstox option contract API.")

    # expiries_raw is a list of contract objects, each with an "expiry" field like "2026-03-20"
    today = date.today()
    expiry_dates = set()
    for contract in expiries_raw:
        expiry_str = contract.get("expiry") if isinstance(contract, dict) else contract
        if expiry_str:
            expiry_dates.add(expiry_str)

    future_expiries = sorted(
        [e for e in expiry_dates if date.fromisoformat(e) >= today]
    )
    if not future_expiries:
        sys.exit("[ERROR] No upcoming expiries found for NIFTY.")

    nearest_expiry = future_expiries[0]
    print(f"[INFO] Nearest expiry selected: {nearest_expiry}")
    return nearest_expiry


def fetch_option_chain(token: str, expiry: str) -> list:
    """Fetch full option chain for NIFTY for given expiry."""
    url = f"{BASE_URL}/option/chain"
    params = {"instrument_key": NIFTY_INDEX_KEY, "expiry_date": expiry}
    resp = requests.get(url, headers=get_headers(token), params=params, timeout=10)
    resp.raise_for_status()
    return resp.json().get("data", [])


def find_strike_keys(chain: list, strike: float) -> dict:
    """
    Given option chain data, find instrument_key for CE and PE
    of the requested strike price.
    Returns {"CE": "NSE_FO|XXXXX", "PE": "NSE_FO|YYYYY", "expiry": "YYYY-MM-DD"}
    """
    result = {}
    for item in chain:
        if float(item.get("strike_price", -1)) == strike:
            result["expiry"] = item.get("expiry")
            call = item.get("call_options", {})
            put = item.get("put_options", {})
            if call:
                result["CE"] = call.get("instrument_key")
            if put:
                result["PE"] = put.get("instrument_key")
            break

    if not result.get("CE") or not result.get("PE"):
        sys.exit(
            f"[ERROR] Strike {strike} not found in the option chain for this expiry. "
            "Please verify the strike price is valid for today's expiry."
        )
    return result


def fetch_intraday_candles(token: str, instrument_key: str) -> list:
    """
    Fetch 1-minute intraday OHLC candles for current day.
    Returns list of candles: [timestamp, open, high, low, close, volume, oi]
    """
    encoded_key = requests.utils.quote(instrument_key, safe="")
    url = f"{BASE_URL}/historical-candle/intraday/{encoded_key}/1minute"
    resp = requests.get(url, headers=get_headers(token), timeout=15)
    resp.raise_for_status()
    candles = resp.json().get("data", {}).get("candles", [])
    return candles


def candles_to_df(candles: list, strike: float, option_type: str) -> pd.DataFrame:
    """
    Convert raw candle list to a clean DataFrame matching the required output format.
    Candle format from API: [timestamp, open, high, low, close, volume, oi]
    """
    rows = []
    for candle in candles:
        ts_str = candle[0]                         # e.g. "2026-03-16T09:15:00+05:30"
        dt = datetime.fromisoformat(ts_str)
        dt_naive = dt.replace(tzinfo=None)         # drop tz for clean display
        rows.append({
            "datetime":     dt_naive.strftime("%Y-%m-%d %H:%M:%S"),
            "strike_price": strike,
            "option_type":  "CALL" if option_type == "CE" else "PUT",
            "open":         candle[1],
            "high":         candle[2],
            "low":          candle[3],
            "close":        candle[4],
        })

    df = pd.DataFrame(rows, columns=[
        "datetime", "strike_price", "option_type", "open", "high", "low", "close"
    ])
    # Sort chronologically (API returns latest-first)
    df = df.sort_values("datetime").reset_index(drop=True)
    return df


# ──────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Fetch current day 1-min OHLC for a NIFTY options strike (CE + PE)"
    )
    parser.add_argument(
        "--strike", type=float, required=True,
        help="Strike price to fetch (e.g. 23400)"
    )
    args = parser.parse_args()
    strike = args.strike

    print(f"[INFO] Fetching 1-min OHLC data for NIFTY {int(strike)} CE & PE ...")

    # 1. Load auth token
    token = load_token()
    print("[INFO] Token loaded.")

    # 2. Get nearest expiry
    nearest_expiry = fetch_option_contracts(token)

    # 4. Fetch full option chain for that expiry
    print(f"[INFO] Fetching option chain for expiry {nearest_expiry} ...")
    chain = fetch_option_chain(token, nearest_expiry)
    if not chain:
        sys.exit("[ERROR] Empty option chain returned.")

    # 5. Find CE and PE instrument keys for the requested strike
    keys = find_strike_keys(chain, strike)
    print(f"[INFO] CE instrument key : {keys['CE']}")
    print(f"[INFO] PE instrument key : {keys['PE']}")
    expiry_date = keys["expiry"]  # may differ from nearest_expiry string

    # 6. Fetch intraday 1-min candles for CE and PE
    print("[INFO] Fetching CE candles ...")
    ce_candles = fetch_intraday_candles(token, keys["CE"])
    print(f"[INFO] CE candles received: {len(ce_candles)}")

    print("[INFO] Fetching PE candles ...")
    pe_candles = fetch_intraday_candles(token, keys["PE"])
    print(f"[INFO] PE candles received: {len(pe_candles)}")

    if not ce_candles and not pe_candles:
        sys.exit("[ERROR] No candle data returned for either CE or PE. "
                 "Make sure you are running this after market hours or during market hours.")

    # 7. Build DataFrames
    df_ce = candles_to_df(ce_candles, strike, "CE")
    df_pe = candles_to_df(pe_candles, strike, "PE")

    # 8. Merge: for each timestamp, CE and PE rows sit together (sorted by datetime then option_type)
    df_all = pd.concat([df_ce, df_pe], ignore_index=True)
    df_all = df_all.sort_values(["datetime", "option_type"],
                                ascending=[True, False])   # PUT before CALL matches sample
    df_all = df_all.reset_index(drop=True)

    # 9. Save to CSV
    today_str = date.today().strftime("%Y-%m-%d")
    output_file = f"API_csv_{today_str}.csv"
    df_all.to_csv(output_file, index=False)

    print(f"\n[SUCCESS] Data saved to: {output_file}")
    print(f"[INFO] Total rows: {len(df_all)}  "
          f"(CE: {len(df_ce)}, PE: {len(df_pe)})")
    print("\nSample output (first 4 rows):")
    print(df_all.head(4).to_string(index=False))


if __name__ == "__main__":
    main()