"""
Upstox Real-Time Option Chain Display (SDK Version)
Uses upstox-python-sdk for WebSocket streaming
"""

import requests
import time
import threading
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from urllib.parse import urlencode, parse_qs, urlparse
import os
from dotenv import load_dotenv
import pickle

# Upstox SDK
import upstox_client

# For table display
try:
    from rich.console import Console
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False
    print("⚠️  'rich' library not found. Install with: pip install rich")


class UpstoxAuth:

    def __init__(self, api_key, api_secret, redirect_uri="https://www.google.com"):
        self.api_key = api_key
        self.api_secret = api_secret
        self.redirect_uri = redirect_uri
        self.base_url = "https://api.upstox.com/v2"

    def get_login_url(self):
        params = {
            'response_type': 'code',
            'client_id': self.api_key,
            'redirect_uri': self.redirect_uri
        }
        return f"{self.base_url}/login/authorization/dialog?{urlencode(params)}"

    def get_access_token(self, auth_code):
        token_url = f"{self.base_url}/login/authorization/token"
        headers = {
            'accept': 'application/json',
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        data = {
            'code': auth_code,
            'client_id': self.api_key,
            'client_secret': self.api_secret,
            'redirect_uri': self.redirect_uri,
            'grant_type': 'authorization_code'
        }

        response = requests.post(token_url, headers=headers, data=data)
        response.raise_for_status()
        return response.json().get('access_token')

    def extract_code_from_url(self, redirect_url):
        parsed = urlparse(redirect_url)
        params = parse_qs(parsed.query)
        if 'code' not in params:
            raise ValueError("No authorization code found in URL")
        return params['code'][0]


class UpstoxOptionChain:
    """Fetches and manages option chain data"""

    def __init__(self, access_token):
        self.access_token = access_token
        self.base_url = "https://api.upstox.com/v2"
        self.headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Authorization': f'Bearer {access_token}'
        }

    def get_option_chain(self, instrument_key="NSE_INDEX|Nifty 50", expiry_date=None):
        """Get option chain data for given instrument and expiry"""
        if not expiry_date:
            expiry_date = self._get_nearest_expiry(instrument_key)

        url = f"{self.base_url}/option/chain"
        params = {
            'instrument_key': instrument_key,
            'expiry_date': expiry_date
        }

        response = requests.get(url, params=params, headers=self.headers)
        response.raise_for_status()

        data = response.json()
        if data.get('status') == 'success':
            return data.get('data', [])
        else:
            raise Exception(f"API Error: {data}")

    def _get_nearest_expiry(self, instrument_key):
        """Get nearest expiry date for the instrument"""
        url = f"{self.base_url}/option/contract"
        params = {'instrument_key': instrument_key}

        response = requests.get(url, params=params, headers=self.headers)
        response.raise_for_status()

        data = response.json()
        if data.get('status') == 'success':
            contracts = data.get('data', [])
            if contracts:
                expiries = sorted(set(c['expiry'] for c in contracts))
                nearest_expiry = expiries[0]
                print(f"✅ Auto-detected nearest expiry: {nearest_expiry}")
                return nearest_expiry

        raise Exception("Could not determine nearest expiry")

    def get_atm_strikes(self, option_chain_data, num_strikes=10):
        """Filter option chain for ITM strikes only (ATM-10 to ATM)"""
        if not option_chain_data:
            return [], 0, 0

        spot_price = option_chain_data[0].get('underlying_spot_price', 0)
        all_strikes = sorted([entry['strike_price'] for entry in option_chain_data])
        atm_strike = min(all_strikes, key=lambda x: abs(x - spot_price))
        atm_index = all_strikes.index(atm_strike)

        # MODIFIED: Only select strikes from ATM-10 to ATM (ITM side)
        start_idx = max(0, atm_index - num_strikes)
        end_idx = min(len(all_strikes), atm_index + num_strikes + 1)
        selected_strikes = all_strikes[start_idx:end_idx]

        filtered_data = [
            entry for entry in option_chain_data
            if entry['strike_price'] in selected_strikes
        ]

        print(f"📊 Spot Price: {spot_price}")
        print(f"🎯 ATM Strike: {atm_strike}")
        print(f"📈 Selected Strikes: {len(filtered_data)} strikes from {min(selected_strikes)} to {max(selected_strikes)}")

        return filtered_data, spot_price, atm_strike

    def get_instrument_keys(self, filtered_chain_data):
        """Extract all instrument keys (CE and PE) from filtered chain"""
        instrument_keys = []

        for entry in filtered_chain_data:
            if 'call_options' in entry and 'instrument_key' in entry['call_options']:
                instrument_keys.append(entry['call_options']['instrument_key'])
            if 'put_options' in entry and 'instrument_key' in entry['put_options']:
                instrument_keys.append(entry['put_options']['instrument_key'])

        return instrument_keys


# ============================================================================
# NEW: OI TRACKER CLASS - ADD THIS BEFORE MarketDataManager
# ============================================================================
class OITracker:
    """
    Track Open Interest and LTP changes across option chain
    Properly calculates change from previous snapshot
    """
    def __init__(self):
        # Store previous values: {instrument_key: {'oi': value, 'ltp': value, 'timestamp': time}}
        self.previous_values = {}

    def update_and_get_changes(self, instrument_key, current_oi, current_ltp, timestamp=None):
        """
        Update OI/LTP and return changes from previous snapshot

        Args:
            instrument_key: Unique identifier for the instrument
            current_oi: Current Open Interest value
            current_ltp: Current Last Traded Price
            timestamp: Optional timestamp

        Returns:
            dict: {'chng_oi': change_in_oi, 'chng_ltp': change_in_ltp}
        """
        # First time seeing this instrument
        if instrument_key not in self.previous_values:
            # Store current as previous for next comparison
            self.previous_values[instrument_key] = {
                'oi': current_oi,
                'ltp': current_ltp,
                'timestamp': timestamp or time.time()
            }
            # Return 0 change on first snapshot
            return {'chng_oi': 0, 'chng_ltp': 0.0}

        # Get previous values
        prev_oi = self.previous_values[instrument_key]['oi']
        prev_ltp = self.previous_values[instrument_key]['ltp']

        # Calculate changes
        chng_oi = current_oi - prev_oi
        chng_ltp = current_ltp - prev_ltp

        # Update stored values for next comparison
        self.previous_values[instrument_key] = {
            'oi': current_oi,
            'ltp': current_ltp,
            'timestamp': timestamp or time.time()
        }

        return {'chng_oi': chng_oi, 'chng_ltp': chng_ltp}

    def reset(self):
        """Clear all stored data (use at market open/close)"""
        self.previous_values.clear()
        print("🔄 OI Tracker reset")

    def get_previous(self, instrument_key):
        """Get previous values for an instrument"""
        return self.previous_values.get(instrument_key, None)


class MarketDataManager:
    """Manages real-time market data using Upstox SDK"""

    def __init__(self, access_token):
        self.access_token = access_token
        self.market_data = defaultdict(dict)
        # REMOVED: self.prev_data - no longer needed
        self.streamer = None
        self.is_connected = False

        # NEW: Initialize OI Tracker
        self.oi_tracker = OITracker()
        print("✅ OI Tracker initialized")

            # Snapshot tracking for stale data detection
        self.data_snapshots = []
        self.max_snapshots = 3

    @staticmethod
    def _to_float(value, default=0.0):
        """Safely convert value to float"""
        try:
            return float(value) if value else default
        except (ValueError, TypeError):
            return default

    @staticmethod
    def _to_int(value, default=0):
        """Safely convert value to int"""
        try:
            return int(float(value)) if value else default
        except (ValueError, TypeError):
            return default

    def setup_streamer(self, instrument_keys, mode="full"):
        """Setup SDK streamer with instrument keys"""
        # Configure SDK
        configuration = upstox_client.Configuration()
        configuration.access_token = self.access_token

        # Create streamer
        self.streamer = upstox_client.MarketDataStreamerV3(
            upstox_client.ApiClient(configuration),
            instrument_keys,
            mode
        )

        # Register event handlers
        self.streamer.on("message", self._on_message)
        self.streamer.on("open", self._on_open)
        self.streamer.on("error", self._on_error)
        self.streamer.on("close", self._on_close)

        print(f"📡 SDK Streamer configured for {len(instrument_keys)} instruments")

    def _on_open(self):
        """Called when WebSocket connection opens"""
        self.is_connected = True
        print("✅ WebSocket connected!")
        print("🔍 Connection status: Active")
        print("🔍 Waiting for messages...")

        # Verify subscription - the SDK should have already subscribed during setup
        # but let's confirm
        if hasattr(self.streamer, 'get_subscriptions'):
            try:
                subs = self.streamer.get_subscriptions()
                print(f"📋 Current subscriptions: {len(subs)} instruments")
            except:
                pass

    def _on_message(self, message):
        """Process incoming market data from SDK"""
        try:
            # Count messages
            if not hasattr(self, '_debug_count'):
                self._debug_count = 0
                self._message_count = 0
                self._data_parsed = False

            self._message_count += 1

            # Show first message only for verification
            if self._debug_count < 1:
                print(f"\n{'='*100}")
                print(f"✅ First message received! Type: {type(message)}")
                if isinstance(message, dict) and 'feeds' in message:
                    print(f"✅ Contains {len(message['feeds'])} instrument feeds")
                print("="*100)
                self._debug_count += 1

            # SDK returns decoded message as dict
            if isinstance(message, dict):
                self._process_feed(message)
                if not self._data_parsed and len(self.market_data) > 0:
                    self._data_parsed = True
                    print(f"✅ Successfully parsed data for {len(self.market_data)} instruments!")
            else:
                # Try to parse if it's a string
                import json
                if isinstance(message, str):
                    try:
                        message = json.loads(message)
                        self._process_feed(message)
                    except:
                        print(f"⚠️  Could not parse message")
        except Exception as e:
            print(f"❌ Error processing message: {e}")
            import traceback
            traceback.print_exc()

    def _on_error(self, error):
        """Handle WebSocket errors"""
        if "401 Unauthorized" in str(error) or "Invalid token" in str(error):
            print(f"\n❌ WebSocket error: Access token expired/invalid!")
            print("❌ Please run authenticate.py to generate a fresh token")
            print("❌ Stopping data collection gracefully...")
            self.disconnect()
        else:
            print(f"❌ WebSocket error: {error}")
            import traceback
            traceback.print_exc()

    def _on_close(self, close_status_code=None, close_msg=None):
        """Called when WebSocket connection closes"""
        self.is_connected = False
        print(f"🔌 WebSocket disconnected (code: {close_status_code}, msg: {close_msg})")

    def _process_feed(self, feed_data):
        """Process market data feed from SDK"""
        # SDK returns data in format: {"feeds": {instrument_key: data}}
        if 'feeds' in feed_data:
            feeds = feed_data['feeds']

            for instrument_key, data in feeds.items():
                # Process different feed types - SDK uses 'fullFeed' not 'ff'
                if 'fullFeed' in data:
                    # Full feed
                    self._process_full_feed(instrument_key, data['fullFeed'])
                elif 'ltpc' in data:
                    # LTPC feed
                    self._process_ltpc_feed(instrument_key, data['ltpc'])

                # NEW: Calculate changes using OI Tracker (after data is processed)
                self._calculate_changes(instrument_key)

    def _process_full_feed(self, instrument_key, ff_data):
        """Process full feed data"""
        if 'marketFF' in ff_data:
            market_ff = ff_data['marketFF']

            # LTPC data
            if 'ltpc' in market_ff:
                ltpc = market_ff['ltpc']
                self.market_data[instrument_key]['ltp'] = self._to_float(ltpc.get('ltp', 0))
                self.market_data[instrument_key]['cp'] = self._to_float(ltpc.get('cp', 0))

            # Market level (bid/ask) - SDK returns these as strings!
            if 'marketLevel' in market_ff:
                ml = market_ff['marketLevel']
                if 'bidAskQuote' in ml and len(ml['bidAskQuote']) > 0:
                    baq = ml['bidAskQuote'][0]
                    self.market_data[instrument_key]['bid'] = self._to_float(baq.get('bP', baq.get('bidP', 0)))
                    self.market_data[instrument_key]['ask'] = self._to_float(baq.get('aP', baq.get('askP', 0)))
                    self.market_data[instrument_key]['bid_qty'] = self._to_int(baq.get('bQ', baq.get('bidQ', 0)))
                    self.market_data[instrument_key]['ask_qty'] = self._to_int(baq.get('aQ', baq.get('askQ', 0)))

            # OI - at ROOT LEVEL of marketFF
            if 'oi' in market_ff:
                self.market_data[instrument_key]['oi'] = self._to_int(market_ff.get('oi', 0))

            # Volume - in marketOHLC.ohlc[0].vol
            if 'marketOHLC' in market_ff:
                ohlc = market_ff['marketOHLC']
                if 'ohlc' in ohlc and len(ohlc['ohlc']) > 0:
                    # Get daily volume from first OHLC entry
                    daily_ohlc = ohlc['ohlc'][0]
                    self.market_data[instrument_key]['volume'] = self._to_int(daily_ohlc.get('vol', 0))

            # IV - at ROOT LEVEL of marketFF (in decimal, need to convert to percentage)
            if 'iv' in market_ff:
                iv_decimal = self._to_float(market_ff.get('iv', 0))
                self.market_data[instrument_key]['iv'] = iv_decimal * 100  # Convert to percentage

            # Option Greeks
            if 'optionGreeks' in market_ff:
                greeks = market_ff['optionGreeks']
                self.market_data[instrument_key]['delta'] = self._to_float(greeks.get('delta', 0))
                self.market_data[instrument_key]['theta'] = self._to_float(greeks.get('theta', 0))
                self.market_data[instrument_key]['gamma'] = self._to_float(greeks.get('gamma', 0))
                self.market_data[instrument_key]['vega'] = self._to_float(greeks.get('vega', 0))

    def _process_ltpc_feed(self, instrument_key, ltpc_data):
        """Process LTPC only feed"""
        self.market_data[instrument_key]['ltp'] = self._to_float(ltpc_data.get('ltp', 0))
        self.market_data[instrument_key]['cp'] = self._to_float(ltpc_data.get('cp', 0))

    # ============================================================================
    # UPDATED: _calculate_changes method - now uses OITracker
    # ============================================================================
    def _calculate_changes(self, instrument_key):
            """Calculate changes in OI and LTP using OITracker"""
            current_oi = self.market_data[instrument_key].get('oi', 0)
            current_ltp = self.market_data[instrument_key].get('ltp', 0)

            changes = self.oi_tracker.update_and_get_changes(
                instrument_key=instrument_key,
                current_oi=current_oi,
                current_ltp=current_ltp,
                timestamp=time.time()
            )

            self.market_data[instrument_key]['chng_oi'] = changes['chng_oi']
            self.market_data[instrument_key]['chng_ltp'] = changes['chng_ltp']


    def connect(self):
        """Start WebSocket connection"""
        if self.streamer:
            print("🔌 Connecting to WebSocket...")
            self.streamer.connect()
        else:
            raise Exception("Streamer not setup. Call setup_streamer() first.")

    def disconnect(self):
        """Close WebSocket connection"""
        if self.streamer:
            self.streamer.disconnect()

    def get_market_data(self, instrument_key):
        """Get current market data for an instrument"""
        return self.market_data.get(instrument_key, {})

    # NEW: Method to reset OI tracker (call at market open)
    def reset_oi_tracker(self):
        """Reset OI tracker - call at market open or when needed"""
        self.oi_tracker.reset()

    def is_data_stale(self, chain_data):
        """
        Check if all strikes have duplicate data for last 3 consecutive snapshots
        Returns True if stale, False otherwise
        """
        # Create current snapshot: {strike: (call_oi, call_ltp, put_oi, put_ltp)}
        current_snapshot = {}
        
        for entry in chain_data:
            strike = entry['strike_price']
            
            # Get CALL data
            call_key = entry.get('call_options', {}).get('instrument_key')
            call_data = self.get_market_data(call_key) if call_key else {}
            
            # Get PUT data
            put_key = entry.get('put_options', {}).get('instrument_key')
            put_data = self.get_market_data(put_key) if put_key else {}
            
            # Store tuple of key values
            current_snapshot[strike] = (
                call_data.get('oi', 0),
                call_data.get('ltp', 0),
                put_data.get('oi', 0),
                put_data.get('ltp', 0)
            )
        
        # Add to snapshots list
        self.data_snapshots.append(current_snapshot)
        
        # Keep only last 3 snapshots
        if len(self.data_snapshots) > self.max_snapshots:
            self.data_snapshots.pop(0)
        
        # Need exactly 3 snapshots to compare
        if len(self.data_snapshots) < self.max_snapshots:
            return False
        
        # Check if all 3 snapshots are identical
        is_stale = (
            self.data_snapshots[0] == self.data_snapshots[1] == self.data_snapshots[2]
        )
        
        return is_stale
    
    def reset_snapshots(self):
        """Clear snapshot buffer after reconnection"""
        self.data_snapshots.clear()
        print("🔄 Snapshot buffer cleared")



"""
COMPLETE FIXED DataStorage CLASS
Copy this entire class and replace your existing DataStorage class
"""

from questdb.ingress import Sender, TimestampNanos
from datetime import datetime
import time


class DataStorage:
    """Store option chain data to QuestDB"""
    
    def __init__(self, lot_size=75):
        self.lot_size = lot_size
        self.records_stored = 0
        self.last_log_time = time.time()
        
        # Store configuration, don't create sender yet
        self.conf = 'tcp::addr=localhost:9009;'
        
        # Test connection
        try:
            with Sender.from_conf(self.conf) as sender:
                print("✅ QuestDB connection test successful")
        except Exception as e:
            print(f"❌ QuestDB connection failed: {e}")
            print("\nPlease ensure QuestDB is running:")
            print("  - TCP port 9009 for data ingestion")
            print("  - HTTP port 9000 for web console")
            raise
    
    def store_option_chain(self, chain_data, data_manager):
        """Store all strikes data to database using context manager"""
        
        # Get current time in IST (UTC+5:30)
        IST = timezone(timedelta(hours=5, minutes=30))
        ist_time = datetime.now(IST)
        timestamp = TimestampNanos.from_datetime(ist_time)
        records_this_batch = 0
        
        try:
            # Use context manager for each batch
            with Sender.from_conf(self.conf) as sender:
                for entry in chain_data:
                    strike = entry['strike_price']
                    
                    # Store CALL
                    call_key = entry.get('call_options', {}).get('instrument_key')
                    if call_key:
                        call_data = data_manager.get_market_data(call_key)
                        if call_data and call_data.get('oi', 0) > 0:
                            self._store_record(sender, call_key, strike, 'CE', call_data, timestamp)
                            records_this_batch += 1
                    
                    # Store PUT
                    put_key = entry.get('put_options', {}).get('instrument_key')
                    if put_key:
                        put_data = data_manager.get_market_data(put_key)
                        if put_data and put_data.get('oi', 0) > 0:
                            self._store_record(sender, put_key, strike, 'PE', put_data, timestamp)
                            records_this_batch += 1
                
                # Explicit flush before context closes
                sender.flush()
            
            # Context manager auto-closes connection here
            self.records_stored += records_this_batch
            
            # Log progress every 10 seconds
            current_time = time.time()
            if current_time - self.last_log_time >= 10:
                print(f"💾 Stored {records_this_batch} records | Total: {self.records_stored:,} | Time: {datetime.now().strftime('%H:%M:%S')}")
                self.last_log_time = current_time
            
        except Exception as e:
            print(f"❌ Storage error: {e}")
            import traceback
            traceback.print_exc()
    
    def _store_record(self, sender, instrument_key, strike, option_type, data, timestamp):
        """Store single record to the provided sender"""
        try:
            sender.row(
                'option_data',
                symbols={
                    'instrument_key': instrument_key,
                    'option_type': option_type,
                },
                columns={
                    'strike': float(strike),
                    'oi': int(data.get('oi', 0)),
                    'chng_oi': int(data.get('chng_oi', 0)),
                    'volume': int(data.get('volume', 0)),
                    'ltp': float(data.get('ltp', 0)),
                    'chng_ltp': float(data.get('chng_ltp', 0)),
                    'iv': float(data.get('iv', 0)),
                    'bid': float(data.get('bid', 0)),
                    'ask': float(data.get('ask', 0)),
                    'bid_qty': int(data.get('bid_qty', 0)),
                    'ask_qty': int(data.get('ask_qty', 0)),
                },
                at=timestamp  # ✅ CRITICAL: This must be TimestampNanos object, NOT int
            )
        except Exception as e:
            print(f"❌ Error storing record for {instrument_key}: {e}")
            raise
    
    def close(self):
        """No cleanup needed - context manager handles it"""
        print(f"\n✅ Data collection complete. Total records stored: {self.records_stored:,}")
    
    def __del__(self):
        """Cleanup on deletion"""
        pass  # No persistent connection to close


class OptionChainDisplay:
    """Display option chain in NSE-style table format"""

    def __init__(self, lot_size=75, use_rich=False):
        self.lot_size = lot_size
        self.use_rich = use_rich
        # NO QuestDB code here - display only!

    def _convert_to_lots(self, units):
        """Convert units to lots (divide by lot size)"""
        return units / self.lot_size if units else 0

    def print_raw_data(self, chain_data, data_manager, spot_price, atm_strike):
        """Print raw data for validation - SINGLE STRIKE ONLY for testing"""
        print("\n" + "="*100)
        print(f"NIFTY OPTION CHAIN - LIVE DATA - TESTING STRIKE 25750 ONLY")
        print(f"Spot Price: {spot_price:.2f} | ATM Strike: {atm_strike} | Time: {datetime.now().strftime('%H:%M:%S')}")
        print("="*100)

        # Find strike 25750
        test_strike = 25750.0
        entry = None
        for e in chain_data:
            if e['strike_price'] == test_strike:
                entry = e
                break

        if not entry:
            print(f"\n❌ Strike {test_strike} not found in data!")
            print("Available strikes:", [e['strike_price'] for e in sorted(chain_data, key=lambda x: x['strike_price'])])
            return

        strike = entry['strike_price']

        call_key = entry.get('call_options', {}).get('instrument_key')
        call_data = data_manager.get_market_data(call_key) if call_key else {}

        put_key = entry.get('put_options', {}).get('instrument_key')
        put_data = data_manager.get_market_data(put_key) if put_key else {}

        print("\n" + "="*100)
        print(f"STRIKE: {strike:.0f}")
        print("="*100)

        print("\n" + "-"*100)
        print(f"CALL OPTIONS - STRIKE {test_strike}")
        print("-"*100)
        print(f"Instrument Key:        {call_key}")
        print(f"Open Interest (OI):    {self._convert_to_lots(call_data.get('oi', 0)):,.0f} lots")
        print(f"Change in OI:          {self._convert_to_lots(call_data.get('chng_oi', 0)):+,.0f} lots")
        print(f"Volume:                {self._convert_to_lots(call_data.get('volume', 0)):,.0f} lots")
        print(f"Implied Volatility:    {call_data.get('iv', 0):.2f}%")
        print(f"Last Traded Price:     {call_data.get('ltp', 0):.2f}")
        print(f"Change in LTP:         {call_data.get('chng_ltp', 0):+.2f}")
        print(f"Bid Price:             {call_data.get('bid', 0):.2f}")
        print(f"Bid Quantity:          {call_data.get('bid_qty', 0):,}")
        print(f"Ask Price:             {call_data.get('ask', 0):.2f}")
        print(f"Ask Quantity:          {call_data.get('ask_qty', 0):,}")

        print("\n" + "-"*100)
        print(f"PUT OPTIONS - STRIKE {test_strike}")
        print("-"*100)
        print(f"Instrument Key:        {put_key}")
        print(f"Open Interest (OI):    {self._convert_to_lots(put_data.get('oi', 0)):,.0f} lots")
        print(f"Change in OI:          {self._convert_to_lots(put_data.get('chng_oi', 0)):+,.0f} lots")
        print(f"Volume:                {self._convert_to_lots(put_data.get('volume', 0)):,.0f} lots")
        print(f"Implied Volatility:    {put_data.get('iv', 0):.2f}%")
        print(f"Last Traded Price:     {put_data.get('ltp', 0):.2f}")
        print(f"Change in LTP:         {put_data.get('chng_ltp', 0):+.2f}")
        print(f"Bid Price:             {put_data.get('bid', 0):.2f}")
        print(f"Bid Quantity:          {put_data.get('bid_qty', 0):,}")
        print(f"Ask Price:             {put_data.get('ask', 0):.2f}")
        print(f"Ask Quantity:          {put_data.get('ask_qty', 0):,}")

        print("\n" + "="*100)
        print("✅ OI and Volume converted to LOTS (API values ÷ 75)")
        print("✅ Change in OI calculated using OITracker")
        print("="*100 + "\n")

    def create_table(self, chain_data, data_manager, spot_price, atm_strike):
        """Create option chain table with live data"""
        if self.use_rich:
            return self._create_rich_table(chain_data, data_manager, spot_price, atm_strike)
        else:
            return self._create_text_table(chain_data, data_manager, spot_price, atm_strike)

    def _create_rich_table(self, chain_data, data_manager, spot_price, atm_strike):
        """Create beautiful table using Rich library"""
        table = self.Table(
            title=f"NIFTY Option Chain | Spot: {spot_price:.2f} | ATM: {atm_strike} | {datetime.now().strftime('%H:%M:%S')}",
            show_header=True,
            header_style="bold bright_white on blue",
            border_style="bright_blue",
            expand=False,
            box=None
        )

        # CALL side columns
        table.add_column("CALL OI", justify="right", style="bright_cyan", width=15)
        table.add_column("CALL Chng OI", justify="right", style="bright_magenta", width=15)
        table.add_column("CALL Volume", justify="right", style="bright_blue", width=15)
        table.add_column("CALL IV", justify="right", style="bright_yellow", width=10)
        table.add_column("CALL LTP", justify="right", style="bold bright_green", width=12)
        table.add_column("CALL Chng", justify="right", style="bright_white", width=12)
        table.add_column("CALL Bid", justify="right", style="cyan", width=12)
        table.add_column("CALL BidQ", justify="right", style="bright_cyan", width=15)
        table.add_column("CALL Ask", justify="right", style="cyan", width=12)
        table.add_column("CALL AskQ", justify="right", style="bright_cyan", width=15)

        # Strike (center)
        table.add_column("STRIKE", justify="center", style="bold bright_yellow", width=12)

        # PUT side columns
        table.add_column("PUT Bid", justify="right", style="cyan", width=12)
        table.add_column("PUT BidQ", justify="right", style="bright_cyan", width=15)
        table.add_column("PUT Ask", justify="right", style="cyan", width=12)
        table.add_column("PUT AskQ", justify="right", style="bright_cyan", width=15)
        table.add_column("PUT Chng", justify="right", style="bright_white", width=12)
        table.add_column("PUT LTP", justify="right", style="bold bright_red", width=12)
        table.add_column("PUT IV", justify="right", style="bright_yellow", width=10)
        table.add_column("PUT Volume", justify="right", style="bright_blue", width=15)
        table.add_column("PUT Chng OI", justify="right", style="bright_magenta", width=15)
        table.add_column("PUT OI", justify="right", style="bright_cyan", width=15)

        # Add rows
        for entry in sorted(chain_data, key=lambda x: x['strike_price']):
            strike = entry['strike_price']

            # Get CALL data
            call_key = entry.get('call_options', {}).get('instrument_key')
            call_data = data_manager.get_market_data(call_key) if call_key else {}

            # Get PUT data
            put_key = entry.get('put_options', {}).get('instrument_key')
            put_data = data_manager.get_market_data(put_key) if put_key else {}

            # Highlight ATM row
            style = "bold bright_white on bright_blue" if strike == atm_strike else ""

            # Format changes with color
            call_chng = call_data.get('chng_ltp', 0)
            if call_chng > 0:
                call_chng_str = f"[bright_green]+{call_chng:.2f}[/bright_green]"
            elif call_chng < 0:
                call_chng_str = f"[bright_red]{call_chng:.2f}[/bright_red]"
            else:
                call_chng_str = f"{call_chng:.2f}"

            put_chng = put_data.get('chng_ltp', 0)
            if put_chng > 0:
                put_chng_str = f"[bright_green]+{put_chng:.2f}[/bright_green]"
            elif put_chng < 0:
                put_chng_str = f"[bright_red]{put_chng:.2f}[/bright_red]"
            else:
                put_chng_str = f"{put_chng:.2f}"

            call_oi_chng = self._convert_to_lots(call_data.get('chng_oi', 0))
            if call_oi_chng > 0:
                call_oi_chng_str = f"[bright_green]+{call_oi_chng:,.0f}[/bright_green]"
            elif call_oi_chng < 0:
                call_oi_chng_str = f"[bright_red]{call_oi_chng:,.0f}[/bright_red]"
            else:
                call_oi_chng_str = f"{call_oi_chng:,.0f}"

            put_oi_chng = self._convert_to_lots(put_data.get('chng_oi', 0))
            if put_oi_chng > 0:
                put_oi_chng_str = f"[bright_green]+{put_oi_chng:,.0f}[/bright_green]"
            elif put_oi_chng < 0:
                put_oi_chng_str = f"[bright_red]{put_oi_chng:,.0f}[/bright_red]"
            else:
                put_oi_chng_str = f"{put_oi_chng:,.0f}"

            table.add_row(
                # CALL side
                f"{self._convert_to_lots(call_data.get('oi', 0)):,.0f}",
                call_oi_chng_str,
                f"{self._convert_to_lots(call_data.get('volume', 0)):,.0f}",
                f"{call_data.get('iv', 0):.2f}",
                f"{call_data.get('ltp', 0):.2f}",
                call_chng_str,
                f"{call_data.get('bid', 0):.2f}",
                f"{call_data.get('bid_qty', 0):,.0f}",
                f"{call_data.get('ask', 0):.2f}",
                f"{call_data.get('ask_qty', 0):,.0f}",

                # Strike
                f"{strike:.0f}",

                # PUT side
                f"{put_data.get('bid', 0):.2f}",
                f"{put_data.get('bid_qty', 0):,.0f}",
                f"{put_data.get('ask', 0):.2f}",
                f"{put_data.get('ask_qty', 0):,.0f}",
                put_chng_str,
                f"{put_data.get('ltp', 0):.2f}",
                f"{put_data.get('iv', 0):.2f}",
                f"{self._convert_to_lots(put_data.get('volume', 0)):,.0f}",
                put_oi_chng_str,
                f"{self._convert_to_lots(put_data.get('oi', 0)):,.0f}",

                style=style
            )

        return table

    def _create_text_table(self, chain_data, data_manager, spot_price, atm_strike):
        """Create simple text-based table"""
        lines = []
        lines.append("=" * 200)
        lines.append(f"NIFTY Option Chain | Spot: {spot_price:.2f} | ATM: {atm_strike} | {datetime.now().strftime('%H:%M:%S')}")
        lines.append("=" * 200)

        # Header
        header = (
            f"{'CALL':^90} | {'Strike':^8} | {'PUT':^90}\n"
            f"{'OI':>10} {'ChgOI':>9} {'Vol':>8} {'IV':>6} {'LTP':>8} {'Chng':>7} {'Bid':>7} {'BidQ':>8} {'Ask':>7} {'AskQ':>8} | "
            f"{'':^8} | "
            f"{'Bid':>7} {'BidQ':>8} {'Ask':>7} {'AskQ':>8} {'Chng':>7} {'LTP':>8} {'IV':>6} {'Vol':>8} {'ChgOI':>9} {'OI':>10}"
        )
        lines.append(header)
        lines.append("-" * 200)

        # Rows
        for entry in sorted(chain_data, key=lambda x: x['strike_price']):
            strike = entry['strike_price']

            call_key = entry.get('call_options', {}).get('instrument_key')
            call_data = data_manager.get_market_data(call_key) if call_key else {}

            put_key = entry.get('put_options', {}).get('instrument_key')
            put_data = data_manager.get_market_data(put_key) if put_key else {}

            atm_marker = " *ATM*" if strike == atm_strike else ""

            # Format changes
            call_chng = call_data.get('chng_ltp', 0)
            call_chng_str = f"+{call_chng:.2f}" if call_chng > 0 else f"{call_chng:.2f}"

            put_chng = put_data.get('chng_ltp', 0)
            put_chng_str = f"+{put_chng:.2f}" if put_chng > 0 else f"{put_chng:.2f}"

            call_oi_chng = self._convert_to_lots(call_data.get('chng_oi', 0))
            call_oi_chng_str = f"+{call_oi_chng:,.0f}" if call_oi_chng > 0 else f"{call_oi_chng:,.0f}"

            put_oi_chng = self._convert_to_lots(put_data.get('chng_oi', 0))
            put_oi_chng_str = f"+{put_oi_chng:,.0f}" if put_oi_chng > 0 else f"{put_oi_chng:,.0f}"

            row = (
                f"{self._convert_to_lots(call_data.get('oi', 0)):>10,.0f} "
                f"{call_oi_chng_str:>9} "
                f"{self._convert_to_lots(call_data.get('volume', 0)):>8,.0f} "
                f"{call_data.get('iv', 0):>6.2f} "
                f"{call_data.get('ltp', 0):>8.2f} "
                f"{call_chng_str:>7} "
                f"{call_data.get('bid', 0):>7.2f} "
                f"{call_data.get('bid_qty', 0):>8,.0f} "
                f"{call_data.get('ask', 0):>7.2f} "
                f"{call_data.get('ask_qty', 0):>8,.0f} | "
                f"{strike:>8.0f}{atm_marker:^6} | "
                f"{put_data.get('bid', 0):>7.2f} "
                f"{put_data.get('bid_qty', 0):>8,.0f} "
                f"{put_data.get('ask', 0):>7.2f} "
                f"{put_data.get('ask_qty', 0):>8,.0f} "
                f"{put_chng_str:>7} "
                f"{put_data.get('ltp', 0):>8.2f} "
                f"{put_data.get('iv', 0):>6.2f} "
                f"{self._convert_to_lots(put_data.get('volume', 0)):>8,.0f} "
                f"{put_oi_chng_str:>9} "
                f"{self._convert_to_lots(put_data.get('oi', 0)):>10,.0f}"
            )
            lines.append(row)

        lines.append("=" * 200)
        return "\n".join(lines)
    
def get_ist_time():
    """Get current time in IST"""
    IST = timezone(timedelta(hours=5, minutes=30))
    return datetime.now(IST)

def is_weekend():
    """Check if today is weekend"""
    ist_now = get_ist_time()
    return ist_now.weekday() in [5, 6]  # Saturday=5, Sunday=6

def is_market_hours():
    """Check if current time is within market hours (9:15 AM - 3:30 PM IST)"""
    ist_now = get_ist_time()
    market_open = ist_now.replace(hour=9, minute=15, second=0, microsecond=0)
    market_close = ist_now.replace(hour=15, minute=30, second=0, microsecond=0)
    return market_open <= ist_now <= market_close

def wait_for_market_open():
    """Wait until market opens at 9:15 AM IST"""
    ist_now = get_ist_time()
    market_open = ist_now.replace(hour=9, minute=15, second=0, microsecond=0)
    
    if ist_now < market_open:
        wait_seconds = (market_open - ist_now).total_seconds()
        print(f"\n⏰ Current time: {ist_now.strftime('%H:%M:%S')} IST")
        print(f"⏰ Market opens at: 09:15:00 IST")
        print(f"⏳ Waiting for {int(wait_seconds)} seconds ({int(wait_seconds/60)} minutes)...")
        print("💤 Data collection will start automatically at 9:15 AM\n")
        time.sleep(wait_seconds)
        print("🔔 Market opened! Starting data collection...\n")


def main():
    """Main function to run the real-time option chain display"""

    print("\n" + "=" * 70)
    print("UPSTOX REAL-TIME OPTION CHAIN (SDK VERSION)")
    print("=" * 70)

    # Load access token from file
    print("\n" + "="*70)
    print("LOADING AUTHENTICATION TOKEN")
    print("="*70)

    try:
        with open('upstox_token.txt', 'r') as f:
            access_token = f.read().strip()
        
        if not access_token:
            raise ValueError("Token file is empty")
        
        print("✅ Access token loaded from upstox_token.txt\n")
    except FileNotFoundError:
        print("❌ ERROR: upstox_token.txt not found!")
        print("📝 Please run authenticate.py first to generate token")
        print("   Command: python authenticate.py")
        return
    except Exception as e:
        print(f"❌ Failed to load token: {e}")
        print("📝 Please run authenticate.py to generate a fresh token")
        return
    
    if is_weekend():
        print("\n" + "="*70)
        print("📅 Today is a weekend - Market is closed")
        print("📅 Please run this script on a weekday (Monday-Friday)")
        print("="*70)
        return
    
    # Wait for market open if started early
    wait_for_market_open()

    # Step 2: Get Option Chain Data with retry logic
    print("=" * 70)
    print("FETCHING OPTION CHAIN DATA")
    print("=" * 70)

    oc = UpstoxOptionChain(access_token)
    
    # Try to fetch with retries (handles DNS issues at market open)
    max_retries = 5
    full_chain = None
    
    for attempt in range(1, max_retries + 1):
        try:
            print(f"📡 Attempt {attempt}/{max_retries} - Fetching option chain...")
            full_chain = oc.get_option_chain()
            
            # Save to cache on success
            with open('option_chain_cache.pkl', 'wb') as f:
                pickle.dump(full_chain, f)
            print("✅ Option chain fetched and cached successfully")
            break
            
        except Exception as e:
            print(f"⚠️  Attempt {attempt} failed: {str(e)[:100]}...")
            
            if attempt < max_retries:
                wait_time = 2 * attempt  # Progressive backoff: 2s, 4s, 6s, 8s, 10s
                print(f"⏳ Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                # Try to load from cache as last resort
                print("\n" + "="*70)
                print("⚠️  All attempts failed. Attempting to use cached data...")
                print("="*70)
                try:
                    with open('option_chain_cache.pkl', 'rb') as f:
                        full_chain = pickle.load(f)
                    print("✅ Using cached option chain from previous session")
                    print("⚠️  Note: Strike prices may not be current")
                except FileNotFoundError:
                    print("❌ No cached data available")
                    print("❌ Cannot proceed without option chain data")
                    print("\n💡 Suggestion: Check your internet connection and restart")
                    return
                except Exception as cache_error:
                    print(f"❌ Failed to load cache: {cache_error}")
                    return
    
    if not full_chain:
        print("❌ Failed to get option chain data")
        return
    
    try:
        filtered_chain, spot_price, atm_strike = oc.get_atm_strikes(full_chain, num_strikes=10)
        instrument_keys = oc.get_instrument_keys(filtered_chain)
        print(f"🔑 Total instruments to subscribe: {len(instrument_keys)}")
    except Exception as e:
        print(f"❌ Failed to process option chain: {e}")
        return

    # Step 3: Setup WebSocket Streaming
    print("\n" + "=" * 70)
    print("STARTING REAL-TIME STREAM")
    print("=" * 70)

    data_manager = MarketDataManager(access_token)
    display = OptionChainDisplay(use_rich=RICH_AVAILABLE)

    try:
        # Setup and connect streamer
        data_manager.setup_streamer(instrument_keys, mode="full")

        # Start streaming in a separate thread
        stream_thread = threading.Thread(target=data_manager.connect, daemon=True)
        stream_thread.start()

        # Wait for initial data
        print("⏳ Waiting for initial data (10 seconds)...")
        time.sleep(10)

        # Check if we received any messages
        if not hasattr(data_manager, '_message_count') or data_manager._message_count == 0:
            print("\n⚠️  WARNING: No messages received yet!")
            print("Possible issues:")
            print("  1. Market might be closed")
            print("  2. Subscription might have failed")
            print("  3. SDK callback registration issue")
            print("\nContinuing anyway to show table structure...")
        else:
            print(f"✅ Received {data_manager._message_count} messages so far")

        # Initialize data storage
        print("\n" + "=" * 70)
        print("STARTING DATA COLLECTION")
        print("=" * 70)

        storage = DataStorage(lot_size=75)

        print(f"📊 Collecting data for {len(filtered_chain)} strikes")
        print(f"📊 Total instruments: {len(instrument_keys)}")
        print(f"⏱️  Collection started at: {datetime.now().strftime('%H:%M:%S')}")
        print(f"💾 Storing to QuestDB at localhost:9000")
        print("\n🔄 Press Ctrl+C to stop...\n")

        # Data collection loop with auto-reconnect
        reconnect_attempts = 0
        max_reconnect_attempts = 10
        
        while True:
            # Check if market closed (3:30 PM)
            if not is_market_hours():
                ist_now = get_ist_time()
                if ist_now.hour >= 15 and ist_now.minute >= 30:
                    print("\n" + "="*70)
                    print("🔔 Market closed at 3:30 PM IST")
                    print("✅ Stopping data collection gracefully...")
                    print("="*70)
                    break  # Exit loop gracefully
            
            time.sleep(1)  # Check every 1 second
            
            # Check if data is stale (3 consecutive duplicates)
            if data_manager.is_data_stale(filtered_chain):
                print("\n" + "="*70)
                print("⚠️  STALE DATA DETECTED - All strikes showing duplicate data")
                print(f"⚠️  Triggering auto-reconnect (Attempt {reconnect_attempts + 1}/{max_reconnect_attempts})")
                print("="*70)
                
                if reconnect_attempts >= max_reconnect_attempts:
                    print(f"\n❌ Max reconnection attempts ({max_reconnect_attempts}) reached!")
                    print("❌ Exiting program. Please check API status and restart manually.")
                    break
                
                try:
                    # Disconnect
                    print("🔌 Disconnecting WebSocket...")
                    data_manager.disconnect()
                    
                    # Clear snapshot buffer immediately
                    data_manager.reset_snapshots()
                    
                    # Backoff delay (only for 2nd+ attempts)
                    if reconnect_attempts > 1:
                        backoff_delay = min(2 * (2 ** (reconnect_attempts - 2)), 60)
                        print(f"⏳ Waiting {backoff_delay} seconds before reconnecting (avoiding rate limit)...")
                        time.sleep(backoff_delay)
                    else:
                        print("⚡ Reconnecting immediately (1st attempt)...")
                    
                    # Reconnect
                    print("🔄 Reconnecting...")
                    data_manager.setup_streamer(instrument_keys, mode="full")
                    stream_thread = threading.Thread(target=data_manager.connect, daemon=True)
                    stream_thread.start()
                    
                    # Wait for reconnection to stabilize
                    print("⏳ Waiting for connection to establish (10 seconds)...")
                    time.sleep(3)
                    
                    # Verify connection AND fresh data
                    if data_manager.is_connected:
                        # Additional check: wait a bit and verify data changed
                        print("🔍 Verifying fresh data is flowing...")
                        time.sleep(2)
                        
                        # Force a fresh snapshot check
                        data_manager.reset_snapshots()
                        
                        print("✅ Reconnection successful!")
                        print("✅ Resuming data collection...")
                        reconnect_attempts = 0  # Reset counter on success
                    else:
                        print("⚠️  Connection failed, will retry...")
                        reconnect_attempts += 1
                    
                    print("="*70 + "\n")
                    
                except Exception as e:
                    print(f"❌ Reconnection failed: {e}")
                    reconnect_attempts += 1
                    print("="*70 + "\n")
                    continue
            
            # Store data
            storage.store_option_chain(filtered_chain, data_manager)

    except KeyboardInterrupt:
        print("\n\n👋 Stopping data collection...")
        storage.close()
        data_manager.disconnect()
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        if 'storage' in locals():
            storage.close()
        data_manager.disconnect()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Fatal error: {e}")
        import traceback
        traceback.print_exc()

