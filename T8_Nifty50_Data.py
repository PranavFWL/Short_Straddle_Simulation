"""
Upstox Nifty50 Real-Time LTP Collection with QuestDB Storage
"""

import upstox_client
import time
import requests
import threading
from datetime import datetime, timezone, timedelta, time as dt_time
from urllib.parse import urlencode, parse_qs, urlparse
from collections import defaultdict
import os
from dotenv import load_dotenv
from questdb.ingress import Sender, TimestampNanos
import pickle


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


class NiftyDataStorage:
    """Store Nifty50 LTP data to QuestDB"""
    
    def __init__(self):
        self.conf = 'tcp::addr=localhost:9009;'
        self.records_stored = 0
        self.last_log_time = time.time()
        
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
    
    def store_ltp(self, ltp):
        """Store single LTP value with IST timestamp"""
        try:
            # Get IST timestamp
            IST = timezone(timedelta(hours=5, minutes=30))
            ist_time = datetime.now(IST)
            timestamp = TimestampNanos.from_datetime(ist_time)
            
            # Use context manager for each write
            with Sender.from_conf(self.conf) as sender:
                sender.row(
                    'nifty_ltp',
                    columns={
                        'ltp': float(ltp)
                    },
                    at=timestamp
                )
                sender.flush()
            
            self.records_stored += 1
            
            # Log progress every 10 seconds
            current_time = time.time()
            if current_time - self.last_log_time >= 10:
                print(f"💾 Stored {self.records_stored:,} records | Time: {ist_time.strftime('%H:%M:%S')}")
                self.last_log_time = current_time
                
        except Exception as e:
            print(f"❌ Storage error: {e}")
    
    def close(self):
        """Print summary"""
        print(f"\n✅ Data collection complete. Total records stored: {self.records_stored:,}")


class NiftyDataManager:
    """Manages Nifty50 real-time data with stale detection"""
    
    def __init__(self, access_token):
        self.access_token = access_token
        self.ltp_data = {}
        self.streamer = None
        self.is_connected = False
        
        # Stale data detection
        self.data_snapshots = []
        self.max_snapshots = 3
        
    def setup_streamer(self):
        """Setup SDK streamer"""
        configuration = upstox_client.Configuration()
        configuration.access_token = self.access_token
        
        instrument_key = "NSE_INDEX|Nifty 50"
        
        self.streamer = upstox_client.MarketDataStreamerV3(
            upstox_client.ApiClient(configuration),
            [instrument_key],
            "full"
        )
        
        self.streamer.on("message", self._on_message)
        self.streamer.on("open", self._on_open)
        self.streamer.on("error", self._on_error)
        self.streamer.on("close", self._on_close)
        
        print(f"📡 Streamer configured for Nifty 50")
    
    def _on_open(self):
        """Called when WebSocket connection opens"""
        self.is_connected = True
        print("✅ WebSocket connected!")
    
    def _on_message(self, message):
        """Process incoming market data"""
        if isinstance(message, dict) and 'feeds' in message:
            for instrument_key, data in message['feeds'].items():
                
                # Try LTPC feed
                if 'ltpc' in data:
                    ltp = data['ltpc'].get('ltp', 0)
                    self.ltp_data['ltp'] = float(ltp) if ltp else 0
                
                # Try Full feed - INDEX data
                elif 'fullFeed' in data:
                    if 'indexFF' in data['fullFeed']:
                        index_ff = data['fullFeed']['indexFF']
                        
                        if 'ltpc' in index_ff:
                            ltpc = index_ff['ltpc']
                            ltp = ltpc.get('ltp', 0)
                            self.ltp_data['ltp'] = float(ltp) if ltp else 0
    
    def _on_error(self, error):
        """Handle WebSocket errors"""
        if "401 Unauthorized" in str(error):
            print(f"\n❌ WebSocket error: Access token expired!")
            print("❌ Please restart the program to re-authenticate")
        else:
            print(f"❌ WebSocket error: {error}")
    
    def _on_close(self, close_status_code=None, close_msg=None):
        """Called when WebSocket connection closes"""
        self.is_connected = False
        print(f"🔌 WebSocket disconnected (code: {close_status_code}, msg: {close_msg})")
    
    def is_data_stale(self):
        """Check if LTP has been duplicate for last 3 snapshots"""
        current_ltp = self.ltp_data.get('ltp', 0)
        
        # Add to snapshots list
        self.data_snapshots.append(current_ltp)
        
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
    
    def get_current_ltp(self):
        """Get current LTP value"""
        return self.ltp_data.get('ltp', 0)
    
    def connect(self):
        """Start WebSocket connection"""
        if self.streamer:
            print("🔌 Connecting to WebSocket...")
            self.streamer.connect()
        else:
            raise Exception("Streamer not setup")
    
    def disconnect(self):
        """Close WebSocket connection"""
        if self.streamer:
            self.streamer.disconnect()


def get_ist_time():
    """Get current time in IST"""
    IST = timezone(timedelta(hours=5, minutes=30))
    return datetime.now(IST)


def is_weekend():
    """Check if today is weekend"""
    ist_now = get_ist_time()
    return ist_now.weekday() in [5, 6]


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
    print("\n" + "="*70)
    print("UPSTOX NIFTY50 LTP COLLECTION (QUESTDB STORAGE)")
    print("="*70)
    
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
    
    # Check weekend
    if is_weekend():
        print("\n📅 Today is a weekend - Market is closed")
        print("📅 Please run this script on a weekday")
        return
    
    # Wait for market open
    wait_for_market_open()
    
    # Initialize storage
    print("="*70)
    print("INITIALIZING STORAGE")
    print("="*70 + "\n")
    
    try:
        storage = NiftyDataStorage()
    except Exception as e:
        print(f"❌ Failed to initialize storage: {e}")
        return
    
    # Setup data manager
    print("="*70)
    print("STARTING REAL-TIME STREAM")
    print("="*70 + "\n")
    
    data_manager = NiftyDataManager(access_token)
    
    # Try initial connection with retries
    max_connection_attempts = 5
    connected = False
    
    for attempt in range(1, max_connection_attempts + 1):
        try:
            print(f"📡 Connection attempt {attempt}/{max_connection_attempts}...")
            
            # Setup and connect
            data_manager.setup_streamer()
            
            stream_thread = threading.Thread(target=data_manager.connect, daemon=True)
            stream_thread.start()
            
            print("⏳ Waiting for initial connection (5 seconds)...")
            time.sleep(5)
            
            # Check if connected
            if data_manager.is_connected:
                print("✅ Initial connection successful!\n")
                connected = True
                break
            else:
                raise Exception("Connection timeout")
                
        except Exception as e:
            print(f"⚠️  Attempt {attempt} failed: {str(e)[:100]}...")
            
            if attempt < max_connection_attempts:
                wait_time = 2 * attempt
                print(f"⏳ Retrying in {wait_time} seconds...\n")
                time.sleep(wait_time)
            else:
                print("\n❌ Failed to establish initial connection after all attempts")
                print("💡 Please check your internet and restart")
                return
    
    if not connected:
        print("❌ Could not establish connection")
        return
    
    try:
        # Main loop with auto-reconnect and storage
        reconnect_attempts = 0
        max_reconnect_attempts = 10
        
        print("🔄 Data collection started... Press Ctrl+C to stop\n")
        
        last_ltp = None
        data_count = 0
        
        while True:
            # Check if market closed
            if not is_market_hours():
                ist_now = get_ist_time()
                if ist_now.hour >= 15 and ist_now.minute >= 30:
                    print("\n" + "="*70)
                    print("🔔 Market closed at 3:30 PM IST")
                    print("✅ Stopping data collection gracefully...")
                    print("="*70)
                    break
            
            time.sleep(1)  # Check every second
            
            # Get and print current LTP
            current_ltp = data_manager.get_current_ltp()
            if current_ltp > 0:
                data_count += 1
                ist_now = get_ist_time()
                
                # Print EVERY data point with timestamp
                print(f"[{data_count:05d}] {ist_now.strftime('%H:%M:%S')} | LTP: {current_ltp:.2f}")
                
                # Store to QuestDB
                storage.store_ltp(current_ltp)
                
                last_ltp = current_ltp
            
            # Check for stale data
            if data_manager.is_data_stale():
                reconnect_attempts += 1
                
                print("\n" + "="*70)
                print("⚠️  STALE DATA DETECTED - Same LTP for 3 consecutive seconds")
                print(f"⚠️  Triggering auto-reconnect (Attempt {reconnect_attempts}/{max_reconnect_attempts})")
                print("="*70)
                
                if reconnect_attempts > max_reconnect_attempts:
                    print(f"\n❌ Max reconnection attempts ({max_reconnect_attempts}) reached!")
                    print("❌ Exiting program")
                    break
                
                try:
                    # Disconnect
                    print("🔌 Disconnecting WebSocket...")
                    data_manager.disconnect()
                    
                    # Clear snapshot buffer
                    data_manager.reset_snapshots()
                    
                    # Backoff delay (only for 2nd+ attempts)
                    if reconnect_attempts > 1:
                        backoff_delay = min(2 * (2 ** (reconnect_attempts - 2)), 60)
                        print(f"⏳ Waiting {backoff_delay} seconds before reconnecting...")
                        time.sleep(backoff_delay)
                    else:
                        print("⚡ Reconnecting immediately (1st attempt)...")
                    
                    # Reconnect
                    print("🔄 Reconnecting...")
                    data_manager.setup_streamer()
                    stream_thread = threading.Thread(target=data_manager.connect, daemon=True)
                    stream_thread.start()
                    
                    # Wait for connection
                    print("⏳ Waiting for connection to establish (3 seconds)...")
                    time.sleep(3)
                    
                    # Verify connection
                    if data_manager.is_connected:
                        print("🔍 Verifying fresh data is flowing...")
                        time.sleep(2)
                        
                        # Force fresh snapshot check
                        data_manager.reset_snapshots()
                        
                        print("✅ Reconnection successful!")
                        print("✅ Resuming data collection...")
                        reconnect_attempts = 0  # Reset counter
                    else:
                        print("⚠️  Connection failed, will retry...")
                    
                    print("="*70 + "\n")
                    
                except Exception as e:
                    print(f"❌ Reconnection failed: {e}")
                    print("="*70 + "\n")
                    continue

    except KeyboardInterrupt:
        print("\n\n👋 Stopping data collection...")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        data_manager.disconnect()
        storage.close()
        print("✅ Session ended")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Fatal error: {e}")
        import traceback
        traceback.print_exc()