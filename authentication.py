"""
Upstox Authentication Handler
Generates and saves access token for daily trading sessions
Run this ONCE at the start of each trading day
"""

import requests
import os
from urllib.parse import urlencode, parse_qs, urlparse
from dotenv import load_dotenv
from datetime import datetime


class UpstoxAuthenticator:
    """Handles Upstox API authentication"""

    def __init__(self, api_key, api_secret, redirect_uri="https://www.google.com"):
        self.api_key = api_key
        self.api_secret = api_secret
        self.redirect_uri = redirect_uri
        self.base_url = "https://api.upstox.com/v2"

    def get_login_url(self):
        """Generate login URL for browser authentication"""
        params = {
            'response_type': 'code',
            'client_id': self.api_key,
            'redirect_uri': self.redirect_uri
        }
        return f"{self.base_url}/login/authorization/dialog?{urlencode(params)}"

    def extract_code_from_url(self, redirect_url):
        """Extract authorization code from redirect URL"""
        parsed = urlparse(redirect_url)
        params = parse_qs(parsed.query)
        if 'code' not in params:
            raise ValueError("No authorization code found in URL")
        return params['code'][0]

    def get_access_token(self, auth_code):
        """Exchange authorization code for access token"""
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

    def save_token(self, token, filepath='upstox_token.txt'):
        """Save token to file with timestamp"""
        try:
            with open(filepath, 'w') as f:
                f.write(token)
            
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            print(f"\n✅ Token saved to: {filepath}")
            print(f"✅ Generated at: {timestamp}")
            print(f"✅ Valid for: ~24 hours")
            return True
        except Exception as e:
            print(f"❌ Failed to save token: {e}")
            return False


def main():
    """Main authentication flow"""
    
    print("\n" + "="*70)
    print("UPSTOX AUTHENTICATION")
    print("="*70)
    print("This script generates a fresh access token for today's trading session")
    print("="*70 + "\n")

    # Load credentials from .env
    load_dotenv()
    
    API_KEY = os.getenv('API_KEY')
    API_SECRET = os.getenv('API_SECRET')
    
    if not API_KEY or not API_SECRET:
        print("❌ ERROR: API_KEY or API_SECRET not found in .env file!")
        print("\n📝 Please create a .env file with:")
        print("   API_KEY=your_api_key_here")
        print("   API_SECRET=your_api_secret_here")
        return False
    
    print("✅ Credentials loaded from .env file\n")
    
    # Initialize authenticator
    auth = UpstoxAuthenticator(API_KEY, API_SECRET)
    
    # Generate login URL
    login_url = auth.get_login_url()
    
    print("="*70)
    print("STEP 1: LOGIN TO UPSTOX")
    print("="*70)
    print("\n🔗 Open this URL in your browser:\n")
    print(f"    {login_url}\n")
    print("="*70)
    print("📱 Complete OTP verification in browser")
    print("📋 Copy the FULL redirect URL after login")
    print("="*70 + "\n")
    
    # Get redirect URL from user
    redirect_url = input("🔗 Paste the redirect URL here: ").strip()
    
    if not redirect_url:
        print("\n❌ No URL provided. Authentication cancelled.")
        return False
    
    try:
        # Extract auth code
        print("\n⏳ Processing authentication...")
        auth_code = auth.extract_code_from_url(redirect_url)
        print("✅ Authorization code extracted")
        
        # Get access token
        access_token = auth.get_access_token(auth_code)
        print("✅ Access token generated")
        
        # Save token
        if auth.save_token(access_token):
            print("\n" + "="*70)
            print("🎉 AUTHENTICATION SUCCESSFUL!")
            print("="*70)
            print("✅ Token file: upstox_token.txt")
            print("✅ You can now run:")
            print("   - T9_Option_Chain.py")
            print("   - T8_Nifty50_Data.py")
            print("="*70 + "\n")
            return True
        else:
            return False
            
    except ValueError as e:
        print(f"\n❌ Invalid redirect URL: {e}")
        print("💡 Make sure you copied the COMPLETE URL from browser address bar")
        return False
    except requests.exceptions.HTTPError as e:
        print(f"\n❌ API Error: {e}")
        print("💡 The authorization code may have expired. Please try again.")
        return False
    except Exception as e:
        print(f"\n❌ Authentication failed: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    try:
        success = main()
        exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n⚠️  Authentication cancelled by user")
        exit(1)
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        exit(1)