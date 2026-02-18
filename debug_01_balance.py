import config
import requests
import json
from base58 import b58decode, b58encode
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

def main():
    print(f"Checking balance against: {config.O1_API_URL}")
    
    # 1. Derive Pubkey
    try:
        key_bytes = b58decode(config.O1_PRIVATE_KEY)
        private_key = Ed25519PrivateKey.from_private_bytes(key_bytes[:32])
        pubkey_bytes = private_key.public_key().public_bytes_raw()
        pubkey_str = b58encode(pubkey_bytes).decode()
        print(f"User Pubkey: {pubkey_str}")
    except Exception as e:
        print(f"Error deriving key: {e}")
        return

    # 2. Get Account ID
    try:
        user_resp = requests.get(f"{config.O1_API_URL}/user/{pubkey_str}")
        user_resp.raise_for_status()
        user_data = user_resp.json()
        print(f"User Data: {json.dumps(user_data, indent=2)}")
        
        account_ids = user_data.get("accountIds", [])
        if not account_ids:
            print("No account_ids found!")
            return
            
        account_id = account_ids[0]
        print(f"Using Account ID: {account_id}")
        
    except Exception as e:
        print(f"Error fetching user: {e}")
        return

    # 3. Get Account Balance
    try:
        account_resp = requests.get(f"{config.O1_API_URL}/account/{account_id}")
        account_resp.raise_for_status()
        account_data = account_resp.json()
        print(f"Account Data (Raw): {json.dumps(account_data, indent=2)}")
        
        # Check specific fields
        collateral = float(account_data.get("collateral", 0))
        free = float(account_data.get("freeCollateral", 0))
        equity = float(account_data.get("equity", 0))
        print(f"Parsed: Collateral={collateral}, Free={free}, Equity={equity}")
        
    except Exception as e:
        print(f"Error fetching account: {e}")

if __name__ == "__main__":
    main()
