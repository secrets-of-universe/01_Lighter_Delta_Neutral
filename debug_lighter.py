import asyncio
import os
import logging
from dotenv import load_dotenv
import lighter
import config

# Load env (config already does, but just in case)
load_dotenv()

# Setup simplified logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("debug")

async def main():
    logger.info(f"Connecting to {config.LIGHTER_API_URL}...")
    
    api_config = lighter.Configuration(host=config.LIGHTER_API_URL)
    api_client = lighter.ApiClient(configuration=api_config)
    account_api = lighter.AccountApi(api_client)

    acc_index = str(config.LIGHTER_ACCOUNT_INDEX)
    logger.info(f"Fetching account index: {acc_index}")

    try:
        account = await account_api.account(by="index", value=acc_index)
        
        if hasattr(account, "accounts") and account.accounts:
            acct = account.accounts[0]
            logger.info("--- Account Data ---")
            
            if hasattr(acct, "positions") and acct.positions:
                logger.info(f"Found {len(acct.positions)} positions:")
                for pos in acct.positions:
                    print(f"--- Market ID: {pos.market_id} ---")
                    print(f"  Symbol: {pos.symbol}")
                    print(f"  Size (str): {pos.position}")
                    print(f"  Sign: {pos.sign}")
                    print(f"  Value: {pos.position_value}")
                    print(f"  PNL: {pos.unrealized_pnl}")
            else:
                logger.info("No positions found.")
        else:
            logger.info("No account found.")

    except Exception as e:
        logger.error(f"Error: {e}")
    finally:
        await api_client.close()

if __name__ == "__main__":
    asyncio.run(main())
