"""
Emergency Hedge Tool
Neutralize any unhedged positions across 01 Exchange and Lighter DEX.
Use this if the bot crashed or missed a hedge.
"""

import asyncio
import logging
import sys

import config
from exchange_01 import Exchange01Client
from lighter_client import LighterClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-5s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


async def main():
    logger.info("=" * 60)
    logger.info("  EMERGENCY HEDGE TOOL")
    logger.info("  Neutralizing net position...")
    logger.info("=" * 60)

    # Initialize clients
    o1 = Exchange01Client()
    lighter = LighterClient()

    try:
        logger.info("Connecting to exchanges...")
        o1.initialize()
        await lighter.initialize()

        # Get positions
        pos_01 = o1.get_position()
        pos_lighter = await lighter.get_position()

        net_pos = pos_01 + pos_lighter
        logger.info(f"\n📊 STATUS")
        logger.info(f"  01 Exchange: {pos_01:+.5f} BTC")
        logger.info(f"  Lighter DEX: {pos_lighter:+.5f} BTC")
        logger.info(f"  NET EXPOSURE: {net_pos:+.5f} BTC")

        if abs(net_pos) < 0.0001:
            logger.info("\n✅ Net position is already neutral (or close enough).")
            logger.info("You can restart the main bot safely.")
            return

        # Prepare hedge
        hedge_size = abs(net_pos)
        hedge_side = "sell" if net_pos > 0 else "buy"
        
        print(f"\n⚠️  UNHEDGED EXPOSURE DETECTED: {net_pos:+.5f} BTC")
        print(f"   Action required: {hedge_side.upper()} {hedge_size:.5f} BTC on Lighter")
        
        confirm = input("\nExecute hedge on Lighter now? (y/n): ")
        if confirm.lower() != 'y':
            logger.info("Operation cancelled.")
            return

        # Execute
        logger.info(f"Executing {hedge_side} {hedge_size:.5f} BTC on Lighter...")
        response, price = await lighter.place_taker_order(
            side=hedge_side,
            size=hedge_size,
            slippage_bps=50  # Wider slippage for emergency
        )

        if response:
            logger.info(f"✅ Hedge executed @ ${price:,.2f}")
            logger.info("System is now neutral. You can restart the bot.")
        else:
            logger.error("❌ Hedge failed! Please check logs/keys manually.")

    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
    finally:
        await lighter.close()
        logger.info("\nDone.")


if __name__ == "__main__":
    asyncio.run(main())
