"""
Cycle Farmer — Entry Point
Delta neutral volume farming bot for 01 Exchange + Lighter DEX.
"""

import asyncio
import logging
import sys

import config
import config_manager


def setup_logging():
    """Configure structured logging."""
    fmt = "%(asctime)s │ %(levelname)-5s │ %(message)s"
    datefmt = "%H:%M:%S"

    logging.basicConfig(
        level=logging.INFO,
        format=fmt,
        datefmt=datefmt,
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    # Silence noisy libraries
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("telegram").setLevel(logging.WARNING)

    # Rotating File Handler: Max 5MB per file, keep 2 backups (~15MB total)
    from logging.handlers import RotatingFileHandler
    fh = RotatingFileHandler("cycle_farmer.log", maxBytes=5*1024*1024, backupCount=2)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(
        "%(asctime)s  |  %(levelname)-8s  |  %(message)s",
        datefmt="%Y/%m/%d %H:%M:%S.%f",
    ))
    logging.getLogger().addHandler(fh)


def validate_config():
    """Check that all required credentials are set."""
    errors = []

    if not config.O1_PRIVATE_KEY:
        errors.append("O1_PRIVATE_KEY not set in .env")
    if not config.LIGHTER_API_KEY_PRIVATE_KEY:
        errors.append("LIGHTER_API_KEY_PRIVATE_KEY not set in .env")

    if errors:
        for e in errors:
            print(f"❌ {e}")
        sys.exit(1)


def print_banner():
    """Show startup configuration."""
    mode = "⚠️  DRY RUN MODE" if config.DRY_RUN else "🔴 LIVE TRADING"
    
    # Format ranges
    size_str = f"${config.ORDER_SIZE_RANGE_USD[0]}-${config.ORDER_SIZE_RANGE_USD[1]}"
    hold_str = f"{config.HOLD_DURATION_RANGE_S[0]//60}-{config.HOLD_DURATION_RANGE_S[1]//60} min"
    
    print(f"\n{mode}\n")
    print("╔══════════════════════════════════════════════════════════╗")
    print("║           CYCLE FARMER — Volume Mining                  ║")
    print("║     01 Exchange (Maker) ↔ Lighter DEX (Taker)          ║")
    print("╠══════════════════════════════════════════════════════════╣")
    print(f"║  Order Size:   {size_str:<42}║")
    print(f"║  Spread Off:   {config.SPREAD_OFFSET_BPS} bps{' ' * 40}║")
    print(f"║  Hold Time:    {hold_str:<42}║")
    cooldown_str = f"{config.COOLDOWN_MINUTES_RANGE[0]}-{config.COOLDOWN_MINUTES_RANGE[1]} min"
    print(f"║  Cooldown:     {cooldown_str:<42}║")
    print(f"║  Telegram:     {'ENABLED' if config.TELEGRAM_BOT_TOKEN else 'DISABLED':<42}║")
    print("╚══════════════════════════════════════════════════════════╝")
    print()


async def main():
    setup_logging()
    validate_config()
    config_manager.load_overrides()  # Apply saved Telegram settings
    print_banner()

    from cycle_farmer import CycleFarmer
    farmer = CycleFarmer()

    # Initialize Telegram Bot (optional)
    bot = None
    if config.TELEGRAM_BOT_TOKEN:
        from telegram_bot import TelegramBot
        bot = TelegramBot(farmer)

    try:
        tasks = [asyncio.create_task(farmer.run())]
        if bot:
            tasks.append(asyncio.create_task(bot.run()))
        
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        pass
    finally:
        print("\n👋 Goodbye!")


if __name__ == "__main__":
    asyncio.run(main())
