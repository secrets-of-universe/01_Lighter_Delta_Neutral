"""
Cycle Farmer — Configuration
All tunable parameters in one place.
"""

import os
from dotenv import load_dotenv

load_dotenv()


# ─── 01 Exchange ────────────────────────────────────────────────────────────────

O1_API_URL = "https://zo-mainnet.n1.xyz"
O1_PRIVATE_KEY = os.getenv("O1_PRIVATE_KEY", "")

# BTC market on 01 Exchange
O1_MARKET_ID = 0
O1_SYMBOL = "BTCUSD"


# ─── Lighter DEX ────────────────────────────────────────────────────────────────

LIGHTER_API_URL = os.getenv("LIGHTER_API_URL", "https://mainnet.zklighter.elliot.ai")
LIGHTER_API_KEY_PRIVATE_KEY = os.getenv("LIGHTER_API_KEY_PRIVATE_KEY", "")
LIGHTER_API_KEY_INDEX = int(os.getenv("LIGHTER_API_KEY_INDEX", "0"))
LIGHTER_ACCOUNT_INDEX = int(os.getenv("LIGHTER_ACCOUNT_INDEX", "0"))

# BTC market on Lighter
LIGHTER_MARKET_ID = 1


# ─── Cycle Strategy ─────────────────────────────────────────────────────────────

# Randomization Ranges
ORDER_SIZE_RANGE_USD = (1000, 1300)  # Min/Max order size per cycle
HOLD_DURATION_RANGE_S = (10 * 60, 15 * 60)  # Min/Max hold duration
HOLD_DURATION_RANGE_S = (10 * 60, 15 * 60)  # Min/Max hold duration
COOLDOWN_MINUTES_RANGE = (3, 5)  # Min/Max cooldown minutes between cycles
ORDER_TIMEOUT_S = 5 * 60     # Cancel unfilled OPEN limit after 5 minutes, retry
CLOSE_REPRICE_S = 30         # Re-price close order every 30s to chase market
CLOSE_BUFFER_USD = 20.0       # How far from BBO to place close order (dollars) -> Higher = safer maker, slower fill
SPREAD_OFFSET_BPS = 4        # Spread offset for opening orders (bps from Lighter mid)
LEVERAGE = 40                # Max leverage used on exchanges (for margin calculations)


# ─── Timing ──────────────────────────────────────────────────────────────────────

POLL_INTERVAL_S = 2           # How often to check for fills
STATUS_INTERVAL_S = 30        # How often to log status during hold
SESSION_DURATION_S = 3600     # 01 Exchange session expiry (1 hour)


# ─── Safety ──────────────────────────────────────────────────────────────────────

DRY_RUN = False               # Set True to log orders without sending
HEDGE_SLIPPAGE_BPS = 10      # Max slippage for Lighter taker hedge (basis points)

# ─── Telegram Bot ───────────────────────────────────────────────────────────────

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_ALLOWED_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")  # Only allow commands from this ID
