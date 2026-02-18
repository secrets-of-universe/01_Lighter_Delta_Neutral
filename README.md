# Delta Neutral Market Maker (01 Exchange ↔ Lighter DEX)

A robust, delta-neutral market making bot that farms volume/rewards on **01 Exchange** (Maker) by hedging instantly on **Lighter DEX** (Taker).

## Features
- **Delta Neutral:** Maintains 0 exposure by instantly hedging every fill.
- **Volume Farming:** Maximizes maker volume on 01 Exchange.
- **Resilient:** Handles API timeouts, partial fills, and "Post-Only" rejections automatically.
- **Telegram Bot:** Monitor status, check balances, and change settings (Size, Hold Time, Cooldown) on the fly.
- **Safety:** 
  - Liquidation protection monitoring.
  - "Double Spend" prevention on unwinds.
  - Auto-pause on margin errors.

## Requirements
- Python 3.10+
- 01 Exchange Account (with funds deposited).
- Lighter DEX Account (with funds deposited).
- Telegram Bot Token (for controls).

## Setup

1. **Clone the repository:**
   ```bash
   git clone https://github.com/yourusername/delta-neutral-mm.git
   cd delta-neutral-mm
   ```

2. **Install dependencies:**
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

3. **Configure Environment:**
   Copy the example file and fill in your keys:
   ```bash
   cp .env.example .env
   nano .env
   ```
   *   `O1_PRIVATE_KEY`: Your 01 Exchange private key.
   *   `LIGHTER_API_KEY_PRIVATE_KEY`: Your Lighter API key private key (not wallet PK).
   *   `TELEGRAM_BOT_TOKEN`: From @BotFather.

4. **Run:**
   ```bash
   python main.py
   ```

## Configuration
You can adjust settings in `config.py` or dynamically via Telegram:

- **Order Size:** `/set SIZE 1000 1500` (USD range)
- **Hold Time:** `/set HOLD 15 30` (Minutes)
- **Cooldown:** `/set COOLDOWN 2 5` (Minutes between cycles)

## Disclaimer
This software is for educational purposes only. Use at your own risk. The authors are not responsible for any financial losses.
