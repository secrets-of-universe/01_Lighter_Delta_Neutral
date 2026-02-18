
import logging
import asyncio
import time
from telegram import Update
from telegram.ext import ApplicationBuilder, ContextTypes, CommandHandler
import config
import config_manager

logger = logging.getLogger(__name__)

class TelegramBot:
    def __init__(self, farmer):
        self.farmer = farmer
        self.app = None

        # State Icon Map
        self.STATE_EMOJIS = {
            "IDLE": "💤",
            "OPENING": "🟢",
            "HEDGING": "🛡️",
            "HOLDING": "⏳",
            "CLOSING": "🔴",
            "UNWINDING": "🔄",
            "COOLDOWN": "❄️",
            "PAUSED": "⏸️",
        }
        
        if not config.TELEGRAM_BOT_TOKEN:
            logger.warning("⚠️ No TELEGRAM_BOT_TOKEN found in .env — Bot disabled")
            return

        self.app = ApplicationBuilder().token(config.TELEGRAM_BOT_TOKEN).build()
        
        # Wire up alert callback so farmer can push notifications
        self.farmer.alert_callback = self.send_alert
        
        # Register commands
        self.app.add_handler(CommandHandler("start", self.cmd_start))
        self.app.add_handler(CommandHandler("stop", self.cmd_stop))
        self.app.add_handler(CommandHandler("status", self.cmd_status))
        self.app.add_handler(CommandHandler("balance", self.cmd_balance))
        self.app.add_handler(CommandHandler("config", self.cmd_config))
        self.app.add_handler(CommandHandler("set", self.cmd_set))
        self.app.add_handler(CommandHandler("help", self.cmd_help))

    async def send_alert(self, message: str):
        """Send a push alert to the configured chat."""
        if not self.app or not config.TELEGRAM_ALLOWED_CHAT_ID:
            return
        try:
            await self.app.bot.send_message(
                chat_id=config.TELEGRAM_ALLOWED_CHAT_ID,
                text=message,
            )
        except Exception as e:
            logger.error(f"Telegram alert failed: {e}")

    async def run(self):
        """Run the bot polling loop."""
        if not self.app:
            return

        logger.info("🤖 Telegram Bot starting...")
        
        # Initialize with retry — don't crash the whole bot for a network blip
        for attempt in range(3):
            try:
                await self.app.initialize()
                await self.app.start()
                await self.app.updater.start_polling()
                break
            except Exception as e:
                logger.warning(f"⚠️ Telegram init failed (attempt {attempt+1}/3): {e}")
                if attempt < 2:
                    await asyncio.sleep(5)
                else:
                    logger.error("❌ Telegram Bot failed to start — bot will run without Telegram control")
                    return
        
        # Keep running until cancelled
        try:
            while True:
                await asyncio.sleep(3600)
        except asyncio.CancelledError:
            logger.info("🤖 Telegram Bot stopping...")
            await self.app.updater.stop()
            await self.app.stop()
            await self.app.shutdown()

    def _check_auth(self, update: Update) -> bool:
        """Check if user is authorized."""
        user_id = str(update.effective_user.id)
        if config.TELEGRAM_ALLOWED_CHAT_ID and user_id != config.TELEGRAM_ALLOWED_CHAT_ID:
            logger.warning(f"Unauthorized access attempt from ID: {user_id}")
            return False
        return True

    async def cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._check_auth(update): return
        
        if self.farmer._enabled:
            await update.message.reply_text("✅ Bot is already enabled and running!")
        else:
            self.farmer._enabled = True
            await update.message.reply_text("🚀 Cycle started! Monitoring market...")

    async def cmd_stop(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._check_auth(update): return
        
        self.farmer._enabled = False
        await update.message.reply_text("🛑 Pausing after current cycle completes...")

    async def cmd_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._check_auth(update): return
        
        state = self.farmer.state
        if not self.farmer._enabled:
            state = "PAUSED (Finishing Cycle)" if state != "PAUSED" else "PAUSED (Idle)"
        
        # Add emoji prefix
        icon = self.STATE_EMOJIS.get(self.farmer.state, "❓")
        state_display = f"{icon} {state}"
            
        pos = self.farmer.o1.get_position()
        
        # Calculate USD value
        # Use open_price if active, else last known price, else 0
        ref_price = self.farmer.open_price
        if ref_price == 0 and self.farmer.last_cycle_stats:
            ref_price = self.farmer.last_cycle_stats.get("price", 0)
            
        usd_val = abs(pos * ref_price)
        usd_str = f" (~${usd_val:,.0f})" if usd_val > 10 else ""
        
        # Get active orders
        orders = []
        if self.farmer.bid_order_id: orders.append(f"BID {self.farmer.bid_order_id}")
        if self.farmer.ask_order_id: orders.append(f"ASK {self.farmer.ask_order_id}")
        if self.farmer.close_order_id: orders.append(f"CLOSE {self.farmer.close_order_id}")
        
        msg = (
            f"📊 **STATUS**\n"
            f"State: `{state_display}`\n"
            f"Position: `{pos:.5f} BTC{usd_str}`\n"
            f"Active Orders: `{', '.join(orders) or 'None'}`\n"
            f"Cycles: `{self.farmer.cycle_count}`\n"
        )
        
        # Show time remaining if holding
        if self.farmer.state == "HOLDING":
            try:
                elapsed = time.time() - self.farmer.hold_start_time
                remaining = max(0, self.farmer.hold_duration_s - elapsed)
                rem_min = int(remaining / 60)
                rem_sec = int(remaining % 60)
                msg += f"⏳ Wait: `{rem_min}m {rem_sec}s`\n"
            except Exception:
                 pass
        
        # Show time remaining if cooling down
        if self.farmer.state == "COOLDOWN":
            try:
                elapsed = time.time() - self.farmer.cooldown_start_time
                remaining = max(0, self.farmer.cooldown_duration_s - elapsed)
                rem_min = int(remaining / 60)
                rem_sec = int(remaining % 60)
                msg += f"⏳ Cooldown: `{rem_min}m {rem_sec}s` remaining\n"
            except Exception:
                 pass
        
        if self.farmer.last_cycle_stats:
            ls = self.farmer.last_cycle_stats
            msg += (
                f"\n**Last Cycle:**\n"
                f"Tx: {ls['side'].upper()} {ls['size']:.4f} BTC @ ${ls['price']:,.0f}\n"
                f"Vol: ${ls['volume_usd']:,.2f}\n"
            )
        await update.message.reply_text(msg, parse_mode="Markdown")

    async def cmd_balance(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._check_auth(update): return
        
        # 01 Exchange Balance
        bal_01 = self.farmer.o1.get_balance()
        pos_01 = self.farmer.o1.get_position()
        
        # Lighter Balance
        bal_lighter = await self.farmer.lighter.get_balance()
        pos_lighter = await self.farmer.lighter.get_position()
        
        # Calculate PnL
        pnl_01 = bal_01['equity'] - self.farmer.start_equity_01
        pnl_lighter = bal_lighter['equity'] - self.farmer.start_equity_lighter
        
        # Colorize PnL
        pnl_01_str = f"+${pnl_01:,.2f}" if pnl_01 >= 0 else f"-${abs(pnl_01):,.2f}"
        pnl_lighter_str = f"+${pnl_lighter:,.2f}" if pnl_lighter >= 0 else f"-${abs(pnl_lighter):,.2f}"

        msg = (
            f"💰 **BALANCES**\n\n"
            f"**01 Exchange:**\n"
            f"  💵 Collateral: `${bal_01['collateral']:,.2f}`\n"
            f"  🆓 Free: `${bal_01['free_collateral']:,.2f}`\n"
            f"  📉 Equity: `${bal_01['equity']:,.2f}`\n"
            f"  📈 **Session PnL: {pnl_01_str}**\n"
            f"  🟠 Position: `{pos_01:.5f} BTC`\n\n"
            f"**Lighter DEX:**\n"
            f"  💵 Collateral: `${bal_lighter['collateral']:,.2f}`\n"
            f"  🆓 Free: `${bal_lighter['free_collateral']:,.2f}`\n"
            f"  📉 Equity: `${bal_lighter['equity']:,.2f}`\n"
            f"  📈 **Session PnL: {pnl_lighter_str}**\n"
            f"  🟠 Position: `{pos_lighter:.5f} BTC`\n"
        )
        await update.message.reply_text(msg, parse_mode="Markdown")

    async def cmd_config(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._check_auth(update): return
        
        settings = config_manager.get_settings_display()
        msg = f"⚙️ <b>SETTINGS</b>\n\n{settings}"
        await update.message.reply_text(msg, parse_mode="HTML")

    async def cmd_set(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Change a setting. Usage: /set PARAM VALUE"""
        if not self._check_auth(update): return
        
        args = context.args
        if not args or len(args) < 2:
            help_text = config_manager.get_help_text()
            await update.message.reply_text(
                f"Usage: `/set PARAM VALUE`\n\n{help_text}",
                parse_mode="Markdown",
            )
            return

        key = args[0].upper()
        value_parts = args[1:]

        success, message = config_manager.update_setting(key, value_parts)
        await update.message.reply_text(message, parse_mode="Markdown")

    async def cmd_help(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._check_auth(update): return
        
        # Base commands
        msg = (
            "🤖 **COMMANDS**\n"
            "/start - Start farming\n"
            "/stop - Stop after current phase\n"
            "/status - Show bot state & position\n"
            "/balance - Show wallet balances (01 + Lighter)\n"
            "/config - Show current settings\n"
            "/set - Change a setting (see below)\n"
            "/help - Show this message\n\n"
        )
        
        # Append dynamic settings list
        msg += config_manager.get_help_text()
        
        await update.message.reply_text(msg, parse_mode="Markdown")
