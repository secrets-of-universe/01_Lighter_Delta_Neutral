"""
Cycle Farmer — Delta Neutral Volume Farming

State machine that cycles positions to farm 01 Exchange volume/points:
  OPENING → HEDGING → HOLDING → CLOSING → UNWINDING → COOLDOWN → repeat

Both open and close on 01 are limit orders (maker rebates).
Both hedges on Lighter are taker market orders (0% fee).
"""

import asyncio
import time
import logging
import random

import config
from exchange_01 import Exchange01Client
from lighter_client import LighterClient

logger = logging.getLogger(__name__)


class CycleFarmer:
    """
    Delta Neutral Cycle Farmer.

    Each cycle:
    1. Place bid + ask limits on 01 (whichever fills first wins)
    2. Cancel unfilled side, hedge on Lighter (taker)
    3. Hold hedged position for HOLD_DURATION_S
    4. Place limit close on 01 (opposite side)
    5. When close fills, unwind Lighter hedge (taker)
    6. Cooldown, then repeat
    """

    # States
    IDLE = "IDLE"
    OPENING = "OPENING"
    HEDGING = "HEDGING"
    HOLDING = "HOLDING"
    CLOSING = "CLOSING"
    UNWINDING = "UNWINDING"
    COOLDOWN = "COOLDOWN"

    def __init__(self):
        self.o1 = Exchange01Client()
        self.lighter = LighterClient()

        self.state = self.IDLE
        self.cycle_count = 0

        # Current cycle tracking
        self.open_side: str | None = None       # "bid" or "ask"
        self.open_size: float = 0.0             # BTC size
        self.open_price: float = 0.0            # Fill price on 01
        self.bid_order_id: int | None = None
        self.ask_order_id: int | None = None
        self.close_order_id: int | None = None
        self.hold_start_time: float = 0.0
        self.last_cycle_stats: dict | None = None  # Stores info about last finished cycle
        self.hold_duration_s: float = 0.0 # Duration of current hold phase
        self.cooldown_duration_s: float = 0.0 # Duration of current cooldown
        self.cooldown_start_time: float = 0.0
        
        self._pre_open_pos: float = 0.0  # Position before opening (for corrective hedge)
        self.hedge_price: float = 0.0    # Execution price on Lighter
        
        # Session PnL Tracking
        self.start_equity_01: float = 0.0
        self.start_equity_lighter: float = 0.0

        self.cycle_count = 0
        self._running = False
        self._enabled = True  # Controlled by Telegram (True=Farm, False=Pause after cycle)
        self.alert_callback = None  # Set by TelegramBot for push alerts

    async def run(self):
        """Main entry point."""
        logger.info("=" * 60)
        logger.info("  CYCLE FARMER — Delta Neutral Volume Farming")
        logger.info("  01 Exchange (Maker) ↔ Lighter DEX (Taker)")
        logger.info("=" * 60)

        try:
            await self._initialize()
            await self._run_cycles()
        except KeyboardInterrupt:
            logger.info("Shutdown signal received.")
        except Exception as e:
            logger.error(f"Fatal error: {e}", exc_info=True)
        finally:
            await self._shutdown()

    async def _initialize(self):
        """Initialize both exchange clients."""
        logger.info("─── Initializing 01 Exchange ───")
        self.o1.initialize()

        logger.info("─── Initializing Lighter DEX ───")
        await self.lighter.initialize()

        logger.info("─── Initialization Complete ───\n")

        # Check for existing unexpected positions
        pos_01 = self.o1.get_position()
        if abs(pos_01) > 0.00001:
            logger.warning("⚠️" * 30)
            logger.warning(
                f" DETECTED EXISTING 01 POSITION: {pos_01:.5f} BTC "
                f"(~${abs(pos_01 * 69000):.2f})"
            )
            logger.warning(
                " Recommended: Close manually or run 'python emergency_hedge.py' "
                "before starting for a clean slate."
            )
            logger.warning("⚠️" * 30)
            # Optional: wait so user sees it
            await asyncio.sleep(5)

        # Check for existing Lighter positions
        pos_lighter = await self.lighter.get_position()
        if abs(pos_lighter) > 0.00001:
            logger.warning("⚠️" * 30)
            logger.warning(
                f" DETECTED EXISTING LIGHTER POSITION: {pos_lighter:.5f} BTC "
                f"(~${abs(pos_lighter * 69000):.2f})"
            )
            logger.warning(" Recommended: Close manually before starting.")
            logger.warning("⚠️" * 30)
            await asyncio.sleep(5)

        # Capture Start Equity for Session PnL
        try:
            bal_01 = self.o1.get_balance()
            self.start_equity_01 = bal_01.get("equity", 0.0)
            
            bal_lighter = await self.lighter.get_balance()
            self.start_equity_lighter = bal_lighter.get("equity", 0.0) # Use equity field or collateral
            # Note: lighter_client.get_balance returns free/collateral. 
            # We should check what lighter_client.get_balance actually returns.
            # Assuming standard dict structure.
            
            logger.info(f"💰 Session Start Equity: 01=${self.start_equity_01:.2f} | Lighter=${self.start_equity_lighter:.2f}")
        except Exception as e:
            logger.error(f"Failed to capture start equity: {e}")

    async def _send_alert(self, message: str):
        """Send alert via Telegram callback (if set), otherwise just log."""
        if self.alert_callback:
            try:
                await self.alert_callback(message)
            except Exception as e:
                logger.error(f"Failed to send Telegram alert: {e}")

    async def _run_cycles(self):
        """Run cycles forever."""
        self._running = True

        while self._running:
            # Check pause state (graceful stop after cycle)
            if not self._enabled:
                if self.state != "PAUSED":
                    logger.info("⏸️ Bot paused. Waiting for /start command...")
                    self.state = "PAUSED"
                await asyncio.sleep(1)
                continue

            self.cycle_count += 1
            logger.info(f"\n{'━' * 60}")
            logger.info(f"  🔄 CYCLE {self.cycle_count}")
            logger.info(f"{'━' * 60}")

            try:
                # ── Pre-cycle Balance Check ──
                # Check 01 Exchange
                bal_01 = self.o1.get_balance()
                free_col_01 = bal_01["free_collateral"]
                
                # Check Lighter DEX
                bal_lighter = await self.lighter.get_balance()
                free_col_lighter = bal_lighter["free_collateral"]

                # Margin needed = order_size / leverage, with 50% safety buffer
                min_required = (config.ORDER_SIZE_RANGE_USD[1] / config.LEVERAGE) * 1.5
                
                if free_col_01 > 0 and free_col_01 < min_required:
                    msg = (
                        f"⚠️ **LOW BALANCE** (01 Exchange)\n"
                        f"Current: ${free_col_01:,.2f}\n"
                        f"Required: ${min_required:,.0f}\n\n"
                        f"🛑 Bot PAUSED.\n"
                        f"👉 Top up funds, then type `/start` to resume."
                    )
                    logger.warning(msg)
                    await self._send_alert(msg)
                    self._enabled = False
                    continue

                if free_col_lighter > 0 and free_col_lighter < min_required:
                    msg = (
                        f"⚠️ **LOW BALANCE** (Lighter DEX)\n"
                        f"Current: ${free_col_lighter:,.2f}\n"
                        f"Required: ${min_required:,.0f}\n\n"
                        f"🛑 Bot PAUSED.\n"
                        f"👉 Top up funds, then type `/start` to resume."
                    )
                    logger.warning(msg)
                    await self._send_alert(msg)
                    self._enabled = False
                    continue

                # ── Fill Accumulation Loop ──
                # Keep placing orders + hedging partials until full target is filled.
                lighter_bid, lighter_ask = await self.lighter.get_best_bid_ask()
                if lighter_bid <= 0 or lighter_ask <= 0:
                    logger.error("Cannot fetch Lighter BBO — skipping cycle")
                    await asyncio.sleep(10)
                    continue

                lighter_mid = (lighter_bid + lighter_ask) / 2
                # Randomize size for this cycle
                target_usd = random.uniform(*config.ORDER_SIZE_RANGE_USD)
                target_size = round(target_usd / lighter_mid, 5)
                
                total_filled = 0.0
                fill_attempts = 0
                max_attempts = 10  # Keep trying to fill large positions
                locked_side = None  # After first fill, lock to same side

                logger.info(
                    f"🎯 Target: {target_size:.5f} BTC (${target_usd:.2f})"
                )

                while total_filled < target_size * 0.95 and self._running and self._enabled:
                    remaining = round(target_size - total_filled, 5)
                    fill_attempts += 1

                    if fill_attempts > max_attempts:
                        logger.warning(
                            f"⚠️ Max fill attempts ({max_attempts}) reached. "
                            f"Filled {total_filled:.5f} / {target_size:.5f} BTC"
                        )
                        break

                    if fill_attempts > 1:
                        side_str = locked_side.upper() if locked_side else "EITHER"
                        logger.info(
                            f"📋 Placing remainder: {remaining:.5f} BTC "
                            f"(locked to {side_str}, "
                            f"filled {total_filled:.5f} / {target_size:.5f})"
                        )

                    # Phase 1: Open (locked_side ensures same direction)
                    filled = await self._phase_opening(remaining, locked_side)
                    if not filled:
                        self._cancel_open_orders()  # Safety: ensure no orphaned orders
                        if total_filled > 0:
                            logger.info(
                                f"No more fills — proceeding with {total_filled:.5f} BTC"
                            )
                            break
                        continue  # Nothing at all, retry the cycle

                    # Lock to the side of the first fill
                    if locked_side is None:
                        locked_side = self.open_side
                        logger.info(f"🔒 Side locked to {locked_side.upper()}")

                    # Phase 2: Hedge this fill immediately
                    hedged = await self._phase_hedging()
                    if not hedged:
                        logger.error("❌ HEDGE FAILED — emergency close. PAUSING BOT.")
                        await self._emergency_close()
                        self._enabled = False  # Critical: Stop farming
                        total_filled = 0
                        break

                    hedged_size = self.open_size

                    # Corrective hedge: check for dual-fill race
                    # (other side may have filled between detection and cancel)
                    await asyncio.sleep(1)
                    settled_pos = self.o1.get_position()
                    settled_delta = abs(settled_pos - self._pre_open_pos)
                    correction = round(settled_delta - hedged_size, 5)

                    if correction > 0.00001:
                        logger.info(
                            f"🔧 Corrective hedge: {correction:.5f} BTC "
                            f"(dual-fill detected)"
                        )
                        self.open_size = correction
                        corrected = await self._phase_hedging()
                        if corrected:
                            hedged_size += correction
                        else:
                            logger.warning("⚠️ Corrective hedge failed — small exposure gap")

                    total_filled += hedged_size
                    logger.info(
                        f"📊 Accumulated: {total_filled:.5f} / {target_size:.5f} BTC "
                        f"({total_filled/target_size*100:.0f}%)"
                    )

                if total_filled < 0.00001 or not self._running:
                    continue  # Nothing filled, try again

                # Update open_size to total accumulated for closing/unwinding
                self.open_size = total_filled

                # Phase 3: Hold
                await self._phase_holding()
                if not self._running:
                    break

                # Phase 4: Close position on 01 (chases market as maker)
                closed = await self._phase_closing()
                if not closed or not self._running:
                    break

                # Phase 5: Unwind Lighter hedge
                await self._phase_unwinding()

                # Phase 6: Cooldown
                await self._phase_cooldown()

            except Exception as e:
                logger.error(f"Cycle error: {e}", exc_info=True)
                await self._send_alert(f"🚨 CRITICAL CYCLE ERROR: {e}\nPausing bot.")
                self._enabled = False  # Critical: Stop farming
                await self._emergency_close()
                await asyncio.sleep(10)

        logger.info("Cycle loop ended.")

    # ─── Phase 1: OPENING ────────────────────────────────────────────────────

    async def _phase_opening(self, size: float = None, locked_side: str = None) -> bool:
        """
        Place bid + ask limits on 01, pegged to Lighter BBO.
        Wait for either to fill. Cancel the other.
        If locked_side is set, only place on that side (for remainder fills).
        Returns True if a fill was detected.
        """
        self.state = self.OPENING

        # Get Lighter BBO as price reference
        lighter_bid, lighter_ask = await self.lighter.get_best_bid_ask()
        if lighter_bid <= 0 or lighter_ask <= 0:
            logger.error("Cannot fetch Lighter BBO — skipping")
            return False

        lighter_mid = (lighter_bid + lighter_ask) / 2

        # `size` is always passed from _run_cycles now
        self.open_size = size

        # Place bids/asks on 01 pegged to Lighter prices
        offset = lighter_mid * (config.SPREAD_OFFSET_BPS / 10000)
        bid_price = round(lighter_bid - offset, 1)
        ask_price = round(lighter_ask + offset, 1)

        # SAFETY: Check 01 BBO to ensure we don't cross the book (Post-Only fail)
        # If our calculated price crosses the existing book, clamp it to "Join" or "Improve" without taking.
        try:
            o1_bid, o1_ask = self.o1.get_best_bid_ask()
            tick_size = 0.1  # BTCUSD on 01 usually 0.1 tick

            if o1_ask > 0 and bid_price >= o1_ask:
                logger.info(f"⚠️ Calculated BID (${bid_price}) crosses 01 ASK (${o1_ask}). Clamping to Maker level.")
                bid_price = o1_ask - tick_size

            if o1_bid > 0 and ask_price <= o1_bid:
                logger.info(f"⚠️ Calculated ASK (${ask_price}) crosses 01 BID (${o1_bid}). Clamping to Maker level.")
                ask_price = o1_bid + tick_size
                
        except Exception as e:
            logger.warning(f"Failed to fetch 01 BBO/clamp prices: {e}")

        # Place on locked side only, or both sides for initial order
        try:
            if locked_side == "bid":
                logger.info(
                    f"📋 OPENING | Lighter BBO: ${lighter_bid:,.1f} / ${lighter_ask:,.1f}\n"
                    f"   Placing on 01: BID ${bid_price:,.1f} × {self.open_size:.5f} BTC"
                )
                self.bid_order_id = self.o1.place_limit_order("bid", bid_price, self.open_size, post_only=True)
                self.ask_order_id = None
            elif locked_side == "ask":
                logger.info(
                    f"📋 OPENING | Lighter BBO: ${lighter_bid:,.1f} / ${lighter_ask:,.1f}\n"
                    f"   Placing on 01: ASK ${ask_price:,.1f} × {self.open_size:.5f} BTC"
                )
                self.ask_order_id = self.o1.place_limit_order("ask", ask_price, self.open_size, post_only=True)
                self.bid_order_id = None
            else:
                logger.info(
                    f"📋 OPENING | Lighter BBO: ${lighter_bid:,.1f} / ${lighter_ask:,.1f}\n"
                    f"   Placing on 01: BID ${bid_price:,.1f} / ASK ${ask_price:,.1f} "
                    f"× {self.open_size:.5f} BTC"
                )
                # Place each side independently — if one fails POST_ONLY, keep the other
                try:
                    self.bid_order_id = self.o1.place_limit_order("bid", bid_price, self.open_size, post_only=True)
                except Exception as bid_err:
                    if "POST_ONLY" in str(bid_err):
                        logger.warning(f"⚠️ BID crossed book (Post-Only). Skipping bid side.")
                        self.bid_order_id = None
                    else:
                        raise
                
                try:
                    self.ask_order_id = self.o1.place_limit_order("ask", ask_price, self.open_size, post_only=True)
                except Exception as ask_err:
                    if "POST_ONLY" in str(ask_err):
                        logger.warning(f"⚠️ ASK crossed book (Post-Only). Skipping ask side.")
                        self.ask_order_id = None
                    else:
                        raise
                
                # If BOTH sides failed, wait and retry
                if self.bid_order_id is None and self.ask_order_id is None:
                    logger.warning("⚠️ Both sides crossed book. Waiting 3s before retry...")
                    await asyncio.sleep(3)
                    return False
        except Exception as e:
            error_str = str(e)
            
            # 1. Post-Only Error: Price crossed book (Transient). Retry with delay.
            if "POST_ONLY" in error_str:
                self._cancel_open_orders()
                logger.warning(f"⚠️ Limit order crossed book (Post-Only). Waiting 2s before retry...")
                await asyncio.sleep(2)
                return False

            # 2. Critical Errors: Margin, Risk, Unhealthy. Pause bot.
            elif "RISK" in error_str or "UNHEALTHY" in error_str or "MARGIN" in error_str.upper():
                self._cancel_open_orders()
                msg = f"⚠️ MARGIN ERROR: {error_str}. Insufficient collateral — pausing bot."
                logger.warning(msg)
                await self._send_alert(msg)
                self._enabled = False
                return False
            
            # 3. Other errors: Re-raise
            else:
                raise

        if config.DRY_RUN:
            self.open_side = "bid"
            self.open_price = bid_price
            logger.info(f"[DRY RUN] Simulating bid fill @ ${bid_price:,.1f}")
            return True

        # Poll for fills — re-price periodically to chase market
        start_time = time.time()
        last_reprice_time = start_time
        initial_pos = self.o1.get_position()

        while self._running:
            await asyncio.sleep(config.POLL_INTERVAL_S)
            now = time.time()
            elapsed = now - start_time

            # Check position change FIRST (before timeout)
            current_pos = self.o1.get_position()
            delta = current_pos - initial_pos

            if abs(delta) > 0.00001:
                # Fill detected! Determine side from direction
                if delta > 0:
                    self.open_side = "bid"
                    self.open_price = bid_price
                else:
                    self.open_side = "ask"
                    self.open_price = ask_price

                # Cancel ALL orders immediately
                self._cancel_open_orders()

                # Hedge FAST — use first detected delta, correct later
                self.open_size = abs(delta)
                self._pre_open_pos = initial_pos  # Save for correction check

                msg = (
                    f"🔔 FILL! {self.open_side.upper()} {self.open_size:.5f} BTC "
                    f"@ ~${self.open_price:,.1f} on 01"
                )
                logger.info(msg)
                await self._send_alert(msg)

                return True

            # THEN check timeout
            if elapsed > config.ORDER_TIMEOUT_S:
                logger.warning(f"⏰ No fill after {config.ORDER_TIMEOUT_S}s — cancelling")
                self._cancel_open_orders()

                # Final position check for partial fills
                await asyncio.sleep(1)
                final_pos = self.o1.get_position()
                final_delta = final_pos - initial_pos

                if abs(final_delta) > 0.00001:
                    if final_delta > 0:
                        self.open_side = "bid"
                        self.open_price = bid_price
                    else:
                        self.open_side = "ask"
                        self.open_price = ask_price

                    self.open_size = abs(final_delta)
                    logger.info(
                        f"🔔 PARTIAL FILL detected after cancel! "
                        f"{self.open_side.upper()} {self.open_size:.5f} BTC "
                        f"@ ~${self.open_price:,.1f} — proceeding to hedge"
                    )
                    return True

                logger.info("No fill detected — retrying")
                return False

            # Re-price remainder orders only (locked side, single order)
            # Initial bid+ask don't need re-pricing — one side will get hit
            if locked_side is not None and now - last_reprice_time >= config.CLOSE_REPRICE_S:
                lighter_bid_new, lighter_ask_new = await self.lighter.get_best_bid_ask()
                if lighter_bid_new > 0 and lighter_ask_new > 0:
                    lighter_mid_new = (lighter_bid_new + lighter_ask_new) / 2
                    offset_new = lighter_mid_new * (config.SPREAD_OFFSET_BPS / 10000)

                    if locked_side == "bid":
                        new_price = round(lighter_bid_new - offset_new, 1)
                        if new_price != bid_price:
                            try:
                                # Cancel old first
                                if self.bid_order_id:
                                    try:
                                        self.o1.cancel_order(self.bid_order_id)
                                    except: pass
                                
                                bid_price = new_price
                                self.bid_order_id = self.o1.place_limit_order("bid", bid_price, self.open_size, post_only=True)
                                logger.info(f"   🔄 Re-priced remainder: BID ${bid_price:,.1f} ({elapsed:.0f}s)")
                            except Exception as e:
                                if "POST_ONLY" in str(e):
                                    logger.warning(f"⚠️ Re-price failed (Post-Only). Skipping this update.")
                                else:
                                    logger.error(f"Re-price error: {e}")

                    else:  # locked_side == "ask"
                        new_price = round(lighter_ask_new + offset_new, 1)
                        if new_price != ask_price:
                            try:
                                # Cancel old first
                                if self.ask_order_id:
                                    try:
                                        self.o1.cancel_order(self.ask_order_id)
                                    except: pass
                                
                                ask_price = new_price
                                self.ask_order_id = self.o1.place_limit_order("ask", ask_price, self.open_size, post_only=True)
                                logger.info(f"   🔄 Re-priced remainder: ASK ${ask_price:,.1f} ({elapsed:.0f}s)")
                            except Exception as e:
                                if "POST_ONLY" in str(e):
                                    logger.warning(f"⚠️ Re-price failed (Post-Only). Skipping this update.")
                                else:
                                    logger.error(f"Re-price error: {e}")

                last_reprice_time = now

            # Periodic status
            if int(elapsed) % 30 == 0 and elapsed > 5:
                logger.info(f"   ⏳ Waiting for fill... ({elapsed:.0f}s)")

        return False

    # ─── Phase 2: HEDGING ────────────────────────────────────────────────────

    async def _phase_hedging(self) -> bool:
        """Hedge the 01 fill with a taker market order on Lighter."""
        self.state = self.HEDGING

        # Opposite side for hedge
        hedge_side = "sell" if self.open_side == "bid" else "buy"

        logger.info(
            f"🛡️ HEDGING | {hedge_side.upper()} {self.open_size:.5f} BTC on Lighter"
        )

        response, exec_price = await self.lighter.place_taker_order(
            side=hedge_side,
            size=self.open_size,
            slippage_bps=config.HEDGE_SLIPPAGE_BPS,
        )
        
        if response:
            self.hedge_price = exec_price  # Store for liquidation monitoring

        if response:
            msg = f"✅ Hedge complete! Both legs open. Net exposure ≈ 0"
            logger.info(msg)
            await self._send_alert(msg)
            return True
        else:
            return False

    # ─── Phase 3: HOLDING ────────────────────────────────────────────────────

    async def _phase_holding(self):
        """Wait for the hold duration while monitoring for liquidation risk."""
        self.state = self.HOLDING
        
        # Randomize hold duration
        hold_s = random.uniform(*config.HOLD_DURATION_RANGE_S)
        self.hold_duration_s = hold_s
        hold_mins = hold_s / 60
        
        self.hold_start_time = time.time()
        logger.info(
            f"⏳ HOLDING for {hold_mins:.1f} minutes... "
            f"(Monitoring liquidation risk every 15s)"
        )
        
        while self._running:
            now = time.time()
            accumulated_time = now - self.hold_start_time
            remaining = hold_s - accumulated_time
            
            if remaining <= 0:
                logger.info("⏳ Hold complete.")
                return 

            # Status update every minute
            if int(accumulated_time) % 60 < 15 and accumulated_time > 10:
                logger.info(
                    f"   ⏳ Holding... {accumulated_time/60:.1f}m elapsed, "
                    f"{remaining/60:.1f}m remaining | Side: {self.open_side} | "
                    f"Size: {self.open_size:.5f} BTC"
                )

            # Liquidation Check (every 15s)
            check_interval = 15
            sleep_time = min(remaining, check_interval)
            
            # Use wait_for to allow cancellation
            try:
                await asyncio.sleep(sleep_time)
            except asyncio.CancelledError:
                raise

            # Get current market price (Lighter mid is a good proxy for both)
            bid, ask = await self.lighter.get_best_bid_ask()
            if bid > 0 and ask > 0:
                mid_price = (bid + ask) / 2
                
                # Check 01 Leg (Maker)
                o1_pnl_pct = 0.0
                if self.open_side == "bid":  # Long
                    o1_pnl_pct = (mid_price - self.open_price) / self.open_price * config.LEVERAGE
                else:  # Short
                    o1_pnl_pct = (self.open_price - mid_price) / self.open_price * config.LEVERAGE
                
                # Check Lighter Leg (Taker)
                lighter_pnl_pct = 0.0
                # Hedge side is opposite of open_side
                if self.open_side == "bid":  # Hedge is Short
                    limit_price = self.hedge_price or self.open_price # Fallback
                    lighter_pnl_pct = (limit_price - mid_price) / limit_price * config.LEVERAGE
                else:  # Hedge is Long
                    limit_price = self.hedge_price or self.open_price
                    lighter_pnl_pct = (mid_price - limit_price) / limit_price * config.LEVERAGE

                # Threshold: -80% of margin (e.g. at 40x, that's a -2% price move)
                # Lighter liq fee is 1% of notional, so likely triggers around -1.5% to -2% move.
                # -0.8 ensures we get out before -1.0 (bankruptcy)
                threshold = -0.80
                
                if o1_pnl_pct < threshold or lighter_pnl_pct < threshold:
                    msg = (
                        f"🚨 LIQUIDATION RISK! "
                        f"01 PnL: {o1_pnl_pct*100:.1f}% | Lighter PnL: {lighter_pnl_pct*100:.1f}% "
                        f"(Threshold: {threshold*100}%). Emergency Closing!"
                    )
                    logger.warning(msg)
                    await self._send_alert(msg)
                    return # Exit hold loop to trigger close phase immediately
            # Safety check: if position closed somehow externally, break early
            if accumulated_time % 60 == 0:
                pos = self.o1.get_position()
                if abs(pos) < 0.00001:
                    logger.warning("⚠️ Position closed externally! Ending hold early.")
                    break

    # ─── Phase 4: CLOSING ────────────────────────────────────────────────────

    async def _phase_closing(self) -> bool:
        """
        Close the 01 position with a limit order (maker).
        Chase the market: cancel & re-place every CLOSE_REPRICE_S seconds.
        Handles partial fills by incrementally unwinding Lighter.
        """
        self.state = self.CLOSING

        # Determine close side (Opposite of open)
        close_side = "ask" if self.open_side == "bid" else "bid"
        
        # Determine Lighter unwind side based on open side. 
        # If we opened Long on 01 (bid), we are Short on Lighter. Unwind = Buy.
        # If we opened Short on 01 (ask), we are Long on Lighter. Unwind = Sell.
        lighter_unwind_side = "buy" if self.open_side == "bid" else "sell"

        last_reprice_time = 0.0
        current_close_price = 0.0
        
        # Track position to detect fills
        initial_pos = self.o1.get_position()
        last_checked_pos = initial_pos
        
        # If already flat?
        if abs(initial_pos) < 0.00001:
             logger.info("📋 CLOSING | Position already flat on 01.")
             return True

        logger.info(f"📋 CLOSING | Chasing market as MAKER ({close_side.upper()})...")

        if config.DRY_RUN:
            lighter_bid, lighter_ask = await self.lighter.get_best_bid_ask()
            close_price = lighter_bid if close_side == "ask" else lighter_ask
            logger.info(f"[DRY RUN] Simulating close fill @ ${close_price:,.1f}")
            return True

        start_time = time.time()

        while self._running:
            now = time.time()
            elapsed = now - start_time
            
            # Check current position (source of truth)
            current_pos = self.o1.get_position()
            remaining_size = abs(current_pos)
            
            # Check for partial fills (Position reduced)
            # Use abs() because position can be negative (Short on 01)
            # Delta is how much size we closed since last check
            delta = abs(last_checked_pos) - remaining_size
            
            # Update tracker
            last_checked_pos = current_pos

            if delta > 0.00001:
                logger.info(f"🔔 Partial Close Fill detected: {delta:.5f} BTC. Unwinding Lighter immediately...")
                
                # Unwind the filled amount on Lighter immediately
                # "Fire and forget" approach within the loop to keep closng 01
                # _phase_unwinding will catch any cleanup later
                try:
                    await self.lighter.place_taker_order(
                        side=lighter_unwind_side,
                        size=delta,
                        slippage_bps=config.HEDGE_SLIPPAGE_BPS
                    )
                except Exception as e:
                    logger.error(f"Failed to unwind partial fill on Lighter: {e}")
            
            # If flat, we are done
            if remaining_size < 0.00001:
                logger.info("✅ 01 Position closed successfully (Flat).")
                # Cancel close order if exists
                if self.close_order_id:
                     try:
                         self.o1.cancel_order(self.close_order_id)
                     except: pass
                     self.close_order_id = None
                return True

            # Re-price the close order periodically
            if now - last_reprice_time >= config.CLOSE_REPRICE_S:
                # Get fresh 01 BBO
                o1_bbo = self.o1.get_best_bid_ask()
                o1_bid, o1_ask = o1_bbo[0], o1_bbo[1]
                if o1_bid <= 0 or o1_ask <= 0:
                    logger.warning("Cannot fetch 01 BBO — will retry...")
                    await asyncio.sleep(config.POLL_INTERVAL_S)
                    continue

                # Place close order "further away" (Maker)
                buffer = config.CLOSE_BUFFER_USD
                
                if close_side == "ask":
                    new_price = round(o1_ask + buffer, 1)
                else:
                    new_price = round(o1_bid - buffer, 1)

                # Only update if price changed
                if new_price != current_close_price:
                    logger.info(
                        f"   📋 Re-pricing close: {close_side.upper()} "
                        f"@ ${new_price:,.1f} (01 BBO: ${o1_bid:,.1f}/${o1_ask:,.1f}) "
                        f"[{elapsed:.0f}s elapsed] (Size: {remaining_size:.5f})"
                    )
                    current_close_price = new_price

                    # Cancel old close order
                    if self.close_order_id:
                        try:
                            self.o1.cancel_order(self.close_order_id)
                        except Exception: pass
                    
                    # Place new for REMAINING size
                    self.close_order_id = self.o1.place_limit_order(
                        close_side, current_close_price, remaining_size, post_only=True
                    )
                
                last_reprice_time = now

            await asyncio.sleep(config.POLL_INTERVAL_S)
        
        return False

    # ─── Phase 5: UNWINDING ──────────────────────────────────────────────────

    async def _phase_unwinding(self):
        """Close the Lighter hedge via taker market order."""
        self.state = self.UNWINDING
        
        # IMPORTANT: Partial close fills may have ALREADY unwound Lighter.
        # The Lighter API can be slow to reflect recent trades.
        # Wait a few seconds to let the API catch up before checking.
        logger.info("⏳ Waiting 5s for Lighter API to settle before unwind check...")
        await asyncio.sleep(5)
        
        # Get EXACT position on Lighter to unwind (handles any rounding diffs)
        lighter_pos = await self.lighter.get_position()
        
        # Double-check: wait a bit more and re-read to confirm it's not stale
        if abs(lighter_pos) > 0.000005:
            await asyncio.sleep(3)
            lighter_pos_2 = await self.lighter.get_position()
            
            # If the two reads differ significantly, the API is still updating.
            # Use the SMALLER value (more recent) to avoid over-trading.
            if abs(lighter_pos_2) < abs(lighter_pos):
                logger.info(f"📉 Position updated during wait: {lighter_pos:.5f} → {lighter_pos_2:.5f}")
                lighter_pos = lighter_pos_2
        
        # If flat or tiny dust, nothing to do
        # Threshold lowered to 0.000005 (500 sats) to catch ~$0.35 scraps
        if abs(lighter_pos) < 0.000005: 
            logger.info("✅ Lighter position already flat/dust. Skipping unwind.")
            return

        # Determine unwind side based on ACTUAL position
        # If position is positive (Long), we need to SELL
        # If position is negative (Short), we need to BUY
        unwind_side = "sell" if lighter_pos > 0 else "buy"
        unwind_size = abs(lighter_pos)

        # Deterministic ID for this cycle's unwind to prevent double-spend
        # Use timestamp to ensure uniqueness across bot restarts (prevents DUPLICATE error)
        unwind_id = int(time.time() * 1000)
        
        logger.info(f"🔓 UNWINDING | {unwind_side.upper()} {unwind_size:.5f} BTC on Lighter (ID: {unwind_id})")

        response, _ = await self.lighter.place_taker_order(
            side=unwind_side,
            size=unwind_size,
            slippage_bps=config.HEDGE_SLIPPAGE_BPS,
            custom_id=unwind_id
        )

        if response:
            logger.info("✅ Unwind order sent. Verifying (polling)...")
            
            # Poll for update (max 20s) to avoid race condition/stale data
            start_poll = time.time()
            final_pos = lighter_pos # assume unchanged initially
            
            while time.time() - start_poll < 20:
                await asyncio.sleep(2)
                final_pos = await self.lighter.get_position()
                
                # If flat, we are good!
                if abs(final_pos) < 0.000005:
                    logger.info("✅ Verification successful! Lighter is flat.")
                    break
                
                # CRITICAL: If position FLIPPED sign (e.g. Short -> Long), we definitely over-traded or someone else traded.
                # Stop immediately to prevent further damage.
                if (lighter_pos > 0 and final_pos < -0.000005) or (lighter_pos < 0 and final_pos > 0.000005):
                    logger.warning(f"🚨 Position FLIPPED sign during unwind! ({lighter_pos} -> {final_pos}). Stopping verify.")
                    break

            if abs(final_pos) > 0.000005:
                # Still not flat after 20s?
                
                # Double check if we flipped sign before retrying
                if (lighter_pos > 0 and final_pos < -0.000005) or (lighter_pos < 0 and final_pos > 0.000005):
                     msg = f"⚠️ Unwind resulted in OPPOSITE position ({final_pos} BTC). Do not retry."
                     logger.error(msg)
                     await self._send_alert(msg)
                else:
                    msg = f"⚠️ Unwind verification FAILED! Remaining: {final_pos} BTC. Retrying ONCE..."
                    logger.warning(msg)
                    await self._send_alert(msg)
                    
                    # One careful retry with NEW ID (since previous might have failed)
                    retry_id = int(time.time() * 1000) + 7
                    retry_side = "sell" if final_pos > 0 else "buy"
                    await self.lighter.place_taker_order(
                        side=retry_side, 
                        size=abs(final_pos), 
                        slippage_bps=config.HEDGE_SLIPPAGE_BPS,
                        custom_id=retry_id
                    )
                    await asyncio.sleep(5) 
                    
                    # Check one last time
                    final_pos_2 = await self.lighter.get_position()
                    if abs(final_pos_2) > 0.000005:
                         crit = f"🚨 CRITICAL: Lighter unwind blocked! Stuck with {final_pos_2} BTC."
                         logger.error(crit)
                         await self._send_alert(crit)

        else:
            msg = "⚠️ Unwind failed on Lighter! Check balances."
            logger.error(msg)
            await self._send_alert(msg)

        # Log cycle summary
        logger.info(
            f"\n{'─' * 40}\n"
            f"  Cycle {self.cycle_count} Complete\n"
            f"  Opened: {self.open_side} {self.open_size:.5f} BTC @ ${self.open_price:,.1f}\n"
            f"  Volume generated: ${self.open_size * self.open_price * 2:,.2f}\n"
            f"{'─' * 40}"
        )
        
        self.last_cycle_stats = {
            "side": self.open_side,
            "size": self.open_size,
            "price": self.open_price,
            "volume_usd": self.open_size * self.open_price * 2
        }

        # Telegram Notification
        await self._send_alert(
            f"✅ **Cycle {self.cycle_count} Limit Complete**\n"
            f"Side: {self.open_side.upper()}\n"
            f"Size: {self.open_size} BTC\n"
            f"Vol: ${self.last_cycle_stats['volume_usd']:,.2f}\n"
            f"Status: Cooling down..."
        )

    # ─── Phase 6: COOLDOWN ───────────────────────────────────────────────────

    async def _phase_cooldown(self):
        """Wait randomly between cycles."""
        self.state = self.COOLDOWN
        
        # Randomize cooldown duration
        cd_min, cd_max = config.COOLDOWN_MINUTES_RANGE
        cooldown_mins = random.uniform(cd_min, cd_max)
        self.cooldown_duration_s = cooldown_mins * 60
        self.cooldown_start_time = time.time()
        
        logger.info(f"💤 Cooling down for {cooldown_mins:.1f} minutes...")

        elapsed = 0
        while elapsed < self.cooldown_duration_s and self._running:
            await asyncio.sleep(min(10, self.cooldown_duration_s - elapsed))
            elapsed = time.time() - self.cooldown_start_time

        self.state = self.IDLE

    # ─── Helpers ─────────────────────────────────────────────────────────────

    def _cancel_open_orders(self):
        """Cancel any active opening orders."""
        for oid in [self.bid_order_id, self.ask_order_id]:
            if oid is not None:
                try:
                    self.o1.cancel_order(oid)
                except Exception as e:
                    logger.debug(f"Cancel failed (may already be filled): {e}")
        self.bid_order_id = None
        self.ask_order_id = None

    async def _emergency_close(self):
        """Force-close all positions if something goes wrong."""
        logger.warning("🚨 EMERGENCY CLOSE — forcing all positions flat")

        try:
            # Cancel any open orders on 01
            self._cancel_open_orders()
            if self.close_order_id:
                try:
                    self.o1.cancel_order(self.close_order_id)
                except Exception:
                    pass
                self.close_order_id = None

            # Check 01 position and close if needed
            pos = self.o1.get_position()
            if abs(pos) > 0.00001:
                close_side = "ask" if pos > 0 else "bid"
                # Price at BBO (not 1% through) — we want to close but not hemorrhage
                o1_bbo = self.o1.get_best_bid_ask()
                o1_bid, o1_ask = o1_bbo[0], o1_bbo[1]
                if o1_bid > 0 and o1_ask > 0:
                    if close_side == "ask":
                        price = round(o1_bid, 1)  # Sell at bid (will likely taker but at fair price)
                    else:
                        price = round(o1_ask, 1)  # Buy at ask
                    self.o1.place_limit_order(close_side, price, abs(pos))  # No post_only — emergency
                    logger.warning(f"Emergency 01 close: {close_side} {abs(pos):.5f} @ ${price:,.1f}")
                else:
                    logger.error("Cannot fetch 01 BBO for emergency close — MANUAL INTERVENTION NEEDED")
                    await self._send_alert(
                        f"🚨 EMERGENCY: Cannot close 01 position ({pos:.5f} BTC). "
                        f"Close manually at 01.xyz!"
                    )

            # Check Lighter position and close if needed
            lighter_pos = await self.lighter.get_position()
            if abs(lighter_pos) > 0.00001:
                unwind_side = "sell" if lighter_pos > 0 else "buy"
                await self.lighter.place_taker_order(
                    side=unwind_side,
                    size=abs(lighter_pos),
                    slippage_bps=50,  # Wide slippage for emergency
                )
                logger.warning(f"Emergency Lighter close: {unwind_side} {abs(lighter_pos):.5f}")

        except Exception as e:
            logger.error(f"Emergency close error: {e}", exc_info=True)
            await self._send_alert(f"🚨 EMERGENCY CLOSE FAILED: {e}. Manual intervention needed!")

    async def _shutdown(self):
        """Graceful shutdown."""
        logger.info("─── Shutting down ───")
        self._running = False

        # Cancel all orders
        self._cancel_open_orders()
        if self.close_order_id:
            try:
                self.o1.cancel_order(self.close_order_id)
            except Exception:
                pass

        # Warn about any open positions
        try:
            pos_01 = self.o1.get_position()
            if abs(pos_01) > 0.00001:
                msg = (
                    f"⚠️ SHUTDOWN WARNING: Open 01 position: {pos_01:.5f} BTC. "
                    f"Close manually or run emergency_hedge.py!"
                )
                logger.warning(msg)
                await self._send_alert(msg)
        except Exception:
            pass

        # Close Lighter connection
        try:
            if hasattr(self, 'lighter'):
                 await self.lighter.close()
        except Exception:
            pass
        
        # Suppress final SSL errors on exit
        await asyncio.sleep(0.1)

        logger.info(f"─── Shutdown complete ({self.cycle_count} cycles run) ───")
