"""
Lighter DEX Client — Taker Hedge Orders
Uses lighter-sdk (pip install lighter-sdk, imports as 'lighter').
"""

import logging
import random
from decimal import Decimal

import config

logger = logging.getLogger(__name__)


class LighterClient:
    """Client for Lighter DEX using official Python SDK (lighter-sdk)."""

    def __init__(self):
        self.api_url = config.LIGHTER_API_URL
        self.api_key_private_key = config.LIGHTER_API_KEY_PRIVATE_KEY
        self.api_key_index = config.LIGHTER_API_KEY_INDEX
        self.account_index = config.LIGHTER_ACCOUNT_INDEX
        self.signer = None       # SignerClient for orders
        self.api_client = None   # ApiClient for data
        self.order_api = None    # OrderApi for orderbook

    async def initialize(self):
        """Initialize Lighter API and Signer clients."""
        logger.info(f"Initializing Lighter client at {self.api_url}...")

        try:
            import lighter

            # Data client (async — must be created in event loop)
            api_config = lighter.Configuration(host=self.api_url)
            self.api_client = lighter.ApiClient(configuration=api_config)
            self.order_api = lighter.OrderApi(self.api_client)

            # Signing client (for placing orders)
            if self.api_key_private_key:
                pk = self.api_key_private_key
                if pk.startswith("0x"):
                    pk = pk[2:]

                self.signer = lighter.SignerClient(
                    url=self.api_url,
                    account_index=self.account_index,
                    api_private_keys={self.api_key_index: pk},
                )
                logger.info("Lighter SignerClient initialized.")
            else:
                logger.warning("No Lighter private key — trading disabled.")

            # Quick connectivity check
            bbo = await self.get_best_bid_ask()
            logger.info(f"Lighter ready. BTC BBO: ${bbo[0]:,.1f} / ${bbo[1]:,.1f}")
            return True

        except ImportError:
            logger.error("Lighter SDK not found. Run: pip install lighter-sdk")
            raise
        except Exception as e:
            logger.error(f"Lighter init failed: {e}")
            raise

    async def close(self):
        """Cleanup resources."""
        if self.api_client:
            try:
                await self.api_client.close()
            except Exception:
                pass
        if self.signer:
            try:
                await self.signer.close()
            except Exception:
                pass

    async def get_best_bid_ask(self) -> tuple[float, float]:
        """Return (best_bid, best_ask) from Lighter orderbook."""
        if not self.order_api:
            return (0.0, 0.0)

        try:
            ob = await self.order_api.order_book_orders(
                market_id=config.LIGHTER_MARKET_ID, limit=5
            )

            best_bid = float(ob.bids[0].price) if ob.bids else 0.0
            best_ask = float(ob.asks[0].price) if ob.asks else 0.0
            return (best_bid, best_ask)

        except Exception as e:
            logger.error(f"Lighter orderbook error: {e}")
            return (0.0, 0.0)

    async def place_taker_order(
        self, side: str, size: float, slippage_bps: int = 10, custom_id: int | None = None
    ) -> tuple:
        """
        Place a market taker order for hedging.
        side: "buy" or "sell"
        size: BTC amount
        custom_id: Optional client_order_index for idempotency
        Returns: (response, execution_price) or (None, 0)
        """
        if config.DRY_RUN:
            logger.info(f"[DRY RUN] Lighter taker: {side} {size:.6f} BTC")
            return ("dry_run", 0.0)

        if not self.signer:
            logger.error("Lighter SignerClient not initialized")
            return (None, 0.0)

        try:
            is_ask = side.upper() == "SELL"

            # Get current market price for slippage calc
            best_bid, best_ask = await self.get_best_bid_ask()
            if best_bid == 0 or best_ask == 0:
                logger.error("Cannot fetch Lighter market price — aborting hedge")
                return (None, 0.0)

            # base_amount: integer in smallest units (5 decimal places for BTC)
            size_dec = Decimal(str(size))
            base_amount = int(size_dec * Decimal("100000"))

            # avg_execution_price: integer with 1 decimal removed
            # e.g. $68856.1 → int("688561") = 688561
            buffer_mult = slippage_bps / 10000.0
            if is_ask:
                market_price = best_bid
                target_price = best_bid * (1.0 - buffer_mult)
            else:
                market_price = best_ask
                target_price = best_ask * (1.0 + buffer_mult)

            price_str = f"{target_price:.1f}"
            avg_execution_price = int(price_str.replace(".", ""))

            position_value = size * market_price
            
            # Use custom ID if provided, else random
            client_oid = custom_id if custom_id else random.randint(1_000_000, 999_999_999)
            
            logger.info(
                f"=== HEDGE ORDER (ID: {client_oid}) ===\n"
                f"  Side: {side} | Size: {size:.6f} BTC (${position_value:.2f})\n"
                f"  Market: ${market_price:,.2f} | Limit: ${target_price:,.2f} ({slippage_bps}bps)\n"
                f"  Raw: base_amount={base_amount}, avg_price={avg_execution_price}"
            )

            order, response, error = await self.signer.create_market_order(
                market_index=config.LIGHTER_MARKET_ID,
                client_order_index=client_oid,
                base_amount=base_amount,
                avg_execution_price=avg_execution_price,
                is_ask=is_ask,
            )

            if error:
                logger.error(f"Lighter order error: {error}")
                return (None, 0.0)

            tx_hash = getattr(response, "tx_hash", "unknown")
            logger.info(f"  ✅ Hedge sent! TX: {str(tx_hash)[:16]}...")
            logger.info(f"===================")
            return (response, market_price)

        except Exception as e:
            logger.error(f"Lighter order failed: {e}", exc_info=True)
            return (None, 0.0)

    async def get_position(self) -> float:
        """
        Fetch current BTC position on Lighter.
        Returns: positive = long, negative = short, 0 = flat.
        """
        try:
            import lighter

            api_config = lighter.Configuration(host=self.api_url)
            api_client = lighter.ApiClient(configuration=api_config)
            account_api = lighter.AccountApi(api_client)

            account = await account_api.account(
                by="index", value=str(self.account_index)
            )

            if hasattr(account, "accounts") and account.accounts:
                acct = account.accounts[0]
                if hasattr(acct, "positions") and acct.positions:
                    for pos in acct.positions:
                        mid = getattr(pos, "market_id", None)
                        if mid == config.LIGHTER_MARKET_ID:
                            raw_size = float(getattr(pos, "position", 0) or 0)
                            sign = int(getattr(pos, "sign", 0))
                            
                            # If sign is 0, assume 1? Or warn?
                            # Debug output showed sign=1 for empty.
                            # Assume standard -1/1.
                            if sign == 0: 
                                # Fallback if sign is missing/zero, though unlikely based on debug
                                sign = 1 
                            
                            final_size = raw_size * sign
                            
                            logger.info(f"Fetched Lighter Position: {final_size} (Raw: {raw_size}, Sign: {sign})")

                            await api_client.close()
                            return final_size

            await api_client.close()
            return 0.0

        except Exception as e:
            logger.error(f"Lighter position fetch error: {e}")
            return 0.0

    async def get_balance(self) -> dict:
        """
        Fetch account balance/collateral info from Lighter.
        Returns dict with collateral and equity.
        """
        try:
            import lighter

            api_config = lighter.Configuration(host=self.api_url)
            api_client = lighter.ApiClient(configuration=api_config)
            account_api = lighter.AccountApi(api_client)

            account = await account_api.account(
                by="index", value=str(self.account_index)
            )

            result = {"collateral": 0.0, "equity": 0.0, "free_collateral": 0.0}

            if hasattr(account, "accounts") and account.accounts:
                acct = account.accounts[0]
                # Try common field names from Lighter API
                result["collateral"] = float(getattr(acct, "collateral", 0) or 0)
                result["equity"] = float(getattr(acct, "equity", result["collateral"]) or 0)
                result["free_collateral"] = float(
                    getattr(acct, "free_collateral",
                    getattr(acct, "freeCollateral", result["collateral"])) or 0
                )

            await api_client.close()
            return result

        except Exception as e:
            logger.error(f"Lighter balance fetch error: {e}")
            return {"collateral": 0.0, "equity": 0.0, "free_collateral": 0.0}
