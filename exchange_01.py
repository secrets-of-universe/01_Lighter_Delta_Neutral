"""
01 Exchange Python Client
Session management, order placement, and market data via protobuf + HTTP.
Based on: https://docs.01.xyz/examples/python
"""

import json
import time
import logging
import binascii
import requests
from google.protobuf.internal import encoder, decoder
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from base58 import b58decode, b58encode

import schema_pb2
import config

logger = logging.getLogger(__name__)


# ─── Signing ─────────────────────────────────────────────────────────────────────

def user_sign(key: Ed25519PrivateKey, msg: bytes) -> bytes:
    """Sign hex-encoded message. Used for CreateSession."""
    return key.sign(binascii.hexlify(msg))


def session_sign(key: Ed25519PrivateKey, msg: bytes) -> bytes:
    """Sign raw bytes. Used for PlaceOrder, CancelOrder, etc."""
    return key.sign(msg)


# ─── Protobuf Helpers ────────────────────────────────────────────────────────────

def get_varint_bytes(value: int) -> bytes:
    return encoder._VarintBytes(value)


def read_varint(buffer: bytes, offset: int = 0):
    return decoder._DecodeVarint32(buffer, offset)


# ─── Client ──────────────────────────────────────────────────────────────────────

class Exchange01Client:
    """Client for 01 Exchange using official protobuf API."""

    def __init__(self):
        self.api_url = config.O1_API_URL
        self.user_signkey: Ed25519PrivateKey | None = None
        self.session_signkey: Ed25519PrivateKey | None = None
        self.session_id: int | None = None
        self.session_expiry: int = 0
        self.market_info: dict | None = None
        self._nonce = 0
        self.account_id: int | None = None

    def initialize(self):
        """Load private key and create session."""
        logger.info("Initializing 01 Exchange client...")

        # Load Solana private key from Base58 (Phantom export)
        key_bytes = b58decode(config.O1_PRIVATE_KEY)
        # Phantom exports 64 bytes (private + public). Ed25519 needs first 32.
        self.user_signkey = Ed25519PrivateKey.from_private_bytes(key_bytes[:32])
        user_pubkey = self.user_signkey.public_key().public_bytes_raw()
        logger.info(f"User pubkey: {b58encode(user_pubkey).decode()}")

        # Fetch market info
        self.market_info = self._fetch_market_info()
        market = self.market_info[config.O1_MARKET_ID]
        logger.info(
            f"Market {market['symbol']}: "
            f"price_dec={market['price_decimals']}, "
            f"size_dec={market['size_decimals']}"
        )

        # Look up account ID
        user_pubkey_str = b58encode(user_pubkey).decode()
        resp = requests.get(f"{self.api_url}/user/{user_pubkey_str}")
        resp.raise_for_status()
        account_ids = resp.json().get("accountIds", [])
        if account_ids:
            self.account_id = account_ids[0]
            logger.info(f"Account ID: {self.account_id}")
        else:
            logger.warning("No account found on 01 Exchange!")

        # Create session
        self._create_session()

    def _get_server_timestamp(self) -> int:
        resp = requests.get(f"{self.api_url}/timestamp")
        resp.raise_for_status()
        return int(resp.json())

    def _execute_action(self, action, signing_key, sign_func) -> "schema_pb2.Receipt":
        """Serialize, sign, and send an Action. Returns parsed Receipt."""
        payload = action.SerializeToString()
        length_prefix = get_varint_bytes(len(payload))
        message = length_prefix + payload
        signature = sign_func(signing_key, message)

        resp = requests.post(
            f"{self.api_url}/action",
            data=message + signature,
            headers={"Content-Type": "application/octet-stream"},
        )

        if resp.status_code != 200:
            raise Exception(f"API error {resp.status_code}: {resp.text}")

        msg_len, pos = read_varint(resp.content, 0)
        receipt = schema_pb2.Receipt()
        receipt.ParseFromString(resp.content[pos : pos + msg_len])

        if receipt.HasField("err"):
            error_name = schema_pb2.Error.Name(receipt.err)
            raise Exception(f"Action failed: {error_name}")

        return receipt

    def _create_session(self):
        """Create a trading session (expires in SESSION_DURATION_S)."""
        server_time = self._get_server_timestamp()

        self.session_signkey = Ed25519PrivateKey.generate()
        session_pubkey = self.session_signkey.public_key().public_bytes_raw()

        action = schema_pb2.Action()
        action.current_timestamp = server_time
        action.nonce = 0
        action.create_session.user_pubkey = (
            self.user_signkey.public_key().public_bytes_raw()
        )
        action.create_session.session_pubkey = session_pubkey
        action.create_session.expiry_timestamp = (
            server_time + config.SESSION_DURATION_S
        )

        receipt = self._execute_action(action, self.user_signkey, user_sign)
        self.session_id = receipt.create_session_result.session_id
        self.session_expiry = server_time + config.SESSION_DURATION_S
        self._nonce = 0

        logger.info(f"Session created (ID: {self.session_id})")

    def _ensure_session(self):
        """Refresh session if expired or about to expire (60s buffer)."""
        now = self._get_server_timestamp()
        if now >= self.session_expiry - 60:
            logger.info("Session expiring, refreshing...")
            self._create_session()

    def _fetch_market_info(self) -> dict:
        """Fetch all market metadata from /info."""
        resp = requests.get(f"{self.api_url}/info")
        resp.raise_for_status()
        info = resp.json()

        markets = {}
        for m in info["markets"]:
            markets[m["marketId"]] = {
                "symbol": m["symbol"],
                "price_decimals": m["priceDecimals"],
                "size_decimals": m["sizeDecimals"],
            }
        return markets

    def get_price_size_decimals(self) -> tuple[int, int]:
        """Return (price_decimals, size_decimals) for configured market."""
        m = self.market_info[config.O1_MARKET_ID]
        return m["price_decimals"], m["size_decimals"]

    # ─── Order Management ────────────────────────────────────────────────────

    def place_limit_order(
        self, side: str, price: float, size: float, post_only: bool = False
    ) -> int | None:
        """
        Place a limit order.
        side: "bid" or "ask"
        post_only: if True, order fails if it crosses the book (MAKER guarantee)
        Returns: order_id if posted, None if immediately filled.
        """
        if config.DRY_RUN:
            logger.info(f"[DRY RUN] Place {side} {size:.6f} @ ${price:.1f} (PostOnly={post_only})")
            return None

        self._ensure_session()
        m = self.market_info[config.O1_MARKET_ID]
        price_raw = int(price * (10 ** m["price_decimals"]))
        size_raw = int(size * (10 ** m["size_decimals"]))

        server_time = self._get_server_timestamp()
        action = schema_pb2.Action()
        action.current_timestamp = server_time
        action.place_order.session_id = self.session_id
        action.place_order.market_id = config.O1_MARKET_ID
        action.place_order.side = (
            schema_pb2.Side.BID if side == "bid" else schema_pb2.Side.ASK
        )
        action.place_order.fill_mode = (
            schema_pb2.FillMode.POST_ONLY if post_only else schema_pb2.FillMode.LIMIT
        )
        action.place_order.price = price_raw
        action.place_order.size = size_raw

        receipt = self._execute_action(action, self.session_signkey, session_sign)
        result = receipt.place_order_result

        if result.HasField("posted"):
            order_id = result.posted.order_id
            logger.info(f"Order posted: {side} {size:.6f} @ ${price:.1f} (ID: {order_id})")
            return order_id

        if result.fills:
            logger.info(f"Order immediately filled ({len(result.fills)} fills)")
            return None

        return None

    def cancel_order(self, order_id: int):
        """Cancel an order by ID."""
        if config.DRY_RUN:
            logger.info(f"[DRY RUN] Cancel order {order_id}")
            return

        self._ensure_session()
        server_time = self._get_server_timestamp()

        action = schema_pb2.Action()
        action.current_timestamp = server_time
        action.cancel_order_by_id.session_id = self.session_id
        action.cancel_order_by_id.order_id = order_id

        self._execute_action(action, self.session_signkey, session_sign)
        logger.debug(f"Cancelled order {order_id}")

    def atomic_cancel_and_place(
        self, cancel_ids: list[int], new_orders: list[dict]
    ) -> list[int]:
        """
        Atomically cancel old orders and place new ones (max 4 ops total).
        new_orders: list of {"side": "bid"|"ask", "price": float, "size": float}
        Returns: list of new order IDs.
        """
        if config.DRY_RUN:
            for o in new_orders:
                logger.info(f"[DRY RUN] Atomic: {o['side']} {o['size']:.6f} @ ${o['price']:.1f}")
            return []

        self._ensure_session()
        m = self.market_info[config.O1_MARKET_ID]
        server_time = self._get_server_timestamp()

        action = schema_pb2.Action()
        action.current_timestamp = server_time
        action.atomic.session_id = self.session_id

        # Cancels first (required ordering for same market)
        for oid in cancel_ids:
            sub = action.atomic.actions.add()
            sub.cancel_order.order_id = oid

        # Then place new orders
        for order in new_orders:
            price_raw = int(order["price"] * (10 ** m["price_decimals"]))
            size_raw = int(order["size"] * (10 ** m["size_decimals"]))
            is_post_only = order.get("post_only", False)

            sub = action.atomic.actions.add()
            sub.trade_or_place.market_id = config.O1_MARKET_ID
            sub.trade_or_place.order_type.side = (
                schema_pb2.Side.BID if order["side"] == "bid" else schema_pb2.Side.ASK
            )
            sub.trade_or_place.order_type.fill_mode = (
                schema_pb2.FillMode.POST_ONLY if is_post_only else schema_pb2.FillMode.LIMIT
            )
            sub.trade_or_place.order_type.is_reduce_only = False
            sub.trade_or_place.limit.price = price_raw
            sub.trade_or_place.limit.size = size_raw

        receipt = self._execute_action(action, self.session_signkey, session_sign)

        # Extract new order IDs from atomic result
        new_ids = []
        if hasattr(receipt, "atomic_result") and receipt.atomic_result.results:
            for r in receipt.atomic_result.results:
                if hasattr(r, "trade_or_place_result"):
                    tor = r.trade_or_place_result
                    if tor.HasField("posted"):
                        new_ids.append(tor.posted.order_id)
                    elif tor.fills:
                        new_ids.append(None)  # Filled immediately

        sides_str = ", ".join(f"{o['side']}@${o['price']:.1f}" for o in new_orders)
        logger.info(
            f"Atomic: cancelled {len(cancel_ids)}, placed {len(new_orders)} "
            f"({sides_str}) → IDs: {new_ids}"
        )
        return new_ids

    # ─── Market Data ─────────────────────────────────────────────────────────

    def get_orderbook(self) -> dict:
        """Fetch orderbook for configured market."""
        resp = requests.get(
            f"{self.api_url}/market/{config.O1_MARKET_ID}/orderbook",
        )
        resp.raise_for_status()
        return resp.json()

    def get_best_bid_ask(self) -> tuple[float, float]:
        """Return (best_bid, best_ask) from 01 orderbook."""
        book = self.get_orderbook()
        best_bid = float(book["bids"][0][0]) if book["bids"] else 0.0
        best_ask = float(book["asks"][0][0]) if book["asks"] else 0.0
        return best_bid, best_ask

    def get_balance(self) -> dict:
        """
        Fetch account balance/collateral info via GET /account/{id}.
        Returns dict with collateral, free_collateral, equity.
        """
        if self.account_id is None:
            return {"collateral": 0.0, "free_collateral": 0.0, "equity": 0.0}

        try:
            resp = requests.get(f"{self.api_url}/account/{self.account_id}")
            resp.raise_for_status()
            data = resp.json()

            # Updated parsing: equity from Balances, free from Margins
            balances = data.get("balances", [])
            equity = sum(float(b.get("amount", 0)) for b in balances)
            
            margins = data.get("margins", {})
            free_collateral = float(margins.get("mf", equity))
            collateral = equity # Collateral is effectively equity

            return {
                "collateral": collateral,
                "free_collateral": free_collateral,
                "equity": equity,
            }
        except Exception as e:
            logger.error(f"Balance fetch error: {e}")
            return {"collateral": 0.0, "free_collateral": 0.0, "equity": 0.0}

    def get_position(self) -> float:
        """
        Fetch current BTC position via GET /account/{id}.
        Returns: positive = long, negative = short, 0 = flat.
        """
        if self.account_id is None:
            return 0.0

        try:
            resp = requests.get(f"{self.api_url}/account/{self.account_id}")
            resp.raise_for_status()
            data = resp.json()

            for pos in data.get("positions", []):
                if pos.get("marketId") == config.O1_MARKET_ID:
                    # check for nested perp object
                    if "perp" in pos:
                        p = pos["perp"]
                        size = float(p.get("baseSize", 0))
                        is_long = p.get("isLong", True)
                        return size if is_long else -size
                    else:
                        # spot or flat structure
                        size = float(pos.get("baseSize", 0))
                        is_long = pos.get("isLong", True)
                        return size if is_long else -size

            return 0.0
        except Exception as e:
            logger.error(f"Position fetch error: {e}")
            return 0.0

    def get_market_stats(self) -> dict:
        """Fetch market stats (mark price, funding, etc.)."""
        resp = requests.get(f"{self.api_url}/market/{config.O1_MARKET_ID}/stats")
        resp.raise_for_status()
        return resp.json()
