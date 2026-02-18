"""
Config Manager — Dynamic settings via Telegram.

Manages a whitelist of editable settings that can be changed at runtime
without restarting the bot. Changes are persisted to config_overrides.json.
"""

import json
import logging
import os

import config

logger = logging.getLogger(__name__)

# Path to persist overrides (same directory as config.py)
OVERRIDES_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config_overrides.json")

# Whitelist of settings that can be changed via Telegram.
# Maps setting name -> (type, description, parser)
# parser converts the user's string input to the correct Python type.
EDITABLE_SETTINGS = {
    "ORDER_SIZE_RANGE_USD": {
        "type": "range",
        "desc": "Order size range in USD (min max)",
        "example": "/set ORDER_SIZE_RANGE_USD 800 1200",
        "parse": lambda parts: (float(parts[0]), float(parts[1])),
    },
    "HOLD_DURATION_RANGE_S": {
        "type": "range",
        "desc": "Hold duration range in MINUTES (min max)",
        "example": "/set HOLD 30 60",
        "parse": lambda parts: (float(parts[0]) * 60, float(parts[1]) * 60),
    },
    "COOLDOWN_MINUTES_RANGE": {
        "type": "range",
        "desc": "Cooldown range in minutes (min max)",
        "example": "/set COOLDOWN 10 15",
        "parse": lambda parts: (float(parts[0]), float(parts[1])),
    },
    "ORDER_TIMEOUT_S": {
        "type": "number",
        "desc": "Cancel unfilled order after N seconds",
        "example": "/set ORDER_TIMEOUT_S 300",
        "parse": lambda parts: float(parts[0]),
    },
    "CLOSE_REPRICE_S": {
        "type": "number",
        "desc": "Re-price close order every N seconds",
        "example": "/set CLOSE_REPRICE_S 30",
        "parse": lambda parts: float(parts[0]),
    },
    "CLOSE_BUFFER_USD": {
        "type": "number",
        "desc": "Close order distance from BBO (USD)",
        "example": "/set CLOSE_BUFFER_USD 15",
        "parse": lambda parts: float(parts[0]),
    },
    "SPREAD_OFFSET_BPS": {
        "type": "number",
        "desc": "Opening spread offset (basis points)",
        "example": "/set SPREAD_OFFSET_BPS 10",
        "parse": lambda parts: float(parts[0]),
    },
    "HEDGE_SLIPPAGE_BPS": {
        "type": "number",
        "desc": "Max hedge slippage (basis points)",
        "example": "/set HEDGE_SLIPPAGE_BPS 10",
        "parse": lambda parts: int(parts[0]),
    },
    "DRY_RUN": {
        "type": "bool",
        "desc": "Simulate orders without sending",
        "example": "/set DRY_RUN true",
        "parse": lambda parts: parts[0].lower() in ("true", "1", "yes"),
    },
    "LEVERAGE": {
        "type": "number",
        "desc": "Max leverage for margin calculations",
        "example": "/set LEVERAGE 40",
        "parse": lambda parts: int(parts[0]),
    },
}

# Aliases for easier typing
ALIASES = {
    "ORDER_SIZE": "ORDER_SIZE_RANGE_USD",
    "SIZE": "ORDER_SIZE_RANGE_USD",
    "HOLD": "HOLD_DURATION_RANGE_S",
    "COOLDOWN": "COOLDOWN_MINUTES_RANGE",
    "TIMEOUT": "ORDER_TIMEOUT_S",
    "CLOSE_REPRICE": "CLOSE_REPRICE_S",
    "REPRICE": "CLOSE_REPRICE_S",  # Kept as legacy or alternative
    "CLOSE_BUFFER": "CLOSE_BUFFER_USD",
    "BUFFER": "CLOSE_BUFFER_USD",   # Kept as legacy or alternative
    "SPREAD": "SPREAD_OFFSET_BPS",
    "SLIPPAGE": "HEDGE_SLIPPAGE_BPS",
}


def load_overrides():
    """
    Load saved overrides from config_overrides.json and apply to config module.
    Called once at startup.
    """
    if not os.path.exists(OVERRIDES_FILE):
        return

    try:
        with open(OVERRIDES_FILE, "r") as f:
            overrides = json.load(f)

        applied = 0
        for key, value in overrides.items():
            if key in EDITABLE_SETTINGS:
                # Convert lists back to tuples for range types
                if EDITABLE_SETTINGS[key]["type"] == "range" and isinstance(value, list):
                    value = tuple(value)
                setattr(config, key, value)
                applied += 1

        if applied:
            logger.info(f"Loaded {applied} config override(s) from {OVERRIDES_FILE}")

    except Exception as e:
        logger.error(f"Failed to load config overrides: {e}")


def update_setting(key: str, value_parts: list[str]) -> tuple[bool, str]:
    """
    Update a config setting.
    Returns (success, message).
    """
    key = key.upper()
    
    # Resolve alias
    if key in ALIASES:
        key = ALIASES[key]

    if key not in EDITABLE_SETTINGS:
        available = "\n".join(f"  `{k}`" for k in EDITABLE_SETTINGS)
        return False, f"Unknown setting: `{key}`\n\nAvailable:\n{available}"

    setting = EDITABLE_SETTINGS[key]

    # Validate input count
    if setting["type"] == "range" and len(value_parts) != 2:
        return False, f"Range requires 2 values.\nExample: `{setting['example']}`"
    elif setting["type"] != "range" and len(value_parts) != 1:
        return False, f"Expected 1 value.\nExample: `{setting['example']}`"

    try:
        new_value = setting["parse"](value_parts)
    except (ValueError, IndexError):
        return False, f"Invalid value. Example: `{setting['example']}`"

    # Apply to config module (takes effect immediately)
    old_value = getattr(config, key, None)
    setattr(config, key, new_value)

    # Persist to JSON
    _save_overrides()

    return True, f"✅ `{key}` updated:\n`{old_value}` → `{new_value}`"


def _save_overrides():
    """Save all current overrideable settings to JSON."""
    overrides = {}
    for key in EDITABLE_SETTINGS:
        val = getattr(config, key, None)
        if val is not None:
            # Convert tuples to lists for JSON
            overrides[key] = list(val) if isinstance(val, tuple) else val

    try:
        with open(OVERRIDES_FILE, "w") as f:
            json.dump(overrides, f, indent=2)
    except Exception as e:
        logger.error(f"Failed to save config overrides: {e}")


import html

def get_settings_display() -> str:
    """Format current settings for Telegram display with aliases (HTML)."""
    lines = []
    
    # Invert ALIASES to find short names for keys
    key_to_alias = {}
    for alias, real_key in ALIASES.items():
        # Prefer shorter aliases
        if real_key not in key_to_alias or len(alias) < len(key_to_alias[real_key]):
            key_to_alias[real_key] = alias

    for key, meta in EDITABLE_SETTINGS.items():
        val = getattr(config, key, "?")
        if meta["type"] == "range":
            val_str = f"{val[0]}-{val[1]}"
        else:
            val_str = str(val)
            
        # Use alias if available, else key
        display_name = key_to_alias.get(key, key)
        
        # Escape strings for HTML safety
        d_name_safe = html.escape(display_name)
        val_safe = html.escape(val_str)
        desc_safe = html.escape(meta['desc'])
        example_safe = html.escape(meta['example'].replace(key, display_name))

        lines.append(f"🔹 <b>{d_name_safe}</b>: <code>{val_safe}</code>")
        lines.append(f"   <i>{desc_safe}</i>")
        lines.append(f"   Example: <code>{example_safe}</code>\n")

    return "\n".join(lines)


def get_help_text() -> str:
    """Format help text for /set command."""
    lines = ["**Editable Settings (Aliases):**\n"]
    for key, meta in EDITABLE_SETTINGS.items():
        # Find aliases for this key
        aliases = [k for k, v in ALIASES.items() if v == key]
        alias_str = f" ({', '.join(aliases)})" if aliases else ""
        
        lines.append(f"`{key}`{alias_str} — {meta['desc']}")
        lines.append(f"  _{meta['example']}_")

    return "\n".join(lines)
