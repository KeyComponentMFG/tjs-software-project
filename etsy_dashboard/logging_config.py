"""
Centralized logging configuration for the Etsy Financial Dashboard.

Usage:
    from etsy_dashboard.logging_config import get_logger
    logger = get_logger("my_module")
    logger.info("Something happened")
"""

import logging
import sys

# ── Configure root "dashboard" logger ────────────────────────────────────────

_LOG_FORMAT = "[%(asctime)s] %(levelname)s %(name)s: %(message)s"

_root_logger = logging.getLogger("dashboard")
_root_logger.setLevel(logging.DEBUG)  # allow children to filter further

# Avoid duplicate handlers if this module is imported more than once
if not _root_logger.handlers:
    _console_handler = logging.StreamHandler(sys.stdout)
    _console_handler.setLevel(logging.INFO)
    _console_handler.setFormatter(logging.Formatter(_LOG_FORMAT))
    _root_logger.addHandler(_console_handler)


def get_logger(name: str) -> logging.Logger:
    """Return a child logger under the 'dashboard' namespace.

    Example: get_logger("callbacks") -> logger named "dashboard.callbacks"
    """
    return logging.getLogger(f"dashboard.{name}")
