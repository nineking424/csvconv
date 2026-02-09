"""Logging configuration for csvconv."""

import logging
import sys


def setup_logging(log_level="INFO"):
    """Configure logging with stdout/stderr separation.

    INFO and below go to stdout.
    WARNING and above go to stderr.

    Args:
        log_level: Logging level string (INFO, WARN, WARNING, ERROR).
                   WARN is accepted as alias for WARNING.
    """
    # Normalize WARN -> WARNING
    if log_level.upper() == "WARN":
        log_level = "WARNING"

    level = getattr(logging, log_level.upper(), logging.INFO)

    # Clear existing handlers
    root = logging.getLogger("csvconv")
    root.handlers.clear()
    root.setLevel(level)

    # stdout handler for INFO and below
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.DEBUG)
    stdout_handler.addFilter(lambda record: record.levelno < logging.WARNING)
    stdout_handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))

    # stderr handler for WARNING and above
    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setLevel(logging.WARNING)
    stderr_handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))

    root.addHandler(stdout_handler)
    root.addHandler(stderr_handler)

    return root
