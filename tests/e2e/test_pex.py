"""PEX binary verification tests.

These tests require the PEX to be built first via `make build-pex`.
Skip if PEX doesn't exist.
"""
import os
import subprocess
import sys

import pytest

PEX_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "dist", "csvconv.pex")


@pytest.fixture
def pex_binary():
    """Skip if PEX binary not built."""
    pex_abs = os.path.abspath(PEX_PATH)
    if not os.path.exists(pex_abs):
        pytest.skip("PEX binary not built. Run 'make build-pex' first.")
    return pex_abs


def test_pex_binary_shows_help(pex_binary):
    result = subprocess.run(
        [sys.executable, pex_binary, "--help"],
        capture_output=True, text=True
    )
    assert result.returncode == 0
    assert "--input" in result.stdout


def test_pex_binary_shows_version(pex_binary):
    result = subprocess.run(
        [sys.executable, pex_binary, "--version"],
        capture_output=True, text=True
    )
    assert result.returncode == 0
