"""One-shot script: derive L2 CLOB credentials from wallet private key.

Run once:
    source .venv/bin/activate
    python scripts/derive_clob_key.py

Copy the output into .env as:
    POLYMARKET_API_KEY=...
    POLYMARKET_SECRET=...
    POLYMARKET_PASSPHRASE=...
"""

import os
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env")

PRIVATE_KEY = os.getenv("PRIVATE_KEY")
if not PRIVATE_KEY:
    sys.exit("PRIVATE_KEY not set in .env")

try:
    from py_clob_client.client import ClobClient
except ImportError:
    sys.exit("py-clob-client not installed — run: pip install -e '.[trading]'")

client = ClobClient(
    host="https://clob.polymarket.com",
    key=PRIVATE_KEY,
    chain_id=137,
)

print("Deriving L2 API credentials from private key…")
try:
    resp = client.derive_api_key()
except Exception as e:
    sys.exit(f"Error: {e}")

# py-clob-client returns ApiCreds or a dict depending on version
if hasattr(resp, "api_key"):
    key = resp.api_key
    secret = resp.api_secret
    passphrase = resp.api_passphrase
elif isinstance(resp, dict):
    key = resp.get("apiKey") or resp.get("api_key") or resp.get("key")
    secret = resp.get("secret") or resp.get("apiSecret")
    passphrase = resp.get("passphrase") or resp.get("apiPassphrase")
else:
    sys.exit(f"Unexpected response format: {resp!r}")

print("\nAdd these to your .env:\n")
print(f"POLYMARKET_API_KEY={key}")
print(f"POLYMARKET_SECRET={secret}")
print(f"POLYMARKET_PASSPHRASE={passphrase}")
print("\nDone. Keep these secret — treat them like password.")
