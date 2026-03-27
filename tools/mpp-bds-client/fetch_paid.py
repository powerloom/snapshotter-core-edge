"""
Call a paid Core API URL under /mpp/snapshot/... using pympp's Client (402 → pay → retry).

Configure signing via TempoAccount.from_env() — see https://mpp.dev/sdk/python/
"""
from __future__ import annotations

import argparse
import asyncio
import sys

try:
    from mpp.client import Client
    from mpp.methods.tempo import ChargeIntent, TempoAccount, tempo
    import mpp.methods.tempo.client as _mpp_tempo_client
except ModuleNotFoundError as exc:  # pympp not on this interpreter’s path
    sys.stderr.write(
        "Missing pympp. From tools/mpp-bds-client run:\n"
        "  poetry install && poetry run python fetch_paid.py ...\n"
        "or: pip install 'pympp[tempo]>=0.4.2' && python fetch_paid.py ...\n"
    )
    raise SystemExit(1) from exc

# pympp uses DEFAULT_GAS_LIMIT=100_000 and eth_estimateGas; for Tempo AA (0x76) that
# estimate often fails (swallowed), so gas stays 100k — below intrinsic gas (~272k).
# Until pympp raises the default on PyPI, match upstream main (1M floor).
_mpp_tempo_client.DEFAULT_GAS_LIMIT = 1_000_000


async def main() -> None:
    parser = argparse.ArgumentParser(description="MPP-paid GET to BDS Core API")
    parser.add_argument(
        "--base-url",
        default="http://127.0.0.1:9003",
        help="Core API origin (no trailing slash)",
    )
    parser.add_argument(
        "--path",
        default="/mpp/snapshot/allTrades",
        help="Path under base URL",
    )
    args = parser.parse_args()
    url = args.base_url.rstrip("/") + args.path

    account = TempoAccount.from_env()
    async with Client(
        methods=[tempo(account=account, intents={"charge": ChargeIntent()})]
    ) as client:
        response = await client.get(url)
        print("status", response.status_code)
        print(response.text[:2000])


if __name__ == "__main__":
    asyncio.run(main())
