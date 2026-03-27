"""
Call a paid Core API URL under /mpp/snapshot/... using pympp's Client (402 → pay → retry).

Configure signing via TempoAccount.from_env() — see https://mpp.dev/sdk/python/
"""
from __future__ import annotations

import argparse
import asyncio

from mpp.client import Client
from mpp.methods.tempo import ChargeIntent, TempoAccount, tempo


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
