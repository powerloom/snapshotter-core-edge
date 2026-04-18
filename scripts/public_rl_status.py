#!/usr/bin/env python3
"""
Inspect fixed-window counters for public rate limiting on the **main** snapshotter Redis
(where LIMITER/* keys are stored).

Uses SCAN (not KEYS). Pattern: LIMITER/<PUBLIC_RATE_LIMIT_KEY_PREFIX>*

Examples:
  REDIS_HOST=redis poetry run python scripts/public_rl_status.py
  poetry run python scripts/public_rl_status.py --max 50
"""
from __future__ import annotations

import argparse
import os
import sys


def main() -> int:
    import redis

    parser = argparse.ArgumentParser(description="List public rate limit Redis counter keys")
    parser.add_argument(
        "--max",
        type=int,
        default=200,
        help="Max keys to print (default 200)",
    )
    args = parser.parse_args()

    prefix = os.getenv("PUBLIC_RATE_LIMIT_KEY_PREFIX", "rl:public:").rstrip("/")
    pattern = f"LIMITER/{prefix}*"

    r = redis.Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6379")),
        db=int(os.getenv("REDIS_DB", "0")),
        password=os.getenv("REDIS_PASSWORD") or None,
        decode_responses=True,
    )
    keys: list[str] = []
    try:
        cur: int = 0
        while True:
            cur, batch = r.scan(cursor=cur, match=pattern, count=100)
            for k in batch:
                keys.append(k if isinstance(k, str) else k.decode())
            if cur == 0:
                break
        keys.sort()
        total = len(keys)
        if total > args.max:
            print(f"pattern={pattern} total_matched={total} (showing first {args.max})")
            keys = keys[: args.max]
        else:
            print(f"pattern={pattern} total={total}")

        for k in keys:
            val = r.get(k)
            ttl = r.ttl(k)
            print(f"  ttl={ttl}s count={val} {k}")
    finally:
        r.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
