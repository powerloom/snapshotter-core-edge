#!/usr/bin/env python3
"""
Inspect fixed-window counters for public rate limiting on the **main** snapshotter Redis
(where LIMITER/* keys are stored).

Uses SCAN (not KEYS). Pattern: LIMITER/<PUBLIC_RATE_LIMIT_KEY_PREFIX>*

Core API reads Redis host/port/db from **config/settings.json** (``redis`` section), not
from REDIS_* unless your deploy rewrites that file. For this script, set REDIS_* to match
that section, or use ``--use-core-settings`` from the snapshotter project root
(``config/settings.json`` present).

Counter keys are created on the first qualifying HTTP request (non-``/mpp/``, not in
``PUBLIC_RATE_LIMIT_SKIP_PATHS``). No traffic ⇒ no keys.

Examples:
  REDIS_HOST=redis poetry run python scripts/public_rl_status.py
  poetry run python scripts/public_rl_status.py --max 50
  poetry run python scripts/public_rl_status.py --use-core-settings
"""
from __future__ import annotations

import argparse
import os
import sys


def _redis_from_core_settings():
    from snapshotter.settings.config import settings

    return {
        "host": settings.redis.host,
        "port": settings.redis.port,
        "db": settings.redis.db,
        "password": settings.redis.password,
    }


def main() -> int:
    import redis

    parser = argparse.ArgumentParser(description="List public rate limit Redis counter keys")
    parser.add_argument(
        "--max",
        type=int,
        default=200,
        help="Max keys to print (default 200)",
    )
    parser.add_argument(
        "--use-core-settings",
        action="store_true",
        help="Use redis host/port/db/password from snapshotter config/settings.json (run from repo root)",
    )
    args = parser.parse_args()

    prefix = os.getenv("PUBLIC_RATE_LIMIT_KEY_PREFIX", "rl:public:").rstrip("/")
    pattern = f"LIMITER/{prefix}*"

    if args.use_core_settings:
        conf = _redis_from_core_settings()
        host, port, db, password = (
            conf["host"],
            conf["port"],
            conf["db"],
            conf["password"],
        )
        source = "config/settings.json (redis)"
    else:
        host = os.getenv("REDIS_HOST", "localhost")
        port = int(os.getenv("REDIS_PORT", "6379"))
        db = int(os.getenv("REDIS_DB", "0"))
        password = os.getenv("REDIS_PASSWORD") or None
        source = "REDIS_* environment variables"

    r = redis.Redis(
        host=host,
        port=port,
        db=db,
        password=password,
        decode_responses=True,
    )
    keys: list[str] = []
    try:
        try:
            r.ping()
        except redis.exceptions.RedisError as exc:
            print(f"error: cannot connect to redis {host}:{port} db={db} ({source}): {exc}", file=sys.stderr)
            return 1

        print(f"redis {host}:{port} db={db} ({source})")
        print(f"scan match={pattern!r}")

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
            print(f"total_matched={total} (showing first {args.max})")
            keys = keys[: args.max]
        else:
            print(f"total={total}")

        for k in keys:
            val = r.get(k)
            ttl = r.ttl(k)
            print(f"  ttl={ttl}s count={val} {k}")

        if total == 0:
            sample: list[str] = []
            cur = 0
            while True:
                cur, batch = r.scan(cursor=cur, match="LIMITER/*", count=200)
                for k in batch:
                    kk = k if isinstance(k, str) else k.decode()
                    if kk not in sample:
                        sample.append(kk)
                    if len(sample) >= 5:
                        break
                if len(sample) >= 5 or cur == 0:
                    break
            if sample:
                print(
                    "note: other LIMITER/* keys exist (different prefix than "
                    f"{prefix!r}); e.g. {sample[:3]}",
                    file=sys.stderr,
                )
            else:
                print(
                    "note: no LIMITER/* keys in this db — enable PUBLIC_RATE_LIMIT_ENABLED, "
                    "hit a non-skip route (not /health, /docs, …), and ensure this target "
                    "matches Core API redis in settings.json.",
                    file=sys.stderr,
                )
    finally:
        r.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
