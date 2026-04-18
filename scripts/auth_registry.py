#!/usr/bin/env python3
"""
Manage the auth Redis schema used by Core API public-rate ``check_user_details`` (and optional Auth HTTP).

**Who is allowed to run this?** Anyone who can write to the target Redis (same trust as ``redis-cli``).
There is no separate “owner API” credential: protect Redis (network, password, ACLs).

Optional guard: set env ``AUTH_REGISTRY_CLI_SECRET``; then every invocation must pass ``--secret <value>``.

Uses the same Redis as ``PUBLIC_RATE_LIMIT_AUTH_REDIS_*`` when ``PUBLIC_RATE_LIMIT_AUTH_REDIS_USE_MAIN`` is false,
else ``REDIS_*`` (must match Core API auth lookups).

Examples:
  poetry run python scripts/auth_registry.py create-user --email alice@example.com
  poetry run python scripts/auth_registry.py add-api-key --email alice@example.com --api-key sk_abc
  poetry run python scripts/auth_registry.py show-user --email alice@example.com
  poetry run python scripts/auth_registry.py list-users
  poetry run python scripts/auth_registry.py revoke-api-key --email alice@example.com --api-key sk_abc
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))


def _check_cli_secret(args: argparse.Namespace) -> None:
    expected = os.getenv("AUTH_REGISTRY_CLI_SECRET", "").strip()
    if not expected:
        return
    got = (args.secret or "").strip()
    if got != expected:
        print("error: AUTH_REGISTRY_CLI_SECRET is set; pass matching --secret", file=sys.stderr)
        sys.exit(2)


def _add_secret_flag(p: argparse.ArgumentParser) -> None:
    p.add_argument(
        "--secret",
        default="",
        help="Must match AUTH_REGISTRY_CLI_SECRET when that env var is set",
    )


def _redis_client():
    import redis

    use_main = os.getenv("PUBLIC_RATE_LIMIT_AUTH_REDIS_USE_MAIN", "true").lower() in (
        "1",
        "true",
        "yes",
    )
    if use_main:
        host = os.getenv("REDIS_HOST", "localhost")
        port = int(os.getenv("REDIS_PORT", "6379"))
        db = int(os.getenv("REDIS_DB", "0"))
        password = os.getenv("REDIS_PASSWORD") or None
    else:
        host = os.getenv("PUBLIC_RATE_LIMIT_AUTH_REDIS_HOST") or os.getenv(
            "REDIS_HOST",
            "localhost",
        )
        port = int(os.getenv("PUBLIC_RATE_LIMIT_AUTH_REDIS_PORT", "6379"))
        db = int(os.getenv("PUBLIC_RATE_LIMIT_AUTH_REDIS_DB", "0"))
        raw_pwd = os.getenv("PUBLIC_RATE_LIMIT_AUTH_REDIS_PASSWORD")
        password = raw_pwd if raw_pwd is not None else (os.getenv("REDIS_PASSWORD") or None)

    return redis.Redis(
        host=host,
        port=port,
        db=db,
        password=password,
        decode_responses=True,
    )


def main() -> int:
    # Import after cwd is snapshotter-core-edge (poetry run from repo root)
    from snapshotter.auth.helpers.redis_keys import all_users_set
    from snapshotter.auth.helpers.redis_keys import api_key_to_owner_key
    from snapshotter.auth.helpers.redis_keys import user_active_api_keys_set
    from snapshotter.auth.helpers.redis_keys import user_details_htable
    from snapshotter.auth.helpers.redis_keys import user_revoked_api_keys_set

    parser = argparse.ArgumentParser(description="Auth registry (Redis) for Core API API keys")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_cu = sub.add_parser("create-user", help="SADD allUsers + HSET user:{email}")
    _add_secret_flag(p_cu)
    p_cu.add_argument("--email", required=True)
    p_cu.add_argument("--rate-limit", default="60/minute", dest="rate_limit")
    p_cu.add_argument("--active", default="active", choices=("active", "inactive"))

    p_add = sub.add_parser("add-api-key", help="Register key for existing user")
    _add_secret_flag(p_add)
    p_add.add_argument("--email", required=True)
    p_add.add_argument("--api-key", required=True, dest="api_key")

    p_rev = sub.add_parser("revoke-api-key", help="Move key active -> revoked set")
    _add_secret_flag(p_rev)
    p_rev.add_argument("--email", required=True)
    p_rev.add_argument("--api-key", required=True, dest="api_key")

    p_show = sub.add_parser("show-user", help="Print user hash and key sets")
    _add_secret_flag(p_show)
    p_show.add_argument("--email", required=True)

    p_list = sub.add_parser("list-users", help="Print allUsers members")
    _add_secret_flag(p_list)

    args = parser.parse_args()
    _check_cli_secret(args)

    r = _redis_client()
    try:
        if args.cmd == "create-user":
            email = args.email
            now = int(time.time())
            r.sadd(all_users_set(), email)
            r.hset(
                user_details_htable(email),
                mapping={
                    "email": email,
                    "rate_limit": args.rate_limit,
                    "active": args.active,
                    "callsCount": "0",
                    "throttledCount": "0",
                    "next_reset_at": str(now + 86400),
                },
            )
            print(f"ok: user {email!r} upserted in {user_details_htable(email)}")
        elif args.cmd == "add-api-key":
            email = args.email
            key = args.api_key.strip()
            if not r.sismember(all_users_set(), email):
                print("error: user does not exist; run create-user first", file=sys.stderr)
                return 1
            pipe = r.pipeline(transaction=True)
            pipe.sadd(user_active_api_keys_set(email), key)
            pipe.set(api_key_to_owner_key(key), email)
            pipe.execute()
            print(f"ok: api key registered for {email!r}")
        elif args.cmd == "revoke-api-key":
            email = args.email
            key = args.api_key.strip()
            if not r.sismember(all_users_set(), email):
                print("error: user does not exist", file=sys.stderr)
                return 1
            if not r.sismember(user_active_api_keys_set(email), key):
                print("error: key not in active set", file=sys.stderr)
                return 1
            if r.sismember(user_revoked_api_keys_set(email), key):
                print("error: key already revoked", file=sys.stderr)
                return 1
            r.smove(user_active_api_keys_set(email), user_revoked_api_keys_set(email), key)
            print("ok: key revoked")
        elif args.cmd == "show-user":
            email = args.email
            h = r.hgetall(user_details_htable(email))
            if not h:
                print("error: no user hash", file=sys.stderr)
                return 1
            active = sorted(r.smembers(user_active_api_keys_set(email)))
            revoked = sorted(r.smembers(user_revoked_api_keys_set(email)))
            print("hash", user_details_htable(email), h)
            print("active_api_keys", active)
            print("revoked_api_keys", revoked)
        else:
            users = sorted(r.smembers(all_users_set()))
            for u in users:
                print(u)
    finally:
        r.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
