"""
Public (non-MPP) HTTP rate limiting for Core API free routes.

Uses ``async_limits`` + main ``app.state.redis_conn`` for counters (same stack as
``auth/helpers/rate_limiter.py``). ``X-API-KEY`` / Bearer tokens are validated with
``check_user_details`` against auth Redis (``allUsers``, ``apikey:…:owner``,
``user:{email}``, active key set). Skips ``/mpp/...``. See
``ai-coord-docs/bds-mpp-integration/15-mpp-full-uniswap-surface.md``.
"""

from __future__ import annotations

import hashlib
from typing import Optional

from redis import asyncio as aioredis
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

from snapshotter.auth.helpers.helpers import check_user_details
from snapshotter.auth.helpers.rate_limiter import generic_rate_limiter
from snapshotter.settings.config import settings
from snapshotter.utils.default_logger import default_logger

logger = default_logger.bind(module="PublicRateLimit")

_fail_open_warned = False


def _digest(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:32]


def _forwarded_ip(request: Request) -> str:
    cf = request.headers.get("CF-Connecting-IP") or request.headers.get("cf-connecting-ip")
    if cf and cf.strip():
        return cf.strip()
    xff = request.headers.get("X-Forwarded-For")
    if xff:
        return xff.split(",")[0].strip()
    if request.client and request.client.host:
        return request.client.host
    return "unknown"


def _api_key_or_bearer(request: Request, auth_header_name: str) -> Optional[str]:
    key = request.headers.get(auth_header_name)
    if key and key.strip():
        return key.strip()
    auth = request.headers.get("Authorization") or ""
    if auth.lower().startswith("bearer "):
        tok = auth[7:].strip()
        if tok:
            return tok
    return None


def _should_skip_path(path: str, skip_entries: list[str]) -> bool:
    for raw in skip_entries:
        p = raw.strip()
        if not p:
            continue
        if path == p or path.startswith(p + "/"):
            return True
    return False


def _auth_registry_redis(request: Request) -> Optional[aioredis.Redis]:
    """Redis where API key registry lives (same DB as auth HTTP service)."""
    return getattr(request.app.state, "public_rate_limit_auth_redis_conn", None)


class PublicRateLimitMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        cfg = settings.public_rate_limit_config
        path = request.url.path

        if not cfg.enabled:
            return await call_next(request)

        if path.startswith("/mpp/"):
            return await call_next(request)

        if _should_skip_path(path, cfg.skip_paths_list):
            return await call_next(request)

        redis_conn = getattr(request.app.state, "redis_conn", None)
        script_shas = getattr(request.app.state, "public_rate_limit_script_shas", None)
        pub_limits = getattr(request.app.state, "public_rate_limit_limits_public", None)
        auth_limits = getattr(request.app.state, "public_rate_limit_limits_auth", None)

        if (
            redis_conn is None
            or script_shas is None
            or not pub_limits
            or not auth_limits
        ):
            global _fail_open_warned
            if not _fail_open_warned:
                _fail_open_warned = True
                logger.warning(
                    "public rate limit fail-open: redis_conn={} script_shas={} "
                    "public_limits={} auth_limits={} (check startup logs / Redis)",
                    redis_conn is not None,
                    script_shas is not None,
                    bool(pub_limits),
                    bool(auth_limits),
                )
            return await call_next(request)

        tier_name = "public"
        secret = _api_key_or_bearer(request, cfg.auth_header)
        if secret:
            auth_redis = _auth_registry_redis(request)
            if auth_redis is None:
                logger.warning(
                    "public rate limit: auth Redis not configured; rejecting X-API-KEY / Bearer",
                )
                return JSONResponse(
                    status_code=503,
                    content={
                        "error": "service_unavailable",
                        "message": "API key validation unavailable (auth Redis not initialized)",
                    },
                )
            try:
                auth_check = await check_user_details(secret, auth_redis)
            except Exception as exc:
                logger.warning("public rate limit: API key lookup failed open: {}", exc)
                return await call_next(request)

            if not auth_check.authorized or not auth_check.owner:
                return JSONResponse(
                    status_code=401,
                    content={
                        "error": "unauthorized",
                        "message": auth_check.reason or "Invalid or inactive API key",
                    },
                )

            tier_name = "auth"
            tier_key = f"auth:{_digest(auth_check.owner.email)}"
            parsed_limits = list(auth_limits)
        else:
            tier_key = f"ip:{_digest(_forwarded_ip(request))}"
            parsed_limits = list(pub_limits)

        redis_key_bits = [f"{cfg.key_prefix}{tier_key}"]

        try:
            ok, retry_after, violated = await generic_rate_limiter(
                parsed_limits,
                redis_key_bits,
                redis_conn,
                rate_limit_lua_script_shas=script_shas,
            )
        except Exception as exc:
            logger.warning("public rate limit check failed open: {}", exc)
            return await call_next(request)

        if ok:
            return await call_next(request)

        retry_after = max(1, int(retry_after))
        logger.info(
            "public rate limit 429 tier={} method={} path={} violated={} retry_after={} bucket={}",
            tier_name,
            request.method,
            request.url.path,
            violated,
            retry_after,
            tier_key,
        )
        body: dict = {
            "error": "rate_limit_exceeded",
            "message": "Too many requests; retry later.",
            "retry_after": retry_after,
            "tier": tier_name,
        }
        if violated:
            body["limit"] = str(violated)
        return JSONResponse(
            status_code=429,
            content=body,
            headers={"Retry-After": str(retry_after)},
        )
