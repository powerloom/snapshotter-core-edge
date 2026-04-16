"""
Public (non-MPP) HTTP rate limiting for Core API free routes.

Uses ``async_limits`` + main ``app.state.redis_conn`` (same stack as
``auth/helpers/rate_limiter.py``). Skips ``/mpp/...`` (MPP middleware handles
those). See ``ai-coord-docs/bds-mpp-integration/15-mpp-full-uniswap-surface.md``.
"""

from __future__ import annotations

import hashlib
from typing import Optional

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

from snapshotter.auth.helpers.rate_limiter import generic_rate_limiter
from snapshotter.settings.config import settings
from snapshotter.utils.default_logger import default_logger

logger = default_logger.bind(module="PublicRateLimit")


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
        pub_lim = getattr(request.app.state, "public_rate_limit_item_public", None)
        auth_lim = getattr(request.app.state, "public_rate_limit_item_auth", None)

        if redis_conn is None or script_shas is None or pub_lim is None or auth_lim is None:
            return await call_next(request)

        secret = _api_key_or_bearer(request, cfg.auth_header)
        if secret:
            tier_key = f"auth:{_digest(secret)}"
            parsed_limits = [auth_lim]
        else:
            tier_key = f"ip:{_digest(_forwarded_ip(request))}"
            parsed_limits = [pub_lim]

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
        body: dict = {
            "error": "rate_limit_exceeded",
            "message": "Too many requests; retry later.",
            "retry_after": retry_after,
        }
        if violated:
            body["limit"] = str(violated)
        return JSONResponse(
            status_code=429,
            content=body,
            headers={"Retry-After": str(retry_after)},
        )
