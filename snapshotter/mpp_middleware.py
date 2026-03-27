"""
MPP (Machine Payment Protocol) middleware for paid snapshot API routes.

Configuration: `settings.mpp` (see `MppConfig` in settings_model.py).
Env vars `MPP_*` override defaults and optional `mpp` object in config/settings.json.

pympp reads `MPP_SECRET_KEY` from the environment when charging (HMAC challenges).
Optional: `MPP_REALM` (pympp defaults: HOST, etc., else localhost).
"""

from __future__ import annotations

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

from snapshotter.settings.config import settings

_mpp = None


def _is_protected(path: str) -> bool:
    return any(path.startswith(prefix) for prefix in settings.mpp.protected_paths_list)


def _get_mpp():
    """Lazy-init Mpp so pympp/tempo imports are skipped when MPP is disabled."""
    global _mpp
    if _mpp is None:
        from mpp.server import Mpp
        from mpp.methods.tempo import ChargeIntent, tempo

        mpp_cfg = settings.mpp
        _mpp = Mpp.create(
            method=tempo(
                currency=mpp_cfg.tempo_currency,
                recipient=mpp_cfg.tempo_recipient,
                intents={"charge": ChargeIntent()},
            ),
        )
    return _mpp


class MppPaymentMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        if not settings.mpp.enabled or not _is_protected(request.url.path):
            return await call_next(request)

        try:
            mpp = _get_mpp()
        except ValueError as exc:
            return JSONResponse(
                status_code=500,
                content={
                    "error": "MPP configuration error",
                    "message": str(exc),
                },
            )

        from mpp import Challenge
        from mpp.errors import VerificationError

        try:
            result = await mpp.charge(
                authorization=request.headers.get("Authorization"),
                amount=settings.mpp.charge_amount,
            )
        except VerificationError as exc:
            return JSONResponse(
                status_code=400,
                content={
                    "error": "MPP payment verification failed",
                    "message": str(exc),
                },
            )

        if isinstance(result, Challenge):
            return JSONResponse(
                status_code=402,
                content={
                    "error": "Payment required",
                    "message": "MPP payment required for this endpoint",
                },
                headers={"WWW-Authenticate": result.to_www_authenticate(mpp.realm)},
            )

        _, receipt = result
        response = await call_next(request)
        response.headers["Payment-Receipt"] = receipt.to_payment_receipt()
        return response
