"""
MPP (Machine Payment Protocol) middleware for paid snapshot API routes.

Configuration: `settings.mpp` (see `MppConfig` in settings_model.py).
Env vars `MPP_*` override defaults and optional `mpp` object in config/settings.json.

Modes:
- **billing_mode=tempo** (default): pympp + Tempo ChargeIntent (`Authorization: Payment ...`).
- **billing_mode=signup_api**: deduct credits from SQLite via `bds-agenthub-billing-metering`
  (`MPP_SIGNUP_BILLING_URL` + `MPP_INTERNAL_BILLING_SECRET`; client sends `Authorization: Bearer sk_live_...`).

pympp reads `MPP_SECRET_KEY` from the environment when charging (HMAC challenges).
Optional: `MPP_REALM` (pympp defaults: HOST, etc., else localhost).
"""

from __future__ import annotations

import json

import httpx
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

from snapshotter.settings.config import settings

_mpp = None


def _is_protected(path: str) -> bool:
    return any(path.startswith(prefix) for prefix in settings.mpp.protected_paths_list)


def _is_stream_path(path: str) -> bool:
    """MPP-paid SSE routes use stream_amount (one charge per connection)."""
    return path.startswith("/mpp/stream/")


def _verification_error_payload(exc: Exception) -> dict:
    """Structured body for pympp VerificationError (esp. Tempo RPC fund errors)."""
    msg = str(exc)
    body: dict = {
        "error": "MPP payment verification failed",
        "message": msg,
    }
    low = msg.lower()
    if "insufficient funds" in low or "have 0 want" in low:
        body["hint"] = (
            "If the payer is funded on Tempo testnet but this still appears, the server may "
            "be using the wrong chain: set MPP_TEMPO_CHAIN_ID=42431 (Moderato) or 4217 "
            "(mainnet) to match where you funded. pympp defaults to mainnet when chain_id "
            "was omitted. Also ensure MPP_TEMPO_CURRENCY matches the token you funded."
        )
    return body


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
                chain_id=mpp_cfg.tempo_chain_id,
                intents={"charge": ChargeIntent()},
            ),
        )
    return _mpp


async def _signup_api_billing(request: Request, call_next):
    """Deduct credits via bds-agenthub-billing-metering before serving /mpp/... routes."""
    base = settings.mpp.signup_billing_base_url.strip().rstrip("/")
    secret = settings.mpp.internal_billing_secret.strip()
    if not base or not secret:
        return JSONResponse(
            status_code=500,
            content={
                "error": "MPP configuration error",
                "message": (
                    "billing_mode=signup_api requires MPP_SIGNUP_BILLING_URL and "
                    "MPP_INTERNAL_BILLING_SECRET (must match signup server INTERNAL_BILLING_SECRET)"
                ),
            },
        )

    auth = request.headers.get("Authorization")
    if not auth or not auth.strip():
        return JSONResponse(
            status_code=402,
            content={
                "error": "Payment required",
                "message": (
                    "Bearer API key required (bds-agent signup). "
                    "Tempo MPP is not used when MPP_BILLING_MODE=signup_api."
                ),
            },
        )

    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            r = await client.post(
                f"{base}/internal/billing/deduct",
                headers={
                    "Authorization": auth,
                    "X-BDS-Internal-Billing-Secret": secret,
                    "Content-Type": "application/json",
                },
                json={"path": request.url.path, "method": request.method},
            )
    except httpx.RequestError as exc:
        return JSONResponse(
            status_code=502,
            content={
                "error": "billing_backend_unavailable",
                "message": str(exc),
            },
        )

    if r.status_code == 402:
        try:
            body = r.json()
        except json.JSONDecodeError:
            body = {"message": r.text}
        return JSONResponse(status_code=402, content=body)

    if r.status_code == 401:
        try:
            body = r.json()
        except json.JSONDecodeError:
            body = {"message": r.text}
        return JSONResponse(status_code=401, content=body)

    if r.status_code == 403:
        return JSONResponse(
            status_code=500,
            content={
                "error": "billing_backend_forbidden",
                "message": "Check MPP_INTERNAL_BILLING_SECRET matches signup server INTERNAL_BILLING_SECRET",
            },
        )

    if r.status_code != 200:
        try:
            body = r.json()
        except json.JSONDecodeError:
            body = {"detail": r.text}
        return JSONResponse(
            status_code=502,
            content={"error": "billing_backend_error", "http_status": r.status_code, **body},
        )

    try:
        payload = r.json()
    except json.JSONDecodeError:
        payload = {}

    bal = payload.get("credit_balance")
    response = await call_next(request)
    if isinstance(bal, (int, float)):
        response.headers["X-BDS-Credit-Balance"] = str(bal)
    return response


class MppPaymentMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        if not settings.mpp.enabled or not _is_protected(request.url.path):
            return await call_next(request)

        if settings.mpp.billing_mode == "signup_api":
            return await _signup_api_billing(request, call_next)

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
            charge_amount = (
                settings.mpp.stream_amount
                if _is_stream_path(request.url.path)
                else settings.mpp.charge_amount
            )
            result = await mpp.charge(
                authorization=request.headers.get("Authorization"),
                amount=charge_amount,
            )
        except VerificationError as exc:
            return JSONResponse(status_code=400, content=_verification_error_payload(exc))

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
