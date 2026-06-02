"""
Match incoming HTTP requests to BDS catalog route templates (``endpoints.json``).

Used by MPP middleware to attach ``route_template`` to metering deduct calls.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx

DEFAULT_ENDPOINTS_CATALOG_URL = (
    "https://raw.githubusercontent.com/powerloom/snapshotter-computes/"
    "bds_eth_uniswapv3_core/api/endpoints.json"
)

TIMESERIES_ROUTE_TEMPLATE = (
    "/mpp/timeSeries/{token_address}/{pool_address}/{time_interval}/{step_seconds}"
)

# USD price consumption (see ai-coord-docs/compute-modules/USD_PRICE_FEED.md):
#
# 1. Discover pools: GET /mpp/token/{token}/pools
# 2. Price each pool you care about: GET /mpp/token/price/{token}/{pool}[/{block}]
#
# ``GET /mpp/tokenPrices/all/{token}`` is NOT supported for hub tokens (USDC, WETH, and
# other majors with thousands of indexed pools). The API rejects >20 pools per token.
# Use the two-step pattern above — e.g. ASTEROID ``0xf280B16EF293D8e534e370794ef26bF312694126``.
#
# Pulse / Threshold Guard: per-block ``/mpp/token/price/.../{block_number}`` on pinned pools.
# Dashboards / analytics: ``/mpp/timeSeries/...`` (lookback tiers below), not tokenPrices/all.

# FALLBACK ONLY. The authoritative lookback tiers now live in the catalog
# (endpoints.json ``billing_modifier``). This hardcoded table is used only when a
# catalog entry has no modifier, so a code-before-catalog deploy keeps billing
# byte-identical. Keep it in sync with the catalog until every market ships the
# modifier, then it can be removed.
_LOOKBACK_HISTORY_MULTIPLIERS: tuple[tuple[int, float], ...] = (
    (600, 1.0),       # <= 10 minutes
    (1_800, 2.0),     # <= 30 minutes
    (3_600, 4.0),     # <= 1 hour
    (7_200, 8.0),     # <= 2 hours
    (14_400, 16.0),   # <= 4 hours
    (21_600, 32.0),   # <= 6 hours
    (43_200, 64.0),   # <= 12 hours
    (86_400, 128.0),  # <= 24 hours
    (172_800, 256.0), # <= 48 hours
    (345_600, 512.0), # <= 96 hours
    (604_800, 1024.0),# <= 7 days
)
_MAX_LOOKBACK_HISTORY_MULTIPLIER = 2048.0

# Billing-modifier types (see snapshotter-computes api/endpoints.json).
LOOKBACK_MULTIPLIER_TYPE = "lookback_multiplier"
STREAM_SESSION_TYPE = "stream_session"


@dataclass(frozen=True)
class LookbackTier:
    max_seconds: int
    multiplier: float


@dataclass(frozen=True)
class BillingModifier:
    """Per-route billing policy beyond static credit_weight (lookback tiers or stream session)."""

    type: str
    param: str = ""
    tiers: tuple[LookbackTier, ...] = ()
    overflow_multiplier: float = 1.0
    credits_per_connection: float | None = None


@dataclass(frozen=True)
class CatalogRoute:
    method: str
    path_template: str
    metered: bool
    credit_weight: float = 1.0
    billing_modifier: BillingModifier | None = None


@dataclass(frozen=True)
class CatalogMatch:
    path_template: str
    credit_weight: float
    params: dict[str, str] = field(default_factory=dict)
    billing_modifier: BillingModifier | None = None


class EndpointCatalog:
    """Resolve ``(method, request_path)`` to a catalog path template and credit weight."""

    def __init__(self, routes: list[CatalogRoute]) -> None:
        compiled: list[
            tuple[str, re.Pattern[str], str, float, BillingModifier | None]
        ] = []
        for route in routes:
            if not route.metered:
                continue
            pattern = _template_to_regex(route.path_template)
            w = route.credit_weight if route.credit_weight > 0 else 1.0
            compiled.append(
                (route.method.upper(), pattern, route.path_template, w, route.billing_modifier),
            )
        self._compiled = compiled

    def match(self, method: str, request_path: str) -> CatalogMatch | None:
        m = method.strip().upper() or "GET"
        path = request_path if request_path.startswith("/") else f"/{request_path}"
        for route_method, pattern, template, weight, modifier in self._compiled:
            if route_method != m:
                continue
            mobj = pattern.fullmatch(path)
            if mobj:
                return CatalogMatch(
                    path_template=template,
                    credit_weight=weight,
                    params=mobj.groupdict(),
                    billing_modifier=modifier,
                )
        return None


def _template_to_regex(template: str) -> re.Pattern[str]:
    parts: list[str] = []
    for segment in template.split("/"):
        if not segment:
            continue
        if segment.startswith("{") and segment.endswith("}"):
            name = segment[1:-1]
            # Capture path params as named groups so billing modifiers can read
            # them by name. Non-identifier names fall back to anonymous matching.
            if name.isidentifier():
                parts.append(rf"(?P<{name}>[^/]+)")
            else:
                parts.append(r"[^/]+")
        else:
            parts.append(re.escape(segment))
    body = "/".join(parts)
    return re.compile(rf"^/{body}$")


def _safe_positive_int(value: Any) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _parse_billing_modifier(raw: Any) -> BillingModifier | None:
    """Parse an optional per-route ``billing_modifier`` block; tolerant of junk."""
    if not isinstance(raw, dict):
        return None
    mtype = raw.get("type")
    if not isinstance(mtype, str):
        return None
    if mtype == STREAM_SESSION_TYPE:
        try:
            credits = float(raw.get("credits_per_connection"))
        except (TypeError, ValueError):
            return None
        if credits <= 0:
            return None
        return BillingModifier(
            type=mtype,
            credits_per_connection=credits,
        )
    if mtype != LOOKBACK_MULTIPLIER_TYPE:
        return None
    param = raw.get("param")
    if not isinstance(param, str) or not param:
        return None
    tiers: list[LookbackTier] = []
    tiers_raw = raw.get("tiers")
    if isinstance(tiers_raw, list):
        for tier in tiers_raw:
            if not isinstance(tier, dict):
                continue
            max_seconds = _safe_positive_int(tier.get("max_seconds"))
            try:
                multiplier = float(tier.get("multiplier"))
            except (TypeError, ValueError):
                multiplier = 0.0
            if max_seconds is None or multiplier <= 0:
                continue
            tiers.append(LookbackTier(max_seconds=max_seconds, multiplier=multiplier))
    tiers.sort(key=lambda t: t.max_seconds)
    try:
        overflow = float(raw.get("overflow_multiplier", 1.0))
    except (TypeError, ValueError):
        overflow = 1.0
    if overflow <= 0:
        overflow = 1.0
    if not tiers and overflow == 1.0:
        return None
    return BillingModifier(
        type=mtype,
        param=param,
        tiers=tuple(tiers),
        overflow_multiplier=overflow,
    )


def _load_catalog_json(data: Any) -> list[CatalogRoute]:
    if not isinstance(data, dict):
        raise ValueError("endpoints catalog root must be an object")
    endpoints = data.get("endpoints")
    if not isinstance(endpoints, list):
        raise ValueError("endpoints catalog missing endpoints[]")
    routes: list[CatalogRoute] = []
    for entry in endpoints:
        if not isinstance(entry, dict):
            continue
        path = entry.get("path")
        method = entry.get("method", "GET")
        if not isinstance(path, str) or not isinstance(method, str):
            continue
        metered = bool(entry.get("metered", False))
        raw_weight = entry.get("credit_weight", 1)
        try:
            credit_weight = float(raw_weight)
        except (TypeError, ValueError):
            credit_weight = 1.0
        if credit_weight <= 0:
            credit_weight = 1.0
        routes.append(
            CatalogRoute(
                method=method,
                path_template=path,
                metered=metered,
                credit_weight=credit_weight,
                billing_modifier=_parse_billing_modifier(entry.get("billing_modifier")),
            ),
        )
    return routes


def load_catalog_from_ref(ref: str) -> EndpointCatalog:
    ref = ref.strip()
    if not ref:
        raise ValueError("empty catalog ref")
    if ref.startswith("http://") or ref.startswith("https://"):
        with httpx.Client(timeout=60.0) as client:
            response = client.get(ref)
            response.raise_for_status()
            data = response.json()
    else:
        text = Path(ref).read_text(encoding="utf-8")
        data = json.loads(text)
    return EndpointCatalog(_load_catalog_json(data))


_catalog: EndpointCatalog | None = None
_catalog_ref_loaded: str | None = None


def get_endpoint_catalog(catalog_ref: str | None = None) -> EndpointCatalog:
    global _catalog, _catalog_ref_loaded
    ref = (catalog_ref or os.environ.get("MPP_ENDPOINTS_CATALOG_JSON", "")).strip()
    if not ref:
        ref = DEFAULT_ENDPOINTS_CATALOG_URL
    if _catalog is None or _catalog_ref_loaded != ref:
        _catalog = load_catalog_from_ref(ref)
        _catalog_ref_loaded = ref
    return _catalog


def reset_endpoint_catalog_cache() -> None:
    """Test helper."""
    global _catalog, _catalog_ref_loaded
    _catalog = None
    _catalog_ref_loaded = None


def normalize_client_source(header_value: str | None) -> str:
    if not header_value:
        return "direct"
    value = header_value.strip().lower()
    if value in {"cli", "mcp", "direct", "unknown"}:
        return value
    return "unknown"


def parse_timeseries_lookback_seconds(request_path: str) -> int | None:
    """Parse ``time_interval`` (lookback seconds) from a timeSeries request path."""
    path = request_path if request_path.startswith("/") else f"/{request_path}"
    pattern = re.compile(
        r"^/mpp/timeSeries/[^/]+/[^/]+/(?P<interval>\d+)/\d+$",
    )
    match = pattern.fullmatch(path)
    if not match:
        return None
    try:
        interval = int(match.group("interval"))
    except (TypeError, ValueError):
        return None
    if interval <= 0:
        return None
    return interval


def history_multiplier_for_lookback_seconds(lookback_seconds: int) -> float:
    """
    Extra billing multiplier for timeSeries depth (how far back ``time_interval`` reaches).

    Pulse / Guard use per-block ``/mpp/token/price/.../{block_number}``; timeSeries is for
    dashboards and analytics over a window.
    """
    if lookback_seconds <= 0:
        return 1.0
    for max_seconds, multiplier in _LOOKBACK_HISTORY_MULTIPLIERS:
        if lookback_seconds <= max_seconds:
            return multiplier
    return _MAX_LOOKBACK_HISTORY_MULTIPLIER


def history_multiplier_for_path(request_path: str, path_template: str | None) -> float:
    """
    Legacy fallback: lookback multiplier from the hardcoded table for the known
    timeSeries template (1.0 otherwise). Prefer :func:`history_multiplier_for_match`.
    """
    if path_template != TIMESERIES_ROUTE_TEMPLATE:
        return 1.0
    lookback = parse_timeseries_lookback_seconds(request_path)
    if lookback is None:
        return 1.0
    return history_multiplier_for_lookback_seconds(lookback)


def _multiplier_from_modifier(modifier: BillingModifier, param_value: Any) -> float:
    seconds = _safe_positive_int(param_value)
    if seconds is None:
        return 1.0
    for tier in modifier.tiers:  # ascending by max_seconds
        if seconds <= tier.max_seconds:
            return tier.multiplier
    return modifier.overflow_multiplier


def history_multiplier_for_match(match: CatalogMatch | None) -> float:
    """
    Credit multiplier for a matched route.

    Catalog-driven: when the matched route carries a ``lookback_multiplier``
    billing modifier, read its driving path param and resolve the tier. Falls back
    to the hardcoded table for the known timeSeries template when no modifier is
    present (so a code-before-catalog deploy keeps billing identical). Returns 1.0
    for everything else.
    """
    if match is None:
        return 1.0
    mod = match.billing_modifier
    if mod is not None and mod.type == LOOKBACK_MULTIPLIER_TYPE:
        return _multiplier_from_modifier(mod, match.params.get(mod.param))
    if match.path_template == TIMESERIES_ROUTE_TEMPLATE:
        seconds = _safe_positive_int(match.params.get("time_interval"))
        if seconds is None:
            return 1.0
        return history_multiplier_for_lookback_seconds(seconds)
    return 1.0


def lookback_seconds_for_match(match: CatalogMatch | None) -> int | None:
    """The driving lookback param value for a match (for billing telemetry)."""
    if match is None:
        return None
    mod = match.billing_modifier
    if mod is not None and mod.type == LOOKBACK_MULTIPLIER_TYPE:
        return _safe_positive_int(match.params.get(mod.param))
    if match.path_template == TIMESERIES_ROUTE_TEMPLATE:
        return _safe_positive_int(match.params.get("time_interval"))
    return None
