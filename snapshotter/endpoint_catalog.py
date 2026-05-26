"""
Match incoming HTTP requests to BDS catalog route templates (``endpoints.json``).

Used by MPP middleware to attach ``route_template`` to metering deduct calls.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx

DEFAULT_ENDPOINTS_CATALOG_URL = (
    "https://raw.githubusercontent.com/powerloom/snapshotter-computes/"
    "bds_eth_uniswapv3_core/api/endpoints.json"
)


@dataclass(frozen=True)
class CatalogRoute:
    method: str
    path_template: str
    metered: bool


class EndpointCatalog:
    """Resolve ``(method, request_path)`` to a catalog path template."""

    def __init__(self, routes: list[CatalogRoute]) -> None:
        compiled: list[tuple[str, re.Pattern[str], str]] = []
        for route in routes:
            if not route.metered:
                continue
            pattern = _template_to_regex(route.path_template)
            compiled.append((route.method.upper(), pattern, route.path_template))
        self._compiled = compiled

    def match(self, method: str, request_path: str) -> str | None:
        m = method.strip().upper() or "GET"
        path = request_path if request_path.startswith("/") else f"/{request_path}"
        for route_method, pattern, template in self._compiled:
            if route_method != m:
                continue
            if pattern.fullmatch(path):
                return template
        return None


def _template_to_regex(template: str) -> re.Pattern[str]:
    parts: list[str] = []
    for segment in template.split("/"):
        if not segment:
            continue
        if segment.startswith("{") and segment.endswith("}"):
            parts.append(r"[^/]+")
        else:
            parts.append(re.escape(segment))
    body = "/".join(parts)
    return re.compile(rf"^/{body}$")


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
        routes.append(CatalogRoute(method=method, path_template=path, metered=metered))
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
