"""Tests for BDS endpoint catalog route matching and credit weighting."""

from snapshotter.endpoint_catalog import (
    TIMESERIES_ROUTE_TEMPLATE,
    BillingModifier,
    CatalogRoute,
    EndpointCatalog,
    LookbackTier,
    _load_catalog_json,
    history_multiplier_for_lookback_seconds,
    history_multiplier_for_match,
    history_multiplier_for_path,
    lookback_seconds_for_match,
    normalize_client_source,
    parse_timeseries_lookback_seconds,
)

_TIMESERIES_TIERS = (
    LookbackTier(600, 1.0),
    LookbackTier(1_800, 2.0),
    LookbackTier(3_600, 4.0),
    LookbackTier(7_200, 8.0),
    LookbackTier(86_400, 128.0),
    LookbackTier(604_800, 1024.0),
)


def _timeseries_route() -> CatalogRoute:
    return CatalogRoute(
        method="GET",
        path_template=TIMESERIES_ROUTE_TEMPLATE,
        metered=True,
        credit_weight=5,
        billing_modifier=BillingModifier(
            type="lookback_multiplier",
            param="time_interval",
            tiers=_TIMESERIES_TIERS,
            overflow_multiplier=2048.0,
        ),
    )


def _ts_path(interval: int, step: int = 144) -> str:
    return f"/mpp/timeSeries/0xtoken/0xpool/{interval}/{step}"


def test_match_metered_snapshot_route():
    catalog = EndpointCatalog(
        [
            CatalogRoute(
                method="GET",
                path_template="/mpp/snapshot/allTrades/{block_number}",
                metered=True,
                credit_weight=1,
            ),
            CatalogRoute(
                method="GET",
                path_template="/mpp/snapshot/allTrades",
                metered=True,
                credit_weight=1,
            ),
        ]
    )
    m1 = catalog.match("GET", "/mpp/snapshot/allTrades/21234567")
    assert m1 is not None
    assert m1.path_template == "/mpp/snapshot/allTrades/{block_number}"
    assert m1.credit_weight == 1
    assert m1.params == {"block_number": "21234567"}
    m2 = catalog.match("GET", "/mpp/snapshot/allTrades")
    assert m2 is not None
    assert m2.path_template == "/mpp/snapshot/allTrades"
    assert catalog.match("POST", "/mpp/snapshot/allTrades") is None


def test_match_token_prices_premium_weight():
    catalog = EndpointCatalog(
        [
            CatalogRoute(
                method="GET",
                path_template="/mpp/tokenPrices/all/{token_address}/{block_number}",
                metered=True,
                credit_weight=10,
            ),
        ]
    )
    m = catalog.match("GET", "/mpp/tokenPrices/all/0xabc/21900123")
    assert m is not None
    assert m.credit_weight == 10
    assert m.params == {"token_address": "0xabc", "block_number": "21900123"}


def test_normalize_client_source_defaults_to_direct():
    assert normalize_client_source(None) == "direct"
    assert normalize_client_source("cli") == "cli"
    assert normalize_client_source("MCP") == "mcp"
    assert normalize_client_source("weird") == "unknown"


def test_parse_timeseries_lookback_seconds():
    path = (
        "/mpp/timeSeries/0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2/"
        "0x88e6a0c2d3dd730b3f5938e5577a86d301b41c592/3600/144"
    )
    assert parse_timeseries_lookback_seconds(path) == 3600
    assert parse_timeseries_lookback_seconds("/mpp/token/price/0x/0x/1") is None


def test_fallback_history_multiplier_tiers():
    """Hardcoded fallback table (used only when the catalog has no modifier)."""
    assert history_multiplier_for_lookback_seconds(600) == 1.0
    assert history_multiplier_for_lookback_seconds(3_600) == 4.0
    assert history_multiplier_for_lookback_seconds(86_400) == 128.0
    assert history_multiplier_for_lookback_seconds(604_800) == 1024.0
    assert history_multiplier_for_lookback_seconds(2_000_000) == 2048.0


def test_history_multiplier_for_path_non_timeseries():
    assert (
        history_multiplier_for_path("/mpp/token/price/0x/0x/123", "/mpp/token/price/{x}")
        == 1.0
    )


def test_catalog_driven_multiplier_from_modifier():
    catalog = EndpointCatalog([_timeseries_route()])
    m = catalog.match("GET", _ts_path(3_600))
    assert m is not None
    assert m.params["time_interval"] == "3600"
    assert history_multiplier_for_match(m) == 4.0
    assert lookback_seconds_for_match(m) == 3600


def test_catalog_driven_multiplier_overflow():
    catalog = EndpointCatalog([_timeseries_route()])
    m = catalog.match("GET", _ts_path(2_000_000))
    assert m is not None
    assert history_multiplier_for_match(m) == 2048.0


def test_match_multiplier_falls_back_without_modifier():
    """A timeSeries route with no modifier uses the hardcoded fallback table."""
    route = CatalogRoute(
        method="GET",
        path_template=TIMESERIES_ROUTE_TEMPLATE,
        metered=True,
        credit_weight=5,
    )
    catalog = EndpointCatalog([route])
    m = catalog.match("GET", _ts_path(86_400))
    assert m is not None
    assert m.billing_modifier is None
    assert history_multiplier_for_match(m) == 128.0
    assert lookback_seconds_for_match(m) == 86_400


def test_non_timeseries_match_has_unit_multiplier():
    catalog = EndpointCatalog(
        [
            CatalogRoute(
                method="GET",
                path_template="/mpp/snapshot/base/{pool_address}",
                metered=True,
                credit_weight=1,
            ),
        ]
    )
    m = catalog.match("GET", "/mpp/snapshot/base/0xpool")
    assert m is not None
    assert history_multiplier_for_match(m) == 1.0
    assert lookback_seconds_for_match(m) is None


def test_load_catalog_json_parses_billing_modifier():
    data = {
        "market": "TEST",
        "version": 3,
        "endpoints": [
            {
                "path": TIMESERIES_ROUTE_TEMPLATE,
                "method": "GET",
                "metered": True,
                "credit_weight": 5,
                "billing_modifier": {
                    "type": "lookback_multiplier",
                    "param": "time_interval",
                    "tiers": [
                        {"max_seconds": 600, "multiplier": 1},
                        {"max_seconds": 3600, "multiplier": 4},
                    ],
                    "overflow_multiplier": 2048,
                },
            },
        ],
    }
    routes = _load_catalog_json(data)
    assert len(routes) == 1
    mod = routes[0].billing_modifier
    assert mod is not None
    assert mod.type == "lookback_multiplier"
    assert mod.param == "time_interval"
    assert mod.overflow_multiplier == 2048.0
    assert [(t.max_seconds, t.multiplier) for t in mod.tiers] == [(600, 1.0), (3600, 4.0)]

    catalog = EndpointCatalog(routes)
    m = catalog.match("GET", _ts_path(3_600))
    assert history_multiplier_for_match(m) == 4.0


def test_load_catalog_json_no_modifier_is_none():
    data = {
        "market": "TEST",
        "version": 3,
        "endpoints": [
            {
                "path": "/mpp/snapshot/allTrades",
                "method": "GET",
                "metered": True,
                "credit_weight": 1,
            },
        ],
    }
    routes = _load_catalog_json(data)
    assert routes[0].billing_modifier is None
