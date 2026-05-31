"""Tests for BDS endpoint catalog route matching."""

from snapshotter.endpoint_catalog import (
    CatalogRoute,
    EndpointCatalog,
    history_multiplier_for_lookback_seconds,
    history_multiplier_for_path,
    normalize_client_source,
    parse_timeseries_lookback_seconds,
)


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


def test_history_multiplier_tiers():
    assert history_multiplier_for_lookback_seconds(3_600) == 1.0
    assert history_multiplier_for_lookback_seconds(21_600) == 2.0
    assert history_multiplier_for_lookback_seconds(86_400) == 4.0
    assert history_multiplier_for_lookback_seconds(604_800) == 8.0
    assert history_multiplier_for_lookback_seconds(2_000_000) == 12.0


def test_history_multiplier_for_path_non_timeseries():
    assert (
        history_multiplier_for_path("/mpp/token/price/0x/0x/123", "/mpp/token/price/{x}")
        == 1.0
    )
