"""Tests for BDS endpoint catalog route matching."""

from snapshotter.endpoint_catalog import EndpointCatalog, CatalogRoute, normalize_client_source


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
