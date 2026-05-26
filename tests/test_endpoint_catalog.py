"""Tests for BDS endpoint catalog route matching."""

from snapshotter.endpoint_catalog import EndpointCatalog, CatalogRoute, normalize_client_source


def test_match_metered_snapshot_route():
    catalog = EndpointCatalog(
        [
            CatalogRoute(
                method="GET",
                path_template="/mpp/snapshot/allTrades/{block_number}",
                metered=True,
            ),
            CatalogRoute(
                method="GET",
                path_template="/mpp/snapshot/allTrades",
                metered=True,
            ),
        ]
    )
    assert (
        catalog.match("GET", "/mpp/snapshot/allTrades/21234567")
        == "/mpp/snapshot/allTrades/{block_number}"
    )
    assert catalog.match("GET", "/mpp/snapshot/allTrades") == "/mpp/snapshot/allTrades"
    assert catalog.match("POST", "/mpp/snapshot/allTrades") is None


def test_normalize_client_source_defaults_to_direct():
    assert normalize_client_source(None) == "direct"
    assert normalize_client_source("cli") == "cli"
    assert normalize_client_source("MCP") == "mcp"
    assert normalize_client_source("weird") == "unknown"
