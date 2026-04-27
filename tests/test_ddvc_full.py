from __future__ import annotations

from app.ddvc_full import validar_sku_directo


def test_validar_sku_directo_exists_uses_cache(monkeypatch):
    calls = {"count": 0}

    def fake_gql(graphql_url, query, variables, timeout_s):
        calls["count"] += 1
        return {
            "data": {
                "products": {
                    "items": [
                        {"sku": variables["sku"], "is_salable": True},
                    ]
                }
            }
        }

    monkeypatch.setattr("app.ddvc_full.gql", fake_gql)
    cache = {}

    first = validar_sku_directo("https://example.test/graphql", "abc-123", cache=cache)
    second = validar_sku_directo("https://example.test/graphql", "ABC-123", cache=cache)

    assert first == (True, True)
    assert second == (True, True)
    assert calls["count"] == 1


def test_validar_sku_directo_not_found(monkeypatch):
    def fake_gql(graphql_url, query, variables, timeout_s):
        return {"data": {"products": {"items": []}}}

    monkeypatch.setattr("app.ddvc_full.gql", fake_gql)

    result = validar_sku_directo("https://example.test/graphql", "missing-sku", cache={})

    assert result == (False, None)


def test_validar_sku_directo_error_returns_none_tuple(monkeypatch):
    def fake_gql(graphql_url, query, variables, timeout_s):
        raise RuntimeError("boom")

    monkeypatch.setattr("app.ddvc_full.gql", fake_gql)
    cache = {}

    result = validar_sku_directo("https://example.test/graphql", "err-sku", cache=cache)

    assert result == (None, None)
    assert cache["ERR-SKU"] == (None, None)


def test_validar_skus_batch_basic(monkeypatch):
    """Batch validation debe encontrar SKUs existentes."""

    def fake_gql(graphql_url, query, variables, timeout_s):
        # Simular que solo 2 de 3 SKUs existen
        requested = variables.get("skus", [])
        items = []
        for sku in requested:
            if sku != "MISSING-SKU":
                items.append({
                    "sku": sku,
                    "is_salable": True,
                    "price_range": {
                        "minimum_price": {
                            "regular_price": {"value": 100.0},
                            "final_price": {"value": 95.0},
                        }
                    },
                })
        return {"data": {"products": {"items": items}}}

    monkeypatch.setattr("app.ddvc_full.gql", fake_gql)
    from app.ddvc_full import validar_skus_batch

    results = validar_skus_batch(
        "https://example.test/graphql",
        ["SKU-A", "SKU-B", "MISSING-SKU"],
        chunk_size=10,
    )

    assert "SKU-A" in results
    assert "SKU-B" in results
    assert "MISSING-SKU" not in results
    assert results["SKU-A"]["is_salable"] is True
