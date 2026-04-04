from modules.ip import IPModule


def test_ip_returns_error_when_patent_client_missing() -> None:
    mod = IPModule()
    r = mod.run("MSFT")
    assert r.name == "ip"
    try:
        import patent_client  # noqa: F401

        assert r.status == "error"
        assert "not wired" in (r.error_message or "").lower() or "patent" in (
            r.error_message or ""
        ).lower()
    except ImportError:
        assert r.status == "error"
        assert r.error_message is not None
