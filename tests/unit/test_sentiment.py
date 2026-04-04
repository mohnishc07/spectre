from modules.sentiment import SentimentModule


def test_sentiment_returns_success_when_vader_installed() -> None:
    mod = SentimentModule()
    if not mod.validate():
        # Skip assertion detail when vader missing in minimal env
        r = mod.run("GOOG")
        assert r.status == "error"
        return
    r = mod.run("GOOG")
    assert r.name == "sentiment"
    assert r.status == "success"
    assert r.sentiment is not None
    assert r.sentiment.sample_size is not None
    assert r.sentiment.sample_size >= 1
