import requests

from scripts.opendota_pipeline_utils import fetch_json_with_backoff


class FakeResponse:
    def __init__(self, *, status_code: int, payload: object) -> None:
        self.status_code = status_code
        self._payload = payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"status={self.status_code}")

    def json(self) -> object:
        return self._payload


def test_fetch_json_with_backoff_retries_rate_limit_then_returns_json(monkeypatch) -> None:
    responses = [
        FakeResponse(status_code=429, payload={"error": "rate_limited"}),
        FakeResponse(status_code=200, payload={"ok": True}),
    ]
    recorded_sleeps: list[float] = []

    def fake_get(*args, **kwargs):
        return responses.pop(0)

    monkeypatch.setattr("scripts.opendota_pipeline_utils.requests.get", fake_get)
    monkeypatch.setattr(
        "scripts.opendota_pipeline_utils.time.sleep",
        lambda sleep_seconds: recorded_sleeps.append(sleep_seconds),
    )

    response_payload = fetch_json_with_backoff(
        "https://api.opendota.com/api/proMatches",
        max_retries=2,
        retry_sleep_seconds=0.5,
    )

    assert response_payload == {"ok": True}
    assert recorded_sleeps == [0.5]


def test_fetch_json_with_backoff_raises_after_retry_budget_exhausts(monkeypatch) -> None:
    responses = [
        FakeResponse(status_code=429, payload={"error": "rate_limited"}),
        FakeResponse(status_code=429, payload={"error": "rate_limited_again"}),
    ]
    recorded_sleeps: list[float] = []

    def fake_get(*args, **kwargs):
        return responses.pop(0)

    monkeypatch.setattr("scripts.opendota_pipeline_utils.requests.get", fake_get)
    monkeypatch.setattr(
        "scripts.opendota_pipeline_utils.time.sleep",
        lambda sleep_seconds: recorded_sleeps.append(sleep_seconds),
    )

    try:
        fetch_json_with_backoff(
            "https://api.opendota.com/api/proMatches",
            max_retries=1,
            retry_sleep_seconds=0.5,
        )
    except requests.HTTPError as request_error:
        assert "status=429" in str(request_error)
    else:
        raise AssertionError("Expected a rate-limited request to raise after retries.")

    assert recorded_sleeps == [0.5]
