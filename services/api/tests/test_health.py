from fastapi.testclient import TestClient
from unittest.mock import AsyncMock
from scheduler_api.main import create_app
from scheduler_api.db import get_session


def test_liveness_has_stable_shape() -> None:
    response = TestClient(create_app()).get("/health/live")
    assert response.status_code == 200
    assert response.json() == {"status": "alive"}


def test_unknown_route_uses_error_envelope() -> None:
    response = TestClient(create_app()).get("/does-not-exist")
    assert response.status_code == 404
    body = response.json()["error"]
    assert body["code"] == "not_found"
    assert body["message"] is not None
    assert body["trace_id"] is not None


def test_readiness_healthy() -> None:
    app = create_app()
    mock_session = AsyncMock()
    app.dependency_overrides[get_session] = lambda: mock_session
    client = TestClient(app)
    response = client.get("/health/ready")
    assert response.status_code == 200
    assert response.json() == {"status": "ready"}
    mock_session.execute.assert_called_once()

