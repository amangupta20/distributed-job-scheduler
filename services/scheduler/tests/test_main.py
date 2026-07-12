import pytest
from fastapi.testclient import TestClient
from scheduler_service.main import app


def test_liveness_and_metrics() -> None:
    # Use TestClient with lifespan context manager
    with TestClient(app) as client:
        # Check live probe
        res_live = client.get("/health/live")
        assert res_live.status_code == 200
        assert res_live.json() == {"status": "alive"}

        # Check ready probe
        res_ready = client.get("/health/ready")
        assert res_ready.status_code == 200
        assert res_ready.json() == {"status": "ready"}

        # Check metrics endpoint
        res_metrics = client.get("/metrics")
        assert res_metrics.status_code == 200
        assert b"process_virtual_memory_bytes" in res_metrics.content
