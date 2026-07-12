import pytest
from datetime import datetime, timedelta, timezone
from scheduler_api.enums import RetryStrategy
from scheduler_api.services.retries import calculate_retry_at


@pytest.mark.parametrize(
    ("strategy", "attempt", "expected_seconds"),
    [
        (RetryStrategy.FIXED, 1, 10),
        (RetryStrategy.FIXED, 2, 10),
        (RetryStrategy.FIXED, 3, 10),
        (RetryStrategy.LINEAR, 1, 10),
        (RetryStrategy.LINEAR, 2, 20),
        (RetryStrategy.LINEAR, 3, 30),
        (RetryStrategy.EXPONENTIAL, 1, 10),
        (RetryStrategy.EXPONENTIAL, 2, 20),
        (RetryStrategy.EXPONENTIAL, 3, 40),
    ],
)
def test_calculate_retry_at(strategy: RetryStrategy, attempt: int, expected_seconds: int) -> None:
    now = datetime(2026, 7, 12, 12, 0, 0, tzinfo=timezone.utc)
    base_delay = 10
    result = calculate_retry_at(
        strategy=strategy,
        base_delay_seconds=base_delay,
        attempt=attempt,
        now=now,
    )
    assert result == now + timedelta(seconds=expected_seconds)
