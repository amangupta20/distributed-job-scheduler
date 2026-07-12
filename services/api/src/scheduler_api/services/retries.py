from datetime import datetime, timedelta
from scheduler_api.enums import RetryStrategy


def calculate_retry_at(strategy: RetryStrategy, base_delay_seconds: int, attempt: int, now: datetime) -> datetime:
    if strategy == RetryStrategy.FIXED:
        delay = base_delay_seconds
    elif strategy == RetryStrategy.LINEAR:
        delay = base_delay_seconds * attempt
    elif strategy == RetryStrategy.EXPONENTIAL:
        delay = base_delay_seconds * (2 ** (attempt - 1))
    else:
        delay = base_delay_seconds

    return now + timedelta(seconds=delay)
