from enum import Enum


class JobStatus(str, Enum):
    QUEUED = "queued"
    SCHEDULED = "scheduled"
    CLAIMED = "claimed"
    RUNNING = "running"
    RETRY_SCHEDULED = "retry_scheduled"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    DEAD_LETTERED = "dead_lettered"


class RetryStrategy(str, Enum):
    FIXED = "fixed"
    LINEAR = "linear"
    EXPONENTIAL = "exponential"


class WorkerState(str, Enum):
    STARTING = "starting"
    READY = "ready"
    DRAINING = "draining"
    OFFLINE = "offline"


class Role(str, Enum):
    OWNER = "owner"
    ADMIN = "admin"
    MEMBER = "member"
