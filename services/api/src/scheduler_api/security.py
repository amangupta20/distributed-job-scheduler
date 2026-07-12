from datetime import datetime, timedelta, timezone
from typing import Any, Dict
from uuid import UUID
import jwt
from pwdlib import PasswordHash

from scheduler_api.enums import Role

password_hash = PasswordHash.recommended()


def verify_password(plain_password: str, hashed_password: str) -> bool:
    try:
        return password_hash.verify(plain_password, hashed_password)
    except Exception:
        return False


def get_password_hash(password: str) -> str:
    return password_hash.hash(password)


def create_access_token(*, user_id: UUID, org_id: UUID, role: Role, settings: Any) -> str:
    now = datetime.now(timezone.utc)
    payload = {
        "sub": str(user_id),
        "org_id": str(org_id),
        "role": role.value,
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(hours=1)).timestamp()),
    }
    token = jwt.encode(payload, settings.jwt_secret, algorithm="HS256")
    if isinstance(token, bytes):
        return token.decode("utf-8")
    return token



def decode_access_token(token: str, settings: Any) -> Dict[str, Any]:
    return jwt.decode(token, settings.jwt_secret, algorithms=["HS256"])
