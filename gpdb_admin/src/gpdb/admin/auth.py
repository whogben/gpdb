"""Password hashing and simple signed session cookies."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
from dataclasses import dataclass


SESSION_COOKIE_NAME = "gpdb_admin_session"
_PASSWORD_ALGORITHM = "pbkdf2_sha256"
_PASSWORD_ITERATIONS = 200_000


def hash_password(password: str) -> str:
    """Return a salted password hash suitable for storage."""
    salt = os.urandom(16)
    digest = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt,
        _PASSWORD_ITERATIONS,
    )
    salt_text = base64.urlsafe_b64encode(salt).decode("ascii")
    digest_text = base64.urlsafe_b64encode(digest).decode("ascii")
    return f"{_PASSWORD_ALGORITHM}${_PASSWORD_ITERATIONS}${salt_text}${digest_text}"


def verify_password(password: str, stored_hash: str) -> bool:
    """Verify a password against a stored PBKDF2 hash."""
    try:
        algorithm, iteration_text, salt_text, digest_text = stored_hash.split("$", 3)
    except ValueError:
        return False
    if algorithm != _PASSWORD_ALGORITHM:
        return False

    try:
        iterations = int(iteration_text)
        salt = base64.urlsafe_b64decode(salt_text.encode("ascii"))
        expected_digest = base64.urlsafe_b64decode(digest_text.encode("ascii"))
    except (ValueError, TypeError):
        return False

    actual_digest = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt,
        iterations,
    )
    return hmac.compare_digest(actual_digest, expected_digest)


@dataclass(frozen=True)
class SessionData:
    """Identity data stored in the signed browser cookie."""

    user_id: str
    auth_version: int


class SessionSigner:
    """Create and validate signed session cookies without extra deps."""

    def __init__(self, secret: str):
        self._secret = secret.encode("utf-8")

    def dumps(self, session: SessionData) -> str:
        """Serialize and sign session data for a cookie value."""
        payload = json.dumps(
            {"user_id": session.user_id, "auth_version": session.auth_version},
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        payload_text = _urlsafe_b64encode(payload)
        signature = hmac.new(
            self._secret,
            payload_text.encode("ascii"),
            hashlib.sha256,
        ).digest()
        return f"{payload_text}.{_urlsafe_b64encode(signature)}"

    def loads(self, value: str) -> SessionData | None:
        """Validate and deserialize a signed cookie value."""
        try:
            payload_text, signature_text = value.split(".", 1)
        except ValueError:
            return None

        expected_signature = hmac.new(
            self._secret,
            payload_text.encode("ascii"),
            hashlib.sha256,
        ).digest()
        actual_signature = _urlsafe_b64decode(signature_text)
        if actual_signature is None:
            return None
        if not hmac.compare_digest(expected_signature, actual_signature):
            return None

        payload = _urlsafe_b64decode(payload_text)
        if payload is None:
            return None

        try:
            data = json.loads(payload.decode("utf-8"))
            return SessionData(
                user_id=str(data["user_id"]),
                auth_version=int(data["auth_version"]),
            )
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            return None


def _urlsafe_b64encode(value: bytes) -> str:
    """Encode bytes without padding for compact cookie storage."""
    return base64.urlsafe_b64encode(value).decode("ascii").rstrip("=")


def _urlsafe_b64decode(value: str) -> bytes | None:
    """Decode a compact base64 value, returning None on malformed input."""
    padding = "=" * (-len(value) % 4)
    try:
        return base64.urlsafe_b64decode((value + padding).encode("ascii"))
    except (ValueError, TypeError):
        return None
