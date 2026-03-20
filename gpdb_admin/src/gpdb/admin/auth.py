"""Password hashing and simple signed session cookies."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import secrets
from dataclasses import dataclass


SESSION_COOKIE_NAME = "gpdb_admin_session"
API_KEY_PREFIX = "gpdb"
_PASSWORD_ALGORITHM = "pbkdf2_sha256"
_PASSWORD_ITERATIONS = 200_000


def hash_password(password: str) -> str:
    """Return a salted password hash suitable for storage."""
    return _hash_pbkdf2_secret(password)


def hash_api_key_secret(secret: str) -> str:
    """Return a salted hash for one API key secret fragment."""
    return _hash_pbkdf2_secret(secret)


def verify_api_key_secret(secret: str, stored_hash: str) -> bool:
    """Verify an API key secret fragment against a stored hash."""
    return _verify_pbkdf2_secret(secret, stored_hash)


def generate_api_key() -> "GeneratedAPIKey":
    """Generate a new API key with a stable identifier and secret."""
    key_id = secrets.token_hex(8)
    secret = secrets.token_urlsafe(24)
    token = f"{API_KEY_PREFIX}_{key_id}_{secret}"
    preview = f"{API_KEY_PREFIX}_{key_id}_{secret[:4]}..."
    return GeneratedAPIKey(
        key_id=key_id,
        secret=secret,
        token=token,
        preview=preview,
    )


def parse_api_key(token: str) -> "ParsedAPIKey | None":
    """Split a GPDB API key into its identifier and secret fragment."""
    prefix = f"{API_KEY_PREFIX}_"
    if not token.startswith(prefix):
        return None
    remainder = token[len(prefix) :]
    try:
        key_id, secret = remainder.split("_", 1)
    except ValueError:
        return None
    if not key_id or not secret:
        return None
    return ParsedAPIKey(key_id=key_id, secret=secret)


def parse_provided_api_key(token: str) -> "GeneratedAPIKey | None":
    """Parse a provided API key into its components for storage.
    
    Returns None if the token is not a valid GPDB API key format.
    """
    parsed = parse_api_key(token)
    if parsed is None:
        return None
    preview = f"{API_KEY_PREFIX}_{parsed.key_id}_{parsed.secret[:4]}..."
    return GeneratedAPIKey(
        key_id=parsed.key_id,
        secret=parsed.secret,
        token=token,
        preview=preview,
    )


def extract_bearer_token(header_value: str | None) -> str | None:
    """Return a bearer token from an Authorization header if present."""
    if not header_value:
        return None
    scheme, _, token = header_value.partition(" ")
    if scheme.lower() != "bearer" or not token:
        return None
    return token


def _hash_pbkdf2_secret(secret: str) -> str:
    """Return a salted PBKDF2 hash for a secret value."""
    salt = os.urandom(16)
    digest = hashlib.pbkdf2_hmac(
        "sha256",
        secret.encode("utf-8"),
        salt,
        _PASSWORD_ITERATIONS,
    )
    salt_text = base64.urlsafe_b64encode(salt).decode("ascii")
    digest_text = base64.urlsafe_b64encode(digest).decode("ascii")
    return f"{_PASSWORD_ALGORITHM}${_PASSWORD_ITERATIONS}${salt_text}${digest_text}"


def verify_password(password: str, stored_hash: str) -> bool:
    """Verify a password against a stored PBKDF2 hash."""
    return _verify_pbkdf2_secret(password, stored_hash)


def _verify_pbkdf2_secret(secret: str, stored_hash: str) -> bool:
    """Verify a secret against a stored PBKDF2 hash."""
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
        secret.encode("utf-8"),
        salt,
        iterations,
    )
    return hmac.compare_digest(actual_digest, expected_digest)


@dataclass(frozen=True)
class GeneratedAPIKey:
    """A newly generated API key that can be stored and shown to the user."""

    key_id: str
    secret: str
    token: str
    preview: str


@dataclass(frozen=True)
class ParsedAPIKey:
    """The parsed identifier and secret for one API key."""

    key_id: str
    secret: str


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
