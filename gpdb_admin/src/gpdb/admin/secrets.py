"""Helpers for encrypting reversible admin-managed secrets."""

from __future__ import annotations

import base64
import hashlib

from cryptography.fernet import Fernet, InvalidToken

_SECRET_PREFIX = "fernet:"


class SecretCipher:
    """Encrypt and decrypt admin-managed secrets with a stable app key."""

    def __init__(self, secret: str):
        key = base64.urlsafe_b64encode(hashlib.sha256(secret.encode("utf-8")).digest())
        self._fernet = Fernet(key)

    def encrypt(self, value: str | None) -> str | None:
        """Encrypt a secret value for storage."""
        if value in {None, ""}:
            return None
        token = self._fernet.encrypt(value.encode("utf-8")).decode("ascii")
        return f"{_SECRET_PREFIX}{token}"

    def decrypt(self, value: str | None) -> str | None:
        """Decrypt a stored secret value.

        Untagged values are treated as legacy plaintext so older records
        continue to work until they are rewritten.
        """
        if value in {None, ""}:
            return None
        if not value.startswith(_SECRET_PREFIX):
            return value
        token = value[len(_SECRET_PREFIX) :]
        try:
            return self._fernet.decrypt(token.encode("ascii")).decode("utf-8")
        except InvalidToken as exc:
            raise ValueError("Stored instance secret could not be decrypted") from exc
