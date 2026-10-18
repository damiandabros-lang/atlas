"""Device credential resolution.

Credentials are resolved at call time and never persisted in PostgreSQL. Providers (selected by
``Settings.device_credentials_ref``):

* ``env``        — ``ATLAS_DEVICE_USERNAME`` / ``ATLAS_DEVICE_PASSWORD`` (dev / CI).
* ``k8s-secret`` — a Kubernetes Secret mounted at ``/var/run/secrets/atlas/devices`` with one
  ``<hostname>.json`` file per device (``{"username": ..., "password": ...}``); falls back to
  the ``env`` values when the file is absent.
* ``vault``      — reserved for HashiCorp Vault / cloud secret managers (not implemented in MVP).
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from pydantic import SecretStr

from atlas.adapters.base import AdapterError, DeviceHandle
from atlas.core.config import Settings, get_settings

K8S_SECRET_DIR = Path("/var/run/secrets/atlas/devices")


class CredentialsError(AdapterError):
    """Credentials could not be resolved (misconfiguration — not a device fault)."""


@dataclass(slots=True)
class Credentials:
    username: str
    password: SecretStr

    def __repr__(self) -> str:  # never leak secrets into logs / tracebacks
        return f"Credentials(username={self.username!r}, password=SecretStr('**********'))"

    __str__ = __repr__

    def reveal(self) -> str:
        return self.password.get_secret_value()


def _from_env(settings: Settings) -> Credentials:
    return Credentials(username=settings.device_username, password=settings.device_password)


def _from_k8s_secret(handle: DeviceHandle, settings: Settings, secret_dir: Path) -> Credentials:
    path = secret_dir / f"{handle.hostname}.json"
    if not path.is_file():
        return _from_env(settings)
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return Credentials(username=str(data["username"]), password=SecretStr(str(data["password"])))
    except (OSError, ValueError, KeyError) as exc:
        raise CredentialsError(
            f"invalid credential secret for {handle.hostname}", device=handle.hostname
        ) from exc


def resolve_credentials(
    handle: DeviceHandle, settings: Settings | None = None, *, secret_dir: Path = K8S_SECRET_DIR
) -> Credentials:
    settings = settings or get_settings()
    provider = settings.device_credentials_ref.lower()
    if provider == "env":
        return _from_env(settings)
    if provider in {"k8s-secret", "k8s_secret", "k8s"}:
        return _from_k8s_secret(handle, settings, secret_dir)
    if provider == "vault":
        raise NotImplementedError("vault credential provider is not implemented in the MVP")
    raise CredentialsError(
        f"unknown credential provider '{settings.device_credentials_ref}'", device=handle.hostname
    )


__all__ = ["Credentials", "CredentialsError", "K8S_SECRET_DIR", "resolve_credentials"]
