"""Spec redaction engine for KubeRCA.

Removes or masks sensitive values from Kubernetes resource specs before
they are surfaced to rules, the LLM analyzer, or any external interface.

Redaction applies in three sequential passes:

1. Key-based denylist  -- any field whose key contains a denylist word is
   replaced with "[REDACTED]" unconditionally, regardless of the value.

2. Value-based heuristics -- for fields NOT already redacted and NOT on the
   safe allowlist, the value string is tested against several patterns:
     * JWT tokens (three dot-separated base64-url segments)
     * Long base64 strings (>64 chars of [A-Za-z0-9+/=])
     * URL-embedded tokens (?token=, &token=, ?access_token=, &api_key=)
     * Authorization header values
     * Bearer tokens (``Bearer `` prefix + >=20 chars)
   A match produces "[HASH:sha256:{first8}:{sha256hex}]" so the token can
   be correlated across redacted documents without exposure.

3. Field-specific rules -- env entries, command args, and annotation values
   each have dedicated handling documented below.
"""

from __future__ import annotations

import base64
import hashlib
import json
import re
from typing import Final

# ---------------------------------------------------------------------------
# Key denylist
# ---------------------------------------------------------------------------

_KEY_DENYLIST: Final[frozenset[str]] = frozenset(
    {
        "password",
        "token",
        "secret",
        "key",
        "credential",
        "authorization",
        "api_key",
        "apikey",
        "private_key",
        "access_key",
        "client_secret",
    }
)

# ---------------------------------------------------------------------------
# Safe field allowlist — exempted from value heuristics
# ---------------------------------------------------------------------------

_VALUE_HEURISTIC_ALLOWLIST: Final[frozenset[str]] = frozenset(
    {
        "image",
        "imageid",
        "containerid",
        "resourceversion",
        "uid",
        "selflink",
        "generation",
        "observedgeneration",
    }
)

# ---------------------------------------------------------------------------
# Annotation prefix allowlist — values NOT redacted when prefix matches
# ---------------------------------------------------------------------------

_ANNOTATION_SAFE_PREFIXES: Final[tuple[str, ...]] = (
    "app.kubernetes.io/",
    "kubernetes.io/",
    "helm.sh/",
)

# ---------------------------------------------------------------------------
# Command-arg secret patterns (prefix matching)
# ---------------------------------------------------------------------------

_COMMAND_ARG_SECRET_PREFIXES: Final[tuple[str, ...]] = (
    "--password=",
    "--token=",
    "--key=",
)

# ---------------------------------------------------------------------------
# Compiled value heuristic patterns
# ---------------------------------------------------------------------------

# JWT: three dot-separated base64url segments (header.payload.signature)
_RE_JWT: Final[re.Pattern[str]] = re.compile(r"^[A-Za-z0-9+/=_-]{10,}\.[A-Za-z0-9+/=_-]{10,}\.[A-Za-z0-9+/=_-]{10,}$")

# Long base64: >64 chars drawn exclusively from the base64 alphabet (std + url)
_RE_LONG_BASE64: Final[re.Pattern[str]] = re.compile(r"^[A-Za-z0-9+/=]{65,}$")

# URL-embedded token query parameters
_RE_URL_TOKEN: Final[re.Pattern[str]] = re.compile(r"[?&](?:token|access_token|api_key)=[^\s&\"'#]+")

# Authorization header value (anything that looks like an auth value)
_RE_AUTHORIZATION_HEADER: Final[re.Pattern[str]] = re.compile(
    r"^(?:Basic|Bearer|Token|Digest)\s+\S{8,}",
    re.IGNORECASE,
)

# Bearer token (starts with "Bearer " then >=20 non-whitespace characters)
_RE_BEARER: Final[re.Pattern[str]] = re.compile(
    r"^Bearer\s+\S{20,}$",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Long base64 threshold used in command-arg redaction (>64 chars)
# ---------------------------------------------------------------------------
_BASE64_CMD_THRESHOLD: Final[int] = 64


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _key_contains_denylist_word(key: str) -> bool:
    """Return True if *key* (case-insensitive) contains any denylist word."""
    lower = key.lower()
    return any(word in lower for word in _KEY_DENYLIST)


def _hash_value(value: str) -> str:
    """Return a deterministic hash token for a sensitive string value."""
    digest = hashlib.sha256(value.encode()).hexdigest()
    return f"[HASH:sha256:{digest[:8]}:{digest}]"


def _value_matches_heuristics(value: str) -> bool:
    """Return True if *value* looks like a secret by any heuristic."""
    if _RE_JWT.match(value):
        return True
    if _RE_LONG_BASE64.match(value):
        return True
    if _RE_URL_TOKEN.search(value):
        return True
    if _RE_AUTHORIZATION_HEADER.match(value):
        return True
    return bool(_RE_BEARER.match(value))


def _is_valid_base64_string(s: str) -> bool:
    """Return True if *s* is valid base64 (padded or not) and >64 chars."""
    if len(s) <= _BASE64_CMD_THRESHOLD:
        return False
    # Normalise padding and attempt decode
    padded = s + "=" * (-len(s) % 4)
    try:
        base64.b64decode(padded, validate=True)
        return True
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Public API — individual value redaction
# ---------------------------------------------------------------------------


def redact_value(key: str, value: object) -> object:
    """Redact a single key/value pair using the standard denylist and heuristics.

    Args:
        key:   The field name (used for key-based denylist lookup).
        value: The raw field value. Non-string values are returned unchanged
               unless the key matches the denylist.

    Returns:
        The original value, "[REDACTED]", or "[HASH:sha256:...]" depending on
        which redaction rule fires first.
    """
    # Pass 1: key denylist applies to any value type
    if _key_contains_denylist_word(key):
        return "[REDACTED]"

    # Non-string values cannot be tested against string heuristics
    if not isinstance(value, str):
        return value

    # Pass 2: value heuristics — skip allowlisted keys
    if key.lower() not in _VALUE_HEURISTIC_ALLOWLIST and _value_matches_heuristics(value):
        return _hash_value(value)

    return value


# ---------------------------------------------------------------------------
# Env-entry redaction
# ---------------------------------------------------------------------------


def _redact_env_entry(entry: object) -> object:
    """Redact a single environment variable entry.

    Kubernetes env entries are dicts with ``name`` and optionally ``value``
    or ``valueFrom``. We keep the ``name`` key intact (it identifies the
    variable) but replace ``value`` unconditionally with "[REDACTED]" — the
    caller is only given names, not values.
    """
    if not isinstance(entry, dict):
        return entry

    result: dict[str, object] = {}
    for k, v in entry.items():
        if k == "value":
            result[k] = "[REDACTED]"
        elif k == "valueFrom":
            # valueFrom sub-objects (secretKeyRef, configMapKeyRef, etc.)
            # expose secret references; redact the entire sub-tree
            result[k] = "[REDACTED]"
        else:
            result[k] = v
    return result


# ---------------------------------------------------------------------------
# Command-arg redaction
# ---------------------------------------------------------------------------


def _redact_command_arg(arg: str) -> str:
    """Redact a single command argument if it matches a secret pattern."""
    for prefix in _COMMAND_ARG_SECRET_PREFIXES:
        if arg.lower().startswith(prefix):
            return prefix + "[REDACTED]"

    # Bare base64 argument longer than 64 chars
    stripped = arg.strip()
    if _is_valid_base64_string(stripped) and len(stripped) > _BASE64_CMD_THRESHOLD:
        return "[REDACTED]"

    return arg


# ---------------------------------------------------------------------------
# Annotation redaction
# ---------------------------------------------------------------------------


def _redact_annotation_value(key: str, value: str) -> str:
    """Redact a single annotation value.

    Values whose key starts with a safe prefix are passed through unchanged.
    All other annotation values are replaced with "[REDACTED]".
    """
    for prefix in _ANNOTATION_SAFE_PREFIXES:
        if key.startswith(prefix):
            return value
    return "[REDACTED]"


# ---------------------------------------------------------------------------
# Deep dict redaction
# ---------------------------------------------------------------------------


def redact_dict(data: dict[str, object], *, is_annotation: bool = False) -> dict[str, object]:
    """Recursively redact sensitive values from a Kubernetes spec dict.

    The function mutates the *data* dict in-place and also returns it,
    so callers that pass ``copy.deepcopy(spec)`` can chain the call.

    Args:
        data:          The dict to redact. Must be a ``dict``; nested
                       ``dict`` and ``list`` values are processed recursively.
        is_annotation: When ``True`` the dict is treated as a flat map of
                       annotation key → value and annotation-specific rules
                       apply instead of the standard recursive rules.

    Returns:
        The (mutated) *data* dict.
    """
    if is_annotation:
        return _redact_annotations(data)

    return _redact_recursive(data)


# ---------------------------------------------------------------------------
# Internal recursive helpers
# ---------------------------------------------------------------------------


def _redact_annotations(annotations: dict[str, object]) -> dict[str, object]:
    """Apply annotation-specific redaction to a flat annotations dict."""
    for key, value in annotations.items():
        if isinstance(value, str):
            annotations[key] = _redact_annotation_value(key, value)
        elif value is not None:
            # Non-string annotation values are unusual but present in dry-run
            # objects; redact them unless they can be shown safe via prefix
            is_safe = any(key.startswith(p) for p in _ANNOTATION_SAFE_PREFIXES)
            if not is_safe:
                annotations[key] = "[REDACTED]"
    return annotations


def _redact_recursive(data: dict[str, object]) -> dict[str, object]:
    """Walk *data* recursively applying all redaction rules."""
    for key, value in data.items():
        # --- Special handling for container env arrays ---
        if key == "env" and isinstance(value, list):
            data[key] = [_redact_env_entry(entry) for entry in value]
            continue

        # --- Special handling for command / args arrays ---
        if key in {"command", "args"} and isinstance(value, list):
            data[key] = [_redact_command_arg(arg) if isinstance(arg, str) else arg for arg in value]
            continue

        # --- Key denylist (applies before descending into nested dicts) ---
        if _key_contains_denylist_word(key):
            data[key] = "[REDACTED]"
            continue

        # --- Recurse into nested structures ---
        if isinstance(value, dict):
            data[key] = _redact_recursive(value)
            continue

        if isinstance(value, list):
            data[key] = _redact_list(key, value)
            continue

        # --- Leaf value: apply heuristics ---
        if (
            isinstance(value, str)
            and key.lower() not in _VALUE_HEURISTIC_ALLOWLIST
            and _value_matches_heuristics(value)
        ):
            data[key] = _hash_value(value)

    return data


def _redact_list(parent_key: str, items: list[object]) -> list[object]:
    """Redact elements in a list, recursing into nested dicts."""
    result: list[object] = []
    for item in items:
        if isinstance(item, dict):
            result.append(_redact_recursive(item))
        elif isinstance(item, list):
            result.append(_redact_list(parent_key, item))
        elif isinstance(item, str) and parent_key.lower() not in _VALUE_HEURISTIC_ALLOWLIST:
            if _key_contains_denylist_word(parent_key):
                result.append("[REDACTED]")
            elif _value_matches_heuristics(item):
                result.append(_hash_value(item))
            else:
                result.append(item)
        else:
            result.append(item)
    return result


# ---------------------------------------------------------------------------
# Convenience: JSON-safe serialisation of a redacted spec
# ---------------------------------------------------------------------------


def redacted_json(data: dict[str, object]) -> str:
    """Return a JSON string of *data* after applying full redaction.

    Useful for logging and evidence assembly where a compact, safe
    representation is required.
    """
    import copy

    safe = redact_dict(copy.deepcopy(data))
    return json.dumps(safe, default=str)
