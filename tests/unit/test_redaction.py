"""Tests for kuberca.cache.redaction."""

from __future__ import annotations

import hashlib

from kuberca.cache.redaction import (
    _hash_value,
    redact_dict,
    redact_value,
)

# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def _expected_hash(value: str) -> str:
    digest = hashlib.sha256(value.encode()).hexdigest()
    return f"[HASH:sha256:{digest[:8]}:{digest}]"


# ---------------------------------------------------------------------------
# Key denylist tests
# ---------------------------------------------------------------------------


class TestKeyDenylist:
    def test_password_key_redacted(self) -> None:
        result = redact_value("password", "mysecret")
        assert result == "[REDACTED]"

    def test_token_key_redacted(self) -> None:
        result = redact_value("token", "abc123")
        assert result == "[REDACTED]"

    def test_secret_key_redacted(self) -> None:
        result = redact_value("secret", "topsecret")
        assert result == "[REDACTED]"

    def test_key_key_redacted(self) -> None:
        result = redact_value("key", "some-key-value")
        assert result == "[REDACTED]"

    def test_credential_key_redacted(self) -> None:
        result = redact_value("credential", "cred123")
        assert result == "[REDACTED]"

    def test_authorization_key_redacted(self) -> None:
        result = redact_value("authorization", "Bearer xyz")
        assert result == "[REDACTED]"

    def test_api_key_key_redacted(self) -> None:
        result = redact_value("api_key", "sk-abc123")
        assert result == "[REDACTED]"

    def test_apikey_key_redacted(self) -> None:
        result = redact_value("apikey", "sk-abc123")
        assert result == "[REDACTED]"

    def test_private_key_key_redacted(self) -> None:
        result = redact_value("private_key", "-----BEGIN RSA PRIVATE KEY-----")
        assert result == "[REDACTED]"

    def test_access_key_key_redacted(self) -> None:
        result = redact_value("access_key", "AKIAIOSFODNN7EXAMPLE")
        assert result == "[REDACTED]"

    def test_client_secret_key_redacted(self) -> None:
        result = redact_value("client_secret", "abc")
        assert result == "[REDACTED]"

    def test_case_insensitive_key(self) -> None:
        assert redact_value("PASSWORD", "x") == "[REDACTED]"
        assert redact_value("DB_Password", "x") == "[REDACTED]"
        assert redact_value("API_KEY_NAME", "x") == "[REDACTED]"

    def test_safe_key_passes_through(self) -> None:
        assert redact_value("image", "nginx:1.25") == "nginx:1.25"
        assert redact_value("namespace", "default") == "default"
        assert redact_value("name", "my-pod") == "my-pod"


# ---------------------------------------------------------------------------
# Value heuristics — JWT
# ---------------------------------------------------------------------------


class TestJWTHeuristic:
    _VALID_JWT = (
        "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9"
        ".eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIn0"
        ".SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c"
    )

    def test_jwt_value_hashed(self) -> None:
        result = redact_value("config", self._VALID_JWT)
        assert result == _expected_hash(self._VALID_JWT)

    def test_jwt_not_hashed_on_allowlisted_key(self) -> None:
        result = redact_value("image", self._VALID_JWT)
        assert result == self._VALID_JWT


# ---------------------------------------------------------------------------
# Value heuristics — long base64
# ---------------------------------------------------------------------------


class TestLongBase64Heuristic:
    # 65 chars of valid base64 alphabet
    _LONG_B64 = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"

    def test_long_base64_hashed(self) -> None:
        result = redact_value("data", self._LONG_B64)
        assert result == _expected_hash(self._LONG_B64)

    def test_64_chars_not_hashed(self) -> None:
        # Exactly 64 chars — should NOT be flagged
        val = "A" * 64
        result = redact_value("data", val)
        assert result == val

    def test_long_base64_not_hashed_on_allowlisted_key(self) -> None:
        result = redact_value("uid", self._LONG_B64)
        assert result == self._LONG_B64


# ---------------------------------------------------------------------------
# Value heuristics — Bearer token
# ---------------------------------------------------------------------------


class TestBearerTokenHeuristic:
    def test_bearer_token_hashed(self) -> None:
        token = "Bearer " + "x" * 20
        result = redact_value("custom_header", token)
        assert result == _expected_hash(token)

    def test_bearer_too_short_not_hashed(self) -> None:
        token = "Bearer short"
        result = redact_value("config", token)
        assert result == token

    def test_bearer_case_insensitive(self) -> None:
        token = "BEARER " + "y" * 25
        result = redact_value("config", token)
        assert result == _expected_hash(token)


# ---------------------------------------------------------------------------
# Value heuristics — URL-embedded tokens
# ---------------------------------------------------------------------------


class TestURLTokenHeuristic:
    def test_url_with_token_param_hashed(self) -> None:
        url = "https://example.com/api?token=abc123secretvalue"
        result = redact_value("endpoint", url)
        assert result == _expected_hash(url)

    def test_url_with_access_token_hashed(self) -> None:
        url = "https://example.com?access_token=mysecrettoken"
        result = redact_value("endpoint", url)
        assert result == _expected_hash(url)

    def test_url_with_api_key_hashed(self) -> None:
        url = "https://api.example.com/v1/data?api_key=sk-test123"
        result = redact_value("endpoint", url)
        assert result == _expected_hash(url)

    def test_plain_url_not_hashed(self) -> None:
        url = "https://example.com/api/v1/pods"
        result = redact_value("endpoint", url)
        assert result == url


# ---------------------------------------------------------------------------
# redact_dict — standard spec
# ---------------------------------------------------------------------------


class TestRedactDict:
    def test_nested_password_redacted(self) -> None:
        spec: dict[str, object] = {"db": {"password": "hunter2", "host": "localhost"}}
        result = redact_dict(spec)
        assert result["db"]["password"] == "[REDACTED]"  # type: ignore[index]
        assert result["db"]["host"] == "localhost"  # type: ignore[index]

    def test_env_values_redacted(self) -> None:
        spec: dict[str, object] = {
            "containers": [
                {
                    "env": [
                        {"name": "DB_HOST", "value": "localhost"},
                        {"name": "DB_PASSWORD", "value": "secret123"},
                    ]
                }
            ]
        }
        result = redact_dict(spec)
        container = result["containers"][0]  # type: ignore[index]
        env = container["env"]  # type: ignore[index]
        # All env values replaced with [REDACTED]
        assert env[0]["value"] == "[REDACTED]"  # type: ignore[index]
        assert env[1]["value"] == "[REDACTED]"  # type: ignore[index]
        # Names preserved
        assert env[0]["name"] == "DB_HOST"  # type: ignore[index]
        assert env[1]["name"] == "DB_PASSWORD"  # type: ignore[index]

    def test_command_args_secret_prefix_redacted(self) -> None:
        spec: dict[str, object] = {
            "containers": [
                {
                    "command": ["myapp"],
                    "args": ["--password=hunter2", "--port=8080", "--token=abc123"],
                }
            ]
        }
        result = redact_dict(spec)
        container = result["containers"][0]  # type: ignore[index]
        args = container["args"]  # type: ignore[index]
        assert args[0] == "--password=[REDACTED]"
        assert args[1] == "--port=8080"
        assert args[2] == "--token=[REDACTED]"

    def test_image_field_not_redacted(self) -> None:
        # A JWT-length image tag should NOT be redacted
        spec: dict[str, object] = {"image": "nginx:stable-alpine"}
        result = redact_dict(spec)
        assert result["image"] == "nginx:stable-alpine"

    def test_safe_allowlist_fields_not_hashed(self) -> None:
        spec: dict[str, object] = {
            "uid": "a" * 70,  # long base64-like but allowlisted
            "resourceVersion": "12345",
        }
        result = redact_dict(spec)
        assert result["uid"] == "a" * 70
        assert result["resourceVersion"] == "12345"


# ---------------------------------------------------------------------------
# redact_dict — annotation mode
# ---------------------------------------------------------------------------


class TestRedactDictAnnotations:
    def test_safe_annotation_prefixes_pass_through(self) -> None:
        annotations: dict[str, object] = {
            "app.kubernetes.io/name": "my-app",
            "kubernetes.io/managed-by": "helm",
            "helm.sh/chart": "my-chart-1.0",
        }
        result = redact_dict(annotations, is_annotation=True)
        assert result["app.kubernetes.io/name"] == "my-app"
        assert result["kubernetes.io/managed-by"] == "helm"
        assert result["helm.sh/chart"] == "my-chart-1.0"

    def test_unknown_annotation_redacted(self) -> None:
        annotations: dict[str, object] = {
            "custom.corp.io/secret-value": "topsecret",
            "app.kubernetes.io/version": "1.0.0",
        }
        result = redact_dict(annotations, is_annotation=True)
        assert result["custom.corp.io/secret-value"] == "[REDACTED]"
        assert result["app.kubernetes.io/version"] == "1.0.0"

    def test_non_string_unsafe_annotation_redacted(self) -> None:
        annotations: dict[str, object] = {
            "corp.io/config": {"nested": "value"},
        }
        result = redact_dict(annotations, is_annotation=True)
        assert result["corp.io/config"] == "[REDACTED]"

    def test_non_string_safe_annotation_not_redacted(self) -> None:
        annotations: dict[str, object] = {
            "app.kubernetes.io/ports": [8080, 9090],
        }
        result = redact_dict(annotations, is_annotation=True)
        assert result["app.kubernetes.io/ports"] == [8080, 9090]


# ---------------------------------------------------------------------------
# Hash function determinism
# ---------------------------------------------------------------------------


class TestHashValue:
    def test_deterministic(self) -> None:
        assert _hash_value("secret") == _hash_value("secret")

    def test_different_values_different_hashes(self) -> None:
        assert _hash_value("a") != _hash_value("b")

    def test_format(self) -> None:
        result = _hash_value("test")
        assert result.startswith("[HASH:sha256:")
        assert result.endswith("]")
        parts = result[len("[HASH:sha256:") : -1].split(":")
        assert len(parts) == 2
        assert len(parts[0]) == 8  # first8
        assert len(parts[1]) == 64  # full sha256 hex
