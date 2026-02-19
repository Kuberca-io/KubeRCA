"""Resource snapshot and cache data structures."""

from __future__ import annotations

import copy
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import cast


class CacheReadiness(StrEnum):
    """Cache completeness state."""

    READY = "ready"
    WARMING = "warming"
    PARTIALLY_READY = "partially_ready"
    DEGRADED = "degraded"


@dataclass(frozen=True)
class ResourceSnapshot:
    """Snapshot of a K8s resource spec at a point in time.

    Used by the Change Ledger for deployment diff computation.
    """

    kind: str
    namespace: str
    name: str
    spec_hash: str
    spec: dict[str, object]
    captured_at: datetime
    resource_version: str


@dataclass(frozen=True)
class FieldChange:
    """A single field change between two resource snapshots.

    Uses JSONPath-style field paths (e.g., spec.template.spec.containers[0].image).
    """

    field_path: str
    old_value: str | None
    new_value: str | None
    changed_at: datetime


@dataclass
class CachedResource:
    """In-memory resource cache entry, maintained by watch streams.

    Stores raw data internally for diff accuracy. Exposes only a redacted view
    to consumers (rule engine, LLM analyzer, evidence assembly).
    """

    kind: str
    namespace: str
    name: str
    resource_version: str
    labels: dict[str, str] = field(default_factory=dict)
    annotations: dict[str, str] = field(default_factory=dict)
    spec: dict[str, object] = field(default_factory=dict)
    status: dict[str, object] = field(default_factory=dict)
    last_updated: datetime = field(default_factory=datetime.utcnow)
    _redacted_view: CachedResourceView | None = field(default=None, repr=False, compare=False)

    def redacted_view(self) -> CachedResourceView:
        """Return a redacted copy safe for rules, LLM, and evidence assembly.

        Computed once per watch event (on ADDED/MODIFIED), cached for reads.
        """
        if self._redacted_view is not None:
            return self._redacted_view

        from kuberca.cache.redaction import redact_dict

        view = CachedResourceView(
            kind=self.kind,
            namespace=self.namespace,
            name=self.name,
            resource_version=self.resource_version,
            labels=copy.deepcopy(self.labels),
            annotations=cast(
                dict[str, str],
                redact_dict(copy.deepcopy(cast(dict[str, object], self.annotations)), is_annotation=True),
            ),
            spec=redact_dict(copy.deepcopy(self.spec)),
            status=redact_dict(copy.deepcopy(self.status)),
            last_updated=self.last_updated,
        )
        self._redacted_view = view
        return view

    def invalidate_view(self) -> None:
        """Invalidate cached redacted view after mutation."""
        self._redacted_view = None


@dataclass(frozen=True)
class CachedResourceView:
    """Immutable, redacted view of a cached resource.

    Rules and LLM analyzer receive ONLY this type, never CachedResource.
    """

    kind: str
    namespace: str
    name: str
    resource_version: str
    labels: dict[str, str]
    annotations: dict[str, str]
    spec: dict[str, object]
    status: dict[str, object]
    last_updated: datetime
