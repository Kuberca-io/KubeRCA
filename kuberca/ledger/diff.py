"""Recursive JSON diff algorithm for Kubernetes resource specs.

Produces a flat list of :class:`~kuberca.models.resources.FieldChange` objects
from two spec dicts.  Every changed leaf value gets its own entry with a
full dotted JSONPath (e.g. ``spec.template.spec.containers[0].image``).

Array elements are addressed by their zero-based index so that the path
is unambiguous and reproducible across diff calls on the same spec version.
Values are JSON-serialised to strings so that ``FieldChange.old_value`` and
``FieldChange.new_value`` are always ``str | None`` and safe to embed in
evidence summaries without further type-checking.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import cast

from kuberca.models.resources import FieldChange

# Recursive value types that appear in a Kubernetes spec
_SpecValue = dict[str, object] | list[object] | str | int | float | bool | None


def compute_diff(old_spec: dict[str, object], new_spec: dict[str, object]) -> list[FieldChange]:
    """Compute the recursive diff between two resource spec dicts.

    Both dicts should represent the *same* resource at different points in
    time (e.g. two consecutive ResourceSnapshot.spec values).  The function
    walks both trees in parallel and emits a :class:`FieldChange` for every
    leaf where the value has changed, been added, or been removed.

    Args:
        old_spec: The earlier spec dict (or an empty dict for a new resource).
        new_spec: The later spec dict (or an empty dict for a deleted resource).

    Returns:
        A list of :class:`FieldChange` objects sorted by ``field_path``.
        An empty list means the two specs are identical.
    """
    changed_at = datetime.utcnow()
    changes: list[FieldChange] = []
    _diff_values(old_spec, new_spec, path="", changes=changes, changed_at=changed_at)
    changes.sort(key=lambda fc: fc.field_path)
    return changes


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _json_str(value: _SpecValue) -> str:
    """Serialise *value* to a compact JSON string for FieldChange storage."""
    return json.dumps(value, separators=(",", ":"), default=str)


def _diff_values(
    old_val: _SpecValue,
    new_val: _SpecValue,
    path: str,
    changes: list[FieldChange],
    changed_at: datetime,
) -> None:
    """Recursively compare *old_val* and *new_val*, appending to *changes*."""
    # Both are dicts — recurse into merged key set
    if isinstance(old_val, dict) and isinstance(new_val, dict):
        _diff_dicts(old_val, new_val, path=path, changes=changes, changed_at=changed_at)
        return

    # Both are lists — recurse by index
    if isinstance(old_val, list) and isinstance(new_val, list):
        _diff_lists(old_val, new_val, path=path, changes=changes, changed_at=changed_at)
        return

    # One side is a complex type and the other is not (structural type change)
    # or both are primitives — emit a leaf change if they differ
    if old_val != new_val:
        changes.append(
            FieldChange(
                field_path=path,
                old_value=_json_str(old_val) if old_val is not None else None,
                new_value=_json_str(new_val) if new_val is not None else None,
                changed_at=changed_at,
            )
        )


def _diff_dicts(
    old_dict: dict[str, object],
    new_dict: dict[str, object],
    path: str,
    changes: list[FieldChange],
    changed_at: datetime,
) -> None:
    """Diff two dicts and recurse into common keys."""
    all_keys = old_dict.keys() | new_dict.keys()
    for key in sorted(all_keys):
        child_path = f"{path}.{key}" if path else key
        if key not in old_dict:
            # Added in new_spec — treat old as None for the leaf / subtree
            _diff_values(
                None,
                cast(_SpecValue, new_dict[key]),
                path=child_path,
                changes=changes,
                changed_at=changed_at,
            )
        elif key not in new_dict:
            # Removed in new_spec — treat new as None
            _diff_values(
                cast(_SpecValue, old_dict[key]),
                None,
                path=child_path,
                changes=changes,
                changed_at=changed_at,
            )
        else:
            _diff_values(
                cast(_SpecValue, old_dict[key]),
                cast(_SpecValue, new_dict[key]),
                path=child_path,
                changes=changes,
                changed_at=changed_at,
            )


def _diff_lists(
    old_list: list[object],
    new_list: list[object],
    path: str,
    changes: list[FieldChange],
    changed_at: datetime,
) -> None:
    """Diff two lists by index.

    Elements beyond the length of the shorter list are treated as added
    (old side None) or removed (new side None) respectively.
    """
    max_len = max(len(old_list), len(new_list))
    for i in range(max_len):
        child_path = f"{path}[{i}]"
        old_item: _SpecValue = cast(_SpecValue, old_list[i]) if i < len(old_list) else None
        new_item: _SpecValue = cast(_SpecValue, new_list[i]) if i < len(new_list) else None
        _diff_values(old_item, new_item, path=child_path, changes=changes, changed_at=changed_at)
