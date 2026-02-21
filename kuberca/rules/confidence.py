"""Confidence scoring for rule engine results.

The formula is deterministic and explainable. base_confidence varies per rule;
the bonus structure is identical for all rules. Hard cap: 0.95.
"""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING

from kuberca.observability.logging import get_logger
from kuberca.observability.metrics import invariant_violations_total

if TYPE_CHECKING:
    from kuberca.models.analysis import CorrelationResult
    from kuberca.models.events import EventRecord
    from kuberca.rules.base import Rule

_logger = get_logger("confidence")

_WITHIN_30_MIN = timedelta(minutes=30)


def any_within_30min(corr: CorrelationResult, event: EventRecord) -> bool:
    """Return True if any rule-relevant change occurred within 30 minutes of the event.

    A change is "within 30 minutes" when its changed_at timestamp falls inside
    [event.last_seen - 30min, event.last_seen + 30min]. The ± window accounts
    for clock skew between the collector and the change ledger.
    """
    for change in corr.changes:
        delta = abs((event.last_seen - change.changed_at).total_seconds())
        if delta <= _WITHIN_30_MIN.total_seconds():
            return True
    return False


def is_single_resource(corr: CorrelationResult) -> bool:
    """Return True when the correlation implicates exactly one related resource.

    A localized incident (one Deployment, one Pod) is more confidently diagnosed
    than a cluster-wide pattern that could have many causes. The rule earns the
    +0.05 bonus only when related_resources contains at most one entry.
    """
    return len(corr.related_resources) <= 1


def compute_confidence(
    rule: Rule,
    corr: CorrelationResult,
    event: EventRecord,
) -> float:
    """Compute a deterministic confidence score for a rule match.

    Formula (identical across all rules, only base_confidence varies):

        score = base_confidence
        + 0.15  if rule-relevant diffs were found in the correlation window
        + 0.10  if any relevant change occurred within 30 minutes of the incident
        + 0.05  if the event has been observed >= 3 times (repeated pattern)
        + 0.05  if the correlation implicates exactly one resource (localized)
        → capped at 0.95 (never 1.0)

    The +0.15 diff bonus requires *rule-relevant* diffs (filtered by the rule's
    relevant_field_paths inside correlate()). Any diff in the cluster MUST NOT
    inflate confidence — only causally-related field changes count.
    """
    score = rule.base_confidence

    if corr.changes:
        score += 0.15

    if any_within_30min(corr, event):
        score += 0.10

    if event.count >= 3:
        score += 0.05

    if is_single_resource(corr):
        score += 0.05

    result = min(score, 0.95)
    if score < 0.0 or score > 0.95:
        _logger.error(
            "invariant_violated",
            invariant="INV-C01_confidence_range",
            value=score,
            base_confidence=rule.base_confidence,
        )
        invariant_violations_total.labels(invariant_name="INV-C01_confidence_range").inc()
        result = max(0.0, min(result, 0.95))
    return result
