"""Rule auto-registration and public API for the rule engine module.

Auto-discovers all Rule subclasses from the r*.py files in this package.
Tier 1 rules (R01–R03) are always registered. Tier 2 rules (R04–R18) are
registered only when tier2_enabled=True (default per §4.5).

Usage::

    from kuberca.rules import build_rule_engine
    from kuberca.rules.base import RuleEngine

    engine = build_rule_engine(cache, ledger, tier2_enabled=True)

Or for manual control::

    from kuberca.rules.base import RuleEngine
    from kuberca.rules.r01_oom_killed import OOMKilledRule

    engine = RuleEngine(cache, ledger)
    engine.register(OOMKilledRule())
"""

from __future__ import annotations

import importlib
import inspect
import pkgutil
from pathlib import Path

from kuberca.observability.logging import get_logger
from kuberca.rules.base import ChangeLedger, ResourceCache, Rule, RuleEngine

__all__ = [
    "Rule",
    "RuleEngine",
    "ResourceCache",
    "ChangeLedger",
    "build_rule_engine",
    "discover_rules",
]

_logger = get_logger("rule_engine.registry")

# Tier 1 rules ship unconditionally (§4.5 — these are ship-blocking).
_TIER1_RULE_IDS: frozenset[str] = frozenset(
    {
        "R01_oom_killed",
        "R02_crash_loop",
        "R03_failed_scheduling",
    }
)

# Tier 2 rules are gated by the tier2_enabled config flag.
_TIER2_RULE_IDS: frozenset[str] = frozenset(
    {
        "R04_image_pull",
        "R05_hpa_misbehavior",
        "R06_service_unreachable",
        "R07_config_drift",
        "R08_volume_mount",
        "R09_node_pressure",
        "R10_readiness_probe",
        "R11_failedmount_configmap",
        "R12_failedmount_secret",
        "R13_failedmount_pvc",
        "R14_failedmount_nfs",
        "R15_failedscheduling_node",
        "R16_exceed_quota",
        "R17_evicted",
        "R18_claim_lost",
    }
)


def discover_rules() -> list[type[Rule]]:
    """Discover all Rule subclasses from r*.py modules in this package.

    Imports each r*.py module and returns concrete (non-abstract) Rule
    subclasses in the order they are found. Order is deterministic:
    modules are processed lexicographically by filename; the RuleEngine
    will re-sort them by (priority, rule_id) on registration.

    Returns a list of Rule subclasses (not instances).
    """
    package_path = Path(__file__).parent
    rule_classes: list[type[Rule]] = []
    seen: set[str] = set()

    for module_info in sorted(pkgutil.iter_modules([str(package_path)]), key=lambda m: m.name):
        if not module_info.name.startswith("r") or not module_info.name[1:2].isdigit():
            continue
        module_name = f"kuberca.rules.{module_info.name}"
        try:
            module = importlib.import_module(module_name)
        except ImportError as exc:
            _logger.error(
                "rule_module_import_failed",
                module=module_name,
                error=str(exc),
            )
            continue

        for _, obj in inspect.getmembers(module, inspect.isclass):
            if (
                issubclass(obj, Rule)
                and obj is not Rule
                and not inspect.isabstract(obj)
                and hasattr(obj, "rule_id")
                and obj.rule_id not in seen
            ):
                rule_classes.append(obj)
                seen.add(obj.rule_id)

    _logger.info(
        "rule_discovery_complete",
        total=len(rule_classes),
        rule_ids=[cls.rule_id for cls in rule_classes],
    )
    return rule_classes


def build_rule_engine(
    cache: ResourceCache,
    ledger: ChangeLedger,
    *,
    tier2_enabled: bool = True,
) -> RuleEngine:
    """Construct a RuleEngine with all applicable rules registered.

    Tier 1 rules (R01–R03) are always registered.
    Tier 2 rules (R04–R18) are registered only when tier2_enabled=True.

    Args:
        cache: ResourceCache implementation to pass to the engine.
        ledger: ChangeLedger implementation to pass to the engine.
        tier2_enabled: Whether to load Tier 2 rules. Defaults to True.

    Returns:
        A fully configured RuleEngine with rules registered in priority order.
    """
    engine = RuleEngine(cache=cache, ledger=ledger)
    rule_classes = discover_rules()
    registered: list[str] = []
    skipped: list[str] = []

    for rule_cls in rule_classes:
        rule_id: str = rule_cls.rule_id
        is_tier1 = rule_id in _TIER1_RULE_IDS
        is_tier2 = rule_id in _TIER2_RULE_IDS

        if is_tier1 or is_tier2 and tier2_enabled:
            engine.register(rule_cls())
            registered.append(rule_id)
        elif is_tier2 and not tier2_enabled:
            skipped.append(rule_id)
        else:
            # Unknown tier — register defensively and log a warning.
            _logger.warning(
                "rule_unknown_tier",
                rule_id=rule_id,
                message="Rule not in tier1 or tier2 sets; registering defensively.",
            )
            engine.register(rule_cls())
            registered.append(rule_id)

    _logger.info(
        "rule_engine_built",
        registered=registered,
        skipped=skipped,
        tier2_enabled=tier2_enabled,
    )
    return engine
