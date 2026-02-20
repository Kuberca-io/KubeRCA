"""Unit tests for kuberca.rules.__init__ — discover_rules() and build_rule_engine().

Covers:
  - discover_rules(): auto-discovery, uniqueness, required attributes, ordering, return type
  - build_rule_engine(): tier gating, RuleEngine type, priority sort invariant
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from kuberca.rules import build_rule_engine, discover_rules
from kuberca.rules.base import Rule, RuleEngine

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_REQUIRED_ATTRIBUTES = (
    "rule_id",
    "display_name",
    "priority",
    "base_confidence",
    "resource_dependencies",
    "relevant_field_paths",
)

_EXPECTED_RULE_COUNT = 18

# All rule IDs that must exist — one per r*.py module.
_ALL_RULE_IDS = frozenset(
    {
        "R01_oom_killed",
        "R02_crash_loop",
        "R03_failed_scheduling",
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

_TIER1_RULE_IDS = frozenset({"R01_oom_killed", "R02_crash_loop", "R03_failed_scheduling"})


def _make_cache() -> MagicMock:
    return MagicMock()


def _make_ledger() -> MagicMock:
    return MagicMock()


# ---------------------------------------------------------------------------
# TestDiscoverRules
# ---------------------------------------------------------------------------


class TestDiscoverRules:
    """Tests for kuberca.rules.discover_rules()."""

    def test_returns_18_rules(self) -> None:
        """discover_rules() must return exactly 18 concrete Rule subclasses (R01-R18)."""
        rules = discover_rules()
        assert len(rules) == _EXPECTED_RULE_COUNT, (
            f"Expected {_EXPECTED_RULE_COUNT} rules, got {len(rules)}: {[cls.rule_id for cls in rules]}"
        )

    def test_no_duplicate_rule_ids(self) -> None:
        """Each returned class must have a unique rule_id.

        Duplicates indicate two classes sharing the same ID, which would cause
        the second to silently shadow the first during evaluation.
        """
        rules = discover_rules()
        rule_ids = [cls.rule_id for cls in rules]
        unique_ids = set(rule_ids)
        assert len(rule_ids) == len(unique_ids), (
            f"Duplicate rule_ids found: {[rid for rid in rule_ids if rule_ids.count(rid) > 1]}"
        )

    def test_all_have_required_attributes(self) -> None:
        """Every discovered class must declare all six required class-level attributes.

        Missing any attribute means the rule cannot be evaluated by the engine.
        """
        rules = discover_rules()
        for rule_cls in rules:
            for attr in _REQUIRED_ATTRIBUTES:
                assert hasattr(rule_cls, attr), (
                    f"Rule class {rule_cls.__name__} (rule_id={getattr(rule_cls, 'rule_id', '<missing>')}) "
                    f"is missing required attribute: {attr!r}"
                )

    def test_rule_ids_ordered(self) -> None:
        """Discovered set must contain R01 through R18 — no gap, no extra entry.

        This guards against a new rule being added without a canonical ID or an
        existing rule being renamed to an out-of-range identifier.
        """
        rules = discover_rules()
        found_ids = {cls.rule_id for cls in rules}
        assert found_ids == _ALL_RULE_IDS, (
            f"Missing: {_ALL_RULE_IDS - found_ids}  |  Extra: {found_ids - _ALL_RULE_IDS}"
        )

    def test_returns_classes_not_instances(self) -> None:
        """discover_rules() must return class objects, not pre-constructed instances.

        The caller (build_rule_engine) is responsible for instantiation so it can
        pass constructor arguments. Returning instances would bypass that contract.
        """
        rules = discover_rules()
        for item in rules:
            assert isinstance(item, type), (
                f"Expected a class (type), got an instance of {type(item).__name__}: {item!r}"
            )
            assert issubclass(item, Rule), f"{item!r} is a type but is not a subclass of Rule"


# ---------------------------------------------------------------------------
# TestBuildRuleEngine
# ---------------------------------------------------------------------------


class TestBuildRuleEngine:
    """Tests for kuberca.rules.build_rule_engine()."""

    def test_returns_rule_engine(self) -> None:
        """build_rule_engine() must return a RuleEngine instance.

        A non-RuleEngine return would silently break every call site that calls
        engine.evaluate() or engine.register().
        """
        engine = build_rule_engine(_make_cache(), _make_ledger())
        assert isinstance(engine, RuleEngine)

    def test_tier2_enabled_registers_all(self) -> None:
        """With tier2_enabled=True (the default), all 18 rules must be registered.

        Tier 2 rules are the majority of diagnostic coverage; omitting them would
        leave R04-R18 silently absent from evaluation.
        """
        engine = build_rule_engine(_make_cache(), _make_ledger(), tier2_enabled=True)
        assert len(engine._rules) == _EXPECTED_RULE_COUNT, (
            f"Expected {_EXPECTED_RULE_COUNT} registered rules, "
            f"got {len(engine._rules)}: {[r.rule_id for r in engine._rules]}"
        )

    def test_tier2_disabled_skips_tier2(self) -> None:
        """With tier2_enabled=False, only the 3 Tier 1 rules must be registered.

        Tier 2 rules are gated by the tier2_enabled config flag (§4.5). This test
        confirms that disabling the flag actually excludes all 15 Tier 2 rules and
        does not accidentally include any of them.
        """
        engine = build_rule_engine(_make_cache(), _make_ledger(), tier2_enabled=False)
        registered_ids = {rule.rule_id for rule in engine._rules}
        assert len(engine._rules) == len(_TIER1_RULE_IDS), (
            f"Expected {len(_TIER1_RULE_IDS)} Tier 1 rules, "
            f"got {len(engine._rules)}: {[r.rule_id for r in engine._rules]}"
        )
        assert registered_ids == _TIER1_RULE_IDS, (
            f"Wrong rules registered with tier2_enabled=False. "
            f"Missing: {_TIER1_RULE_IDS - registered_ids}  |  "
            f"Unexpected: {registered_ids - _TIER1_RULE_IDS}"
        )

    def test_tier2_disabled_excludes_all_tier2_ids(self) -> None:
        """No Tier 2 rule ID must appear in the engine when tier2_enabled=False.

        Complements test_tier2_disabled_skips_tier2 by asserting the negative:
        none of R04-R18 should be registered.
        """
        engine = build_rule_engine(_make_cache(), _make_ledger(), tier2_enabled=False)
        registered_ids = {rule.rule_id for rule in engine._rules}
        tier2_ids = _ALL_RULE_IDS - _TIER1_RULE_IDS
        leaked = registered_ids & tier2_ids
        assert not leaked, f"Tier 2 rules leaked into tier2_enabled=False engine: {leaked}"

    def test_rules_sorted_by_priority(self) -> None:
        """Registered rules in engine._rules must be sorted by (priority, rule_id).

        The RuleEngine.register() method re-sorts on every call using that key.
        Evaluation correctness depends on the list order: lower-priority-value
        rules run first and can short-circuit the evaluation loop.
        """
        engine = build_rule_engine(_make_cache(), _make_ledger(), tier2_enabled=True)
        rules = engine._rules
        assert rules, "engine._rules is empty — no rules were registered"
        sort_keys = [(rule.priority, rule.rule_id) for rule in rules]
        assert sort_keys == sorted(sort_keys), (
            f"Rules are not in sorted (priority, rule_id) order. Actual order: {sort_keys}"
        )

    def test_each_registered_rule_is_a_rule_instance(self) -> None:
        """Every item in engine._rules must be an instance of Rule, not a class.

        build_rule_engine must instantiate each class (rule_cls()) before
        registering. Passing the class itself would cause AttributeError when the
        engine calls instance methods such as match(), correlate(), explain().
        """
        engine = build_rule_engine(_make_cache(), _make_ledger(), tier2_enabled=True)
        for item in engine._rules:
            assert isinstance(item, Rule), f"Expected a Rule instance, got {type(item)!r}: {item!r}"
            assert not isinstance(item, type), f"Rule class {item!r} was registered as a class, not an instance"

    def test_cache_and_ledger_passed_to_engine(self) -> None:
        """The cache and ledger passed to build_rule_engine must be stored on the engine.

        The engine uses self._cache and self._ledger in correlate() calls; if
        build_rule_engine silently ignored the arguments, every rule would operate
        on a different (possibly null) cache/ledger.
        """
        cache = _make_cache()
        ledger = _make_ledger()
        engine = build_rule_engine(cache, ledger)
        assert engine._cache is cache, "engine._cache does not match the cache argument"
        assert engine._ledger is ledger, "engine._ledger does not match the ledger argument"

    @pytest.mark.parametrize("tier2_enabled", [True, False])
    def test_tier1_rules_always_registered(self, tier2_enabled: bool) -> None:
        """R01, R02, R03 must be present regardless of the tier2_enabled flag.

        These are the ship-blocking Tier 1 rules (§4.5). Excluding them under
        any configuration would silently disable OOMKilled, CrashLoop, and
        FailedScheduling detection.
        """
        engine = build_rule_engine(_make_cache(), _make_ledger(), tier2_enabled=tier2_enabled)
        registered_ids = {rule.rule_id for rule in engine._rules}
        for rule_id in _TIER1_RULE_IDS:
            assert rule_id in registered_ids, (
                f"Tier 1 rule {rule_id!r} is missing from engine (tier2_enabled={tier2_enabled})"
            )
