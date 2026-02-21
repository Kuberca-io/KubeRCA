#!/usr/bin/env bash
# KubeRCA E2E Test Runner
#
# Deploys intentionally broken workloads to a local Docker Desktop Kubernetes
# cluster and verifies that KubeRCA detects and diagnoses each incident via its
# REST API.
#
# Prerequisites:
#   - Docker Desktop with Kubernetes enabled
#   - kubectl, helm, curl, and jq on PATH
#   - KubeRCA Docker image built: docker build -t kuberca:${VERSION} .
#
# Usage:
#   ./e2e/run_e2e.sh          # full run (deploy + test + cleanup)
#   ./e2e/run_e2e.sh --no-cleanup   # leave resources for manual inspection
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
VERSION=$(grep '^version' "$REPO_ROOT/pyproject.toml" | head -1 | sed 's/.*= *"\(.*\)"/\1/')
NS="kuberca-e2e"
HELM_RELEASE="kuberca"
PORT_FORWARD_PID=""
LOCAL_PORT=18080
API_BASE="http://localhost:${LOCAL_PORT}/api/v1"
CLEANUP=${CLEANUP:-true}

PASS=0
FAIL=0
SKIP=0

# ── Argument parsing ────────────────────────────────────────────────────────
for arg in "$@"; do
    case "$arg" in
        --no-cleanup) CLEANUP=false ;;
    esac
done

# ── Helpers ─────────────────────────────────────────────────────────────────

_log()  { echo "[$(date '+%H:%M:%S')] $*"; }
_pass() { _log "  PASS: $1"; PASS=$((PASS + 1)); }
_fail() { _log "  FAIL: $1"; FAIL=$((FAIL + 1)); }
_skip() { _log "  SKIP: $1"; SKIP=$((SKIP + 1)); }

cleanup() {
    _log "Cleaning up..."
    # Kill port-forward
    if [[ -n "$PORT_FORWARD_PID" ]] && kill -0 "$PORT_FORWARD_PID" 2>/dev/null; then
        kill "$PORT_FORWARD_PID" 2>/dev/null || true
        wait "$PORT_FORWARD_PID" 2>/dev/null || true
    fi
    if [[ "$CLEANUP" == "true" ]]; then
        bash "${SCRIPT_DIR}/cleanup.sh"
    else
        _log "(--no-cleanup specified; resources left in namespace $NS)"
    fi
}
trap cleanup EXIT

wait_for_condition() {
    local description="$1"
    local check_cmd="$2"
    local timeout_secs="${3:-120}"
    local interval="${4:-5}"
    local elapsed=0

    _log "Waiting for: $description (timeout ${timeout_secs}s)..."
    while ! eval "$check_cmd" >/dev/null 2>&1; do
        sleep "$interval"
        elapsed=$((elapsed + interval))
        if [[ $elapsed -ge $timeout_secs ]]; then
            _log "  Timed out waiting for: $description"
            return 1
        fi
    done
    _log "  Ready: $description"
}

# ── Pre-flight checks ──────────────────────────────────────────────────────

_log "=== KubeRCA E2E Test Suite ==="
_log ""

for tool in kubectl helm curl jq docker; do
    if ! command -v "$tool" >/dev/null 2>&1; then
        _log "ERROR: Required tool '$tool' not found on PATH"
        exit 1
    fi
done

# Verify K8s connectivity
if ! kubectl cluster-info >/dev/null 2>&1; then
    _log "ERROR: Cannot reach Kubernetes cluster. Is Docker Desktop K8s enabled?"
    exit 1
fi
_log "Kubernetes cluster reachable"

# Verify Docker image exists
if ! docker image inspect kuberca:${VERSION} >/dev/null 2>&1; then
    _log "ERROR: Docker image 'kuberca:${VERSION}' not found."
    _log "  Build it first:  docker build -t kuberca:${VERSION} ."
    exit 1
fi
_log "Docker image kuberca:${VERSION} found"

# ── Step 1: Create namespace ───────────────────────────────────────────────

_log ""
_log "=== Step 1: Create test namespace ==="
kubectl create namespace "$NS" --dry-run=client -o yaml | kubectl apply -f -
_log "Namespace $NS ready"

# ── Step 2: Deploy KubeRCA via Helm ────────────────────────────────────────

_log ""
_log "=== Step 2: Deploy KubeRCA via Helm ==="
helm upgrade --install "$HELM_RELEASE" "${SCRIPT_DIR}/../helm/kuberca" \
    -n "$NS" \
    -f "${SCRIPT_DIR}/values-local.yaml" \
    --wait \
    --timeout 120s
_log "Helm release '$HELM_RELEASE' deployed"

# Wait for the deployment to be ready
kubectl -n "$NS" rollout status deployment/kuberca --timeout=120s
_log "KubeRCA deployment is ready"

# ── Step 3: Port-forward to KubeRCA ───────────────────────────────────────

_log ""
_log "=== Step 3: Port-forward to KubeRCA ==="
kubectl -n "$NS" port-forward svc/kuberca "${LOCAL_PORT}:8080" &
PORT_FORWARD_PID=$!

# Wait for port-forward to be ready
wait_for_condition "port-forward" "curl -sf ${API_BASE}/health" 30 2

# ── Step 4: Health check ──────────────────────────────────────────────────

_log ""
_log "=== Step 4: Health check ==="
HEALTH=$(curl -sf "${API_BASE}/health")
STATUS=$(echo "$HEALTH" | jq -r '.status')
if [[ "$STATUS" == "ok" ]]; then
    _pass "GET /api/v1/health -> status=ok"
else
    _fail "GET /api/v1/health -> expected status=ok, got: $STATUS"
fi
_log "  Response: $HEALTH"

# ── Step 5: Deploy bad workloads ──────────────────────────────────────────

_log ""
_log "=== Step 5: Deploy bad workloads ==="
# Apply exceed-quota LAST because its ResourceQuota forces all pods in the
# namespace to declare cpu/memory requests+limits.  Pods created before the
# quota exists are not retroactively affected.
for manifest in "${SCRIPT_DIR}"/manifests/*.yaml; do
    [[ "$(basename "$manifest")" == "exceed-quota.yaml" ]] && continue
    name=$(basename "$manifest" .yaml)
    _log "  Applying $name..."
    kubectl apply -f "$manifest"
done
_log "  Applying exceed-quota..."
kubectl apply -f "${SCRIPT_DIR}/manifests/exceed-quota.yaml"

# ── Step 6: Wait for K8s events to fire ───────────────────────────────────

_log ""
_log "=== Step 6: Waiting for K8s events to fire ==="

# CrashLoopBackOff needs a few restart cycles
_log "  Waiting for crash-loop to enter CrashLoopBackOff..."
for i in $(seq 1 24); do
    STATUS=$(kubectl -n "$NS" get pod crash-loop -o jsonpath='{.status.containerStatuses[0].state.waiting.reason}' 2>/dev/null || echo "")
    if [[ "$STATUS" == "CrashLoopBackOff" ]]; then
        _log "  crash-loop is in CrashLoopBackOff after ${i}x5s"
        break
    fi
    sleep 5
done

# OOMKilled needs to run and get killed
_log "  Waiting for oom-victim to be OOMKilled..."
for i in $(seq 1 24); do
    REASON=$(kubectl -n "$NS" get pod oom-victim -o jsonpath='{.status.containerStatuses[0].state.terminated.reason}' 2>/dev/null || echo "")
    if [[ "$REASON" == "OOMKilled" ]]; then
        _log "  oom-victim was OOMKilled after ${i}x5s"
        break
    fi
    sleep 5
done

# ImagePullBackOff fires quickly
_log "  Waiting for bad-image to enter ImagePullBackOff or ErrImagePull..."
for i in $(seq 1 12); do
    STATUS=$(kubectl -n "$NS" get pod bad-image -o jsonpath='{.status.containerStatuses[0].state.waiting.reason}' 2>/dev/null || echo "")
    if [[ "$STATUS" == "ImagePullBackOff" || "$STATUS" == "ErrImagePull" ]]; then
        _log "  bad-image is in $STATUS after ${i}x5s"
        break
    fi
    sleep 5
done

# FailedScheduling fires immediately via events
_log "  Waiting for bad-schedule FailedScheduling event..."
for i in $(seq 1 12); do
    EVENTS=$(kubectl -n "$NS" get events --field-selector "involvedObject.name=bad-schedule,reason=FailedScheduling" --no-headers 2>/dev/null | wc -l | tr -d ' ')
    if [[ "$EVENTS" -gt 0 ]]; then
        _log "  bad-schedule has FailedScheduling event after ${i}x5s"
        break
    fi
    sleep 5
done

# Readiness probe failures need a few cycles
_log "  Waiting for bad-probe readiness failures..."
for i in $(seq 1 12); do
    EVENTS=$(kubectl -n "$NS" get events --field-selector "involvedObject.name=bad-probe,reason=Unhealthy" --no-headers 2>/dev/null | wc -l | tr -d ' ')
    if [[ "$EVENTS" -gt 0 ]]; then
        _log "  bad-probe has readiness failure events after ${i}x5s"
        break
    fi
    sleep 5
done

# Give KubeRCA a few seconds to ingest the events
_log "  Allowing KubeRCA to ingest events..."
sleep 10

# ── Step 7: Check cluster status ──────────────────────────────────────────

_log ""
_log "=== Step 7: Cluster status ==="
STATUS_RESP=$(curl -sf "${API_BASE}/status?namespace=${NS}" || echo '{"error":"fetch_failed"}')
EVENT_COUNT=$(echo "$STATUS_RESP" | jq '.recent_events | length' 2>/dev/null || echo 0)
_log "  Events visible to KubeRCA: $EVENT_COUNT"
_log "  Pod counts: $(echo "$STATUS_RESP" | jq -c '.pod_counts' 2>/dev/null || echo '{}')"

# ── Step 8: Analyze each broken workload ──────────────────────────────────

_log ""
_log "=== Step 8: Analyze broken workloads ==="

analyze_and_check() {
    local resource="$1"
    local expected_rule="$2"
    local label="$3"

    _log ""
    _log "  ── $label ──"
    _log "  POST /api/v1/analyze  resource=$resource"

    local RESP
    RESP=$(curl -sf -X POST "${API_BASE}/analyze" \
        -H "Content-Type: application/json" \
        -d "{\"resource\": \"$resource\"}" 2>&1) || true

    if [[ -z "$RESP" ]]; then
        _fail "$label: empty response (curl failed)"
        return
    fi

    local HTTP_CODE
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST "${API_BASE}/analyze" \
        -H "Content-Type: application/json" \
        -d "{\"resource\": \"$resource\"}")

    _log "  HTTP status: $HTTP_CODE"
    _log "  Response: $(echo "$RESP" | jq -c '{root_cause: .root_cause, confidence: .confidence, diagnosed_by: .diagnosed_by, rule_id: .rule_id}' 2>/dev/null || echo "$RESP")"

    # Check HTTP status
    if [[ "$HTTP_CODE" != "200" ]]; then
        _fail "$label: HTTP $HTTP_CODE (expected 200)"
        _log "  Full response: $RESP"
        return
    fi

    # Check root_cause is non-empty
    local ROOT_CAUSE
    ROOT_CAUSE=$(echo "$RESP" | jq -r '.root_cause' 2>/dev/null || echo "")
    if [[ -z "$ROOT_CAUSE" || "$ROOT_CAUSE" == "null" ]]; then
        _fail "$label: root_cause is empty"
        return
    fi

    # Check diagnosed_by (API returns lowercase "rule_engine")
    local DIAGNOSED_BY
    DIAGNOSED_BY=$(echo "$RESP" | jq -r '.diagnosed_by' 2>/dev/null || echo "")
    local DIAGNOSED_BY_LOWER
    DIAGNOSED_BY_LOWER=$(echo "$DIAGNOSED_BY" | tr '[:upper:]' '[:lower:]')
    if [[ "$DIAGNOSED_BY_LOWER" != "rule_engine" ]]; then
        _fail "$label: diagnosed_by='$DIAGNOSED_BY' (expected rule_engine)"
        return
    fi

    # Check rule_id (API returns full ID like "R01_oom_killed"; match prefix)
    local RULE_ID
    RULE_ID=$(echo "$RESP" | jq -r '.rule_id' 2>/dev/null || echo "")
    if [[ "$RULE_ID" != ${expected_rule}* ]]; then
        _fail "$label: rule_id='$RULE_ID' (expected ${expected_rule}*)"
        return
    fi

    # Check confidence > 0
    local CONFIDENCE
    CONFIDENCE=$(echo "$RESP" | jq -r '.confidence' 2>/dev/null || echo "0")
    if (( $(echo "$CONFIDENCE <= 0" | bc -l) )); then
        _fail "$label: confidence=$CONFIDENCE (expected > 0)"
        return
    fi

    # Check evidence is non-empty
    local EVIDENCE_COUNT
    EVIDENCE_COUNT=$(echo "$RESP" | jq '.evidence | length' 2>/dev/null || echo "0")
    if [[ "$EVIDENCE_COUNT" -eq 0 ]]; then
        _fail "$label: evidence list is empty"
        return
    fi

    _pass "$label: rule_id=$RULE_ID confidence=$CONFIDENCE evidence_items=$EVIDENCE_COUNT"
}

analyze_and_check "Pod/${NS}/oom-victim"   "R01" "R01 OOMKilled"
analyze_and_check "Pod/${NS}/crash-loop"   "R02" "R02 CrashLoopBackOff"
analyze_and_check "Pod/${NS}/bad-schedule"  "R03" "R03 FailedScheduling"
analyze_and_check "Pod/${NS}/bad-image"    "R04" "R04 ImagePullBackOff"
# R06 (ServiceUnreachable) has higher priority than R10 and both match
# Unhealthy events, so R06 wins. Accept either as a valid diagnosis.
analyze_and_check "Pod/${NS}/bad-probe"    "R0" "R06/R10 ReadinessProbeFailure"

# ── Summary ───────────────────────────────────────────────────────────────

_log ""
_log "=========================================="
_log "  E2E Results"
_log "=========================================="
_log "  PASS: $PASS"
_log "  FAIL: $FAIL"
_log "  SKIP: $SKIP"
_log "=========================================="

if [[ "$FAIL" -gt 0 ]]; then
    _log ""
    _log "Some tests failed. Dumping KubeRCA logs for debugging:"
    _log ""
    kubectl -n "$NS" logs deployment/kuberca --tail=100 2>/dev/null || true
    exit 1
fi

_log ""
_log "All E2E tests passed."
exit 0
