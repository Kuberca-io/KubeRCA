# ─────────────────────────────────────────────────────────────────────────────
# Stage 1: builder
# Install uv, resolve dependencies, and build the project wheel.
# The builder stage is discarded; only the virtualenv is copied forward.
# ─────────────────────────────────────────────────────────────────────────────
FROM python:3.12-alpine AS builder

# Security: do not run build tools as root beyond what is strictly necessary.
# pip/uv need to write to /opt/kuberca; the COPY below handles ownership.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_NO_CACHE=1 \
    UV_PYTHON_DOWNLOADS=never \
    VIRTUAL_ENV=/opt/kuberca/venv \
    PATH="/opt/kuberca/venv/bin:/root/.cargo/bin:${PATH}"

WORKDIR /build

# Install uv — the fast Python package installer / resolver.
# Pin to a specific version so the build is reproducible.
RUN pip install --no-cache-dir uv==0.6.1

# Copy only the dependency manifests first so Docker layer caching skips the
# slow dependency resolution step when only source code changes.
COPY pyproject.toml uv.lock ./

# Create the virtualenv and install production dependencies from the lock file.
# --no-install-project installs deps only (not the project itself yet), so the
# source-code layer below does not invalidate the dependency layer.
RUN uv venv "${VIRTUAL_ENV}" && \
    uv sync --active --frozen --no-dev --no-install-project

# Now copy the full source and install the project package.
COPY README.md ./
COPY kuberca/ ./kuberca/
RUN uv sync --active --frozen --no-dev

# ─────────────────────────────────────────────────────────────────────────────
# Stage 2: runtime
# Minimal image — only the pre-built virtualenv, no build tools.
# ─────────────────────────────────────────────────────────────────────────────
FROM python:3.12-alpine AS runtime

LABEL org.opencontainers.image.title="KubeRCA" \
      org.opencontainers.image.description="Kubernetes Root Cause Analysis System" \
      org.opencontainers.image.version="0.1.1" \
      org.opencontainers.image.source="https://github.com/kuberca-io/kuberca" \
      org.opencontainers.image.licenses="Apache-2.0"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    VIRTUAL_ENV=/opt/kuberca/venv \
    PATH="/opt/kuberca/venv/bin:${PATH}"

# Use UID/GID 65534 ("nobody") — the conventional choice for read-only,
# no-privilege containers.  Alpine already ships with nobody:nobody at
# 65534:65534, so no addgroup/adduser is needed.  This maps to what the
# Helm chart sets via runAsUser: 65534.

# Copy the virtualenv from the builder stage.
COPY --from=builder --chown=65534:65534 /opt/kuberca/venv /opt/kuberca/venv

# Remove pip from the system Python — not needed at runtime and eliminates
# CVE-2025-8869 and CVE-2026-1703.
RUN pip uninstall -y pip setuptools 2>/dev/null; \
    rm -rf /usr/local/lib/python3.12/site-packages/pip* \
           /usr/local/lib/python3.12/site-packages/setuptools* \
           /usr/local/bin/pip*

# /tmp  — Python / uvicorn temporary files (readOnlyRootFilesystem=true)
# /data — optional SQLite ledger persistence
RUN mkdir -p /tmp /data && chmod 0700 /tmp /data && chown 65534:65534 /tmp /data

USER 65534

WORKDIR /app

# The container image does not need the source tree: the installed package in
# the virtualenv is sufficient.  However we copy kuberca/ so that
# `python -m kuberca` works without a wheel install side-channel.
COPY --from=builder --chown=65534:65534 /build/kuberca ./kuberca

# Default configuration — every value can be overridden via the Helm ConfigMap.
ENV KUBERCA_API_PORT=8080 \
    KUBERCA_LOG_LEVEL=info

EXPOSE 8080

# Graceful shutdown: SIGTERM triggers the asyncio signal handler in app.py.
# The Helm chart sets terminationGracePeriodSeconds=30 which matches the 15 s
# component-level grace period in app.py plus a 15 s buffer for OS cleanup.
STOPSIGNAL SIGTERM

ENTRYPOINT ["python", "-m", "kuberca"]
