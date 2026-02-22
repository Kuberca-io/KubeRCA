"""KubeRCA - Kubernetes Root Cause Analysis System."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("kuberca")
except PackageNotFoundError:
    __version__ = "0.0.0-dev"
