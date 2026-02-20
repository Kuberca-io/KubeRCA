# Contributing to KubeRCA

Thank you for your interest in contributing to KubeRCA! This document provides guidelines and instructions for contributing.

## How to Contribute

1. **Report bugs** by opening a [GitHub issue](https://github.com/KubeRCA-io/KubeRCA/issues/new?template=bug_report.md)
2. **Suggest features** via a [feature request](https://github.com/KubeRCA-io/KubeRCA/issues/new?template=feature_request.md)
3. **Submit code** by opening a pull request

## Development Setup

### Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/) (fast Python package installer)
- Docker (for building container images)
- Helm 3.x (for chart development)
- A Kubernetes cluster (for integration testing)

### Getting Started

```bash
# Clone the repository
git clone https://github.com/KubeRCA-io/KubeRCA.git
cd KubeRCA

# Install dependencies (including dev tools)
uv sync --dev

# Verify the setup
uv run pytest
uv run ruff check .
uv run mypy kuberca/

# Install pre-commit hooks (one-time after cloning)
uv run pre-commit install
uv run pre-commit install --hook-type pre-push
```

## Development Workflow

### Fork and Branch

1. Fork the repository on GitHub
2. Create a feature branch from `main`:
   ```bash
   git checkout -b feat/my-feature main
   ```
3. Make your changes
4. Push to your fork and open a pull request

### Running Tests

```bash
# Unit tests
uv run pytest tests/unit/

# Integration tests (requires a Kubernetes cluster)
uv run pytest tests/integration/ -m integration

# All tests with coverage
uv run pytest --cov=kuberca --cov-report=term-missing
```

### Linting and Type Checking

```bash
# Lint
uv run ruff check .

# Auto-fix lint issues
uv run ruff check --fix .

# Format
uv run ruff format .

# Type check (strict mode)
uv run mypy kuberca/
```

### Building Docker Image

```bash
docker build -t kuberca:dev .
```

## Commit Message Conventions

We follow [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>(<scope>): <description>

[optional body]

[optional footer(s)]
```

Types:
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation only
- `refactor`: Code change that neither fixes a bug nor adds a feature
- `test`: Adding or updating tests
- `chore`: Build process, CI, or tooling changes
- `perf`: Performance improvement

Examples:
```
feat(rules): add R19 for init container failures
fix(cache): handle nil metadata in pod watch events
docs: update configuration reference table
```

## Pull Request Process

1. Ensure all tests pass (`uv run pytest`)
2. Ensure linting passes (`uv run ruff check .`)
3. Ensure type checking passes (`uv run mypy kuberca/`)
4. Update documentation if your change affects user-facing behavior
5. Fill out the pull request template completely
6. Request a review from a maintainer

## Code of Conduct

This project follows the [Contributor Covenant Code of Conduct](https://www.contributor-covenant.org/version/2/1/code_of_conduct/). By participating, you are expected to uphold this code.

## License

By contributing to KubeRCA, you agree that your contributions will be licensed under the [Apache License 2.0](LICENSE).
