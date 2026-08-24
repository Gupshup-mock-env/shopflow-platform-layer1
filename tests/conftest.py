"""Fixtures for the repository-level cross-service tests.

These tests load each service's ``models.py`` from disk under a unique module
name so producer and consumer schemas can be compared in one process without the
``import app`` name collisions that would otherwise occur across services.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType
from typing import Callable

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture(scope="session")
def repo_root() -> Path:
    return REPO_ROOT


@pytest.fixture(scope="session")
def load_models() -> Callable[[str], ModuleType]:
    def _load(service: str) -> ModuleType:
        path = REPO_ROOT / service / "models.py"
        spec = importlib.util.spec_from_file_location(f"{service}__models", path)
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        # Register before executing so pydantic can resolve the module's
        # `from __future__ import annotations` forward references (e.g. OrderLine).
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        return module

    return _load
