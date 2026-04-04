from __future__ import annotations

from abc import ABC, abstractmethod

from core.schema import ModuleResult


class AbstractModule(ABC):
    name: str

    @abstractmethod
    def run(self, ticker: str) -> ModuleResult:
        """Execute synchronous analysis for ``ticker``."""

    @abstractmethod
    def validate(self) -> bool:
        """Return True if dependencies/configuration are usable."""
