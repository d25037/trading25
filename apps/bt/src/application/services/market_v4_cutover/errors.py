"""Focused Market v4 cutover responsibility module."""

from __future__ import annotations


from src.infrastructure.db.market import managed_root as _managed_root


class RuntimeStopError(_managed_root.CutoverSafetyError):
    """An owned runtime stop failed, with an explicit join verdict."""

    def __init__(self, message: str, *, process_joined: bool) -> None:
        super().__init__(message)
        self.process_joined = process_joined


class WorkerShutdownError(_managed_root.CutoverSafetyError):
    """A directory-bound helper cleanup failed, with an explicit join verdict."""

    def __init__(self, message: str, *, process_joined: bool) -> None:
        super().__init__(message)
        self.process_joined = process_joined


class RetainedMarketMutationError(_managed_root.CutoverSafetyError):
    """The retained Market DB or Parquet identity changed during smoke."""
