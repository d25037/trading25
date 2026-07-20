"""Focused Market v5 cutover responsibility module."""

from __future__ import annotations

from dataclasses import dataclass

from .contracts import RetainedPromotionPreparation
from .journal import PromotionJournal


@dataclass(frozen=True)
class RetainedPromotionContext:
    """Live exact evidence needed to unwind one retained promotion attempt."""

    preparation: RetainedPromotionPreparation
    journal: PromotionJournal
