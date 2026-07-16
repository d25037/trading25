"""Strategy authoring HTTP request schemas."""

from typing import Any

from pydantic import BaseModel, Field


class DefaultConfigStructuredUpdateRequest(BaseModel):
    """Structured default-config update request."""

    execution: dict[str, Any] = Field(default_factory=dict)
    shared_config: dict[str, Any] = Field(default_factory=dict)
