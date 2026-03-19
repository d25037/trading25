"""
Strategy authoring editor schemas.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field

from src.entrypoints.http.schemas.signal_reference import FieldConstraints, SignalCategorySchema

AuthoringFieldType = Literal["boolean", "number", "string", "select", "string_list"]
AuthoringWidgetType = Literal[
    "switch",
    "number",
    "text",
    "textarea",
    "select",
    "combobox",
    "string_list",
]
AuthoringFieldSection = Literal["strategy", "shared_config", "execution"]
AuthoringFieldSource = Literal["default", "strategy"]


class AuthoringFieldSchema(BaseModel):
    """Metadata for one editor field."""

    path: str = Field(description="Dot-separated field path")
    section: AuthoringFieldSection = Field(description="Owning config section")
    group: str | None = Field(default=None, description="Visual group key")
    label: str = Field(description="Field label")
    type: AuthoringFieldType = Field(description="Value type")
    widget: AuthoringWidgetType = Field(description="Suggested UI widget")
    description: str = Field(description="Detailed help text")
    summary: str | None = Field(default=None, description="Short helper summary")
    default: Any = Field(default=None, description="Model default value when available")
    options: list[str] | None = Field(default=None, description="Allowed options")
    constraints: FieldConstraints | None = Field(default=None, description="Numeric constraints")
    placeholder: str | None = Field(default=None, description="Suggested placeholder")
    unit: str | None = Field(default=None, description="Display unit")
    examples: list[str] = Field(default_factory=list, description="Example values")
    required: bool = Field(default=False, description="Whether the field is required")
    advanced_only: bool = Field(
        default=False,
        description="Field is only editable through the advanced YAML fallback",
    )


class AuthoringFieldGroupSchema(BaseModel):
    """Field group metadata."""

    key: str
    label: str
    description: str | None = None


class StrategyEditorCapabilities(BaseModel):
    """Server-reported editor capabilities."""

    visual_editor: bool = True
    yaml_fallback: bool = True
    preview: bool = True
    preserves_unknown_fields: bool = True
    structured_default_edit: bool = True


class StrategyEditorReferenceResponse(BaseModel):
    """Editor reference payload for metadata-driven authoring."""

    basics: list[AuthoringFieldSchema]
    shared_config_fields: list[AuthoringFieldSchema]
    execution_fields: list[AuthoringFieldSchema]
    shared_config_groups: list[AuthoringFieldGroupSchema]
    execution_groups: list[AuthoringFieldGroupSchema]
    signal_categories: list[SignalCategorySchema]
    capabilities: StrategyEditorCapabilities = Field(
        default_factory=StrategyEditorCapabilities
    )


class AuthoringFieldProvenance(BaseModel):
    """Source-of-truth provenance for a field."""

    path: str
    source: AuthoringFieldSource
    overridden: bool


class StrategyEditorContextResponse(BaseModel):
    """Strategy editor context payload."""

    strategy_name: str
    category: str
    raw_config: dict[str, Any]
    default_shared_config: dict[str, Any]
    default_execution: dict[str, Any]
    effective_shared_config: dict[str, Any]
    effective_execution: dict[str, Any]
    shared_config_provenance: list[AuthoringFieldProvenance]
    execution_provenance: list[AuthoringFieldProvenance]
    unknown_top_level_keys: list[str] = Field(default_factory=list)


class DefaultConfigEditorContextResponse(BaseModel):
    """Default config editor context payload."""

    raw_yaml: str
    raw_document: dict[str, Any]
    raw_execution: dict[str, Any]
    raw_shared_config: dict[str, Any]
    effective_execution: dict[str, Any]
    effective_shared_config: dict[str, Any]
    advanced_only_paths: list[str] = Field(default_factory=list)


class DefaultConfigStructuredUpdateRequest(BaseModel):
    """Structured default-config update request."""

    execution: dict[str, Any] = Field(default_factory=dict)
    shared_config: dict[str, Any] = Field(default_factory=dict)
