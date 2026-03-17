"""Compatibility wrapper for renamed analytics_market routes."""

from src.entrypoints.http.routes import analytics_market as _analytics_market
from src.entrypoints.http.routes.analytics_market import *  # noqa: F403

_executor = _analytics_market._executor
_get_executor = _analytics_market._get_executor
_get_margin_service = _analytics_market._get_margin_service
_get_roe_service = _analytics_market._get_roe_service
