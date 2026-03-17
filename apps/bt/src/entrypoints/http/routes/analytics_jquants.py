"""Compatibility wrapper for renamed analytics_market routes."""

from src.entrypoints.http.routes import analytics_market as _analytics_market
from src.entrypoints.http.routes.analytics_market import *  # noqa: F403

_executor = _analytics_market._executor


def _get_executor():  # noqa: ANN201
    _analytics_market._executor = _executor
    executor = _analytics_market._get_executor()
    globals()["_executor"] = _analytics_market._executor
    return executor


def _get_margin_service(request):  # noqa: ANN001, ANN201
    return _analytics_market._get_margin_service(request)


def _get_roe_service(request):  # noqa: ANN001, ANN201
    return _analytics_market._get_roe_service(request)
