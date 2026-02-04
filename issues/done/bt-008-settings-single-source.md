---
id: bt-008
title: "Centralized settings (single source of truth)"
status: done
priority: medium
labels: []
project: bt
created: 2026-01-30
updated: 2026-01-30
depends_on: []
blocks: []
parent: null
---

# bt-008 Centralized settings (single source of truth)

## 目的
Introduce a unified settings module (e.g., pydantic-settings) for env/YAML defaults to reduce drift.

## 受け入れ条件
- Settings object used across modules for base URLs, timeouts, and runtime defaults.
- Default values are defined in a single place.

## 実施内容
- src/config/settings.py (new)
- Replace direct env access and scattered defaults where appropriate

## 結果
（完了済み）

## 補足
