# Dataset Module

`@trading25/shared/dataset` は、FastAPI バックエンド（`:3002`）前提の
**型・設定・バリデーション・進捗ユーティリティ**を提供します。

## Scope

- Preset / config (`presets`, `createConfig`, `validateConfig`)
- API 連携ユーティリティ (`ApiClient`)
- Progress utilities (`ProgressTracker`, `createConsoleProgressCallback`)
- Runtime validators (`validateStockData`, `validateStatementsData`, etc.)

## Out Of Scope

FastAPI 一本化により、`shared` でのローカル DB 実装（Drizzle ベース）は提供しません。

## Usage

```typescript
import { presets, type DatasetPreset } from '@trading25/shared/dataset';

const presetName: DatasetPreset = 'primeMarket';
const config = presets[presetName]('./dataset.db');
```
