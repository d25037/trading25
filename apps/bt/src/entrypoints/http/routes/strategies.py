"""
Strategy Management Endpoints
"""

from io import StringIO

from fastapi import APIRouter, HTTPException
from loguru import logger
from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap

from src.application.services.strategy_authoring_service import (
    build_default_config_editor_context,
    build_strategy_editor_context,
    build_strategy_editor_reference,
)
from src.application.services.strategy_optimization_service import (
    strategy_optimization_service,
)
from src.domains.strategy.runtime.compiler import compile_strategy_config
from src.domains.strategy.runtime.production_requirements import (
    validate_production_strategy_dataset_requirement,
)
from src.domains.strategy.runtime.screening_profile import load_strategy_screening_config
from src.entrypoints.http.schemas.strategy import (
    DefaultConfigResponse,
    DefaultConfigUpdateRequest,
    DefaultConfigUpdateResponse,
    OptimizationDiagnosticResponse,
    StrategyDeleteResponse,
    StrategyDetailResponse,
    StrategyDuplicateRequest,
    StrategyDuplicateResponse,
    StrategyOptimizationDeleteResponse,
    StrategyOptimizationSaveRequest,
    StrategyOptimizationSaveResponse,
    StrategyOptimizationStateResponse,
    StrategyListResponse,
    StrategyMetadataResponse,
    StrategyMoveRequest,
    StrategyMoveResponse,
    StrategyRenameRequest,
    StrategyRenameResponse,
    StrategyUpdateRequest,
    StrategyUpdateResponse,
    StrategyValidationRequest,
    StrategyValidationResponse,
)
from src.entrypoints.http.schemas.strategy_authoring import (
    DefaultConfigEditorContextResponse,
    DefaultConfigStructuredUpdateRequest,
    StrategyEditorContextResponse,
    StrategyEditorReferenceResponse,
)
from src.entrypoints.http.schemas.screening import EntryDecidability, ScreeningSupport
from src.application.services.strategy_dataset_metadata import (
    StrategyDatasetMetadata,
    resolve_strategy_dataset_metadata,
)
from src.domains.strategy.runtime.loader import ConfigLoader
from src.domains.strategy.runtime.models import (
    ExecutionConfig,
    try_validate_strategy_config_dict_strict,
)
from src.domains.strategy.runtime.file_operations import load_yaml_file
from src.shared.models.config import SharedConfig

router = APIRouter(tags=["Strategies"])

# ConfigLoaderインスタンス
_config_loader = ConfigLoader()


def _resolve_screening_metadata(
    strategy_name: str,
) -> tuple[
    ScreeningSupport,
    EntryDecidability | None,
    str | None,
    StrategyDatasetMetadata,
]:
    try:
        loaded = load_strategy_screening_config(_config_loader, strategy_name)
    except Exception as exc:
        logger.warning(f"failed to resolve screening mode for {strategy_name}: {exc}")
        return (
            "unsupported",
            None,
            str(exc),
            StrategyDatasetMetadata(
                dataset_name=None,
                dataset_preset=None,
                screening_default_markets=None,
            ),
        )

    try:
        dataset_metadata = resolve_strategy_dataset_metadata(
            strategy_name,
            config_loader=_config_loader,
            strategy_config=loaded.config,
        )
        screening_error = None
    except Exception as exc:
        dataset_metadata = StrategyDatasetMetadata(
            dataset_name=None,
            dataset_preset=None,
            screening_default_markets=None,
        )
        screening_error = str(exc)

    return (
        loaded.screening_support,
        loaded.entry_decidability,
        screening_error,
        dataset_metadata,
    )


def _resolve_strategy_category(strategy_name: str) -> str | None:
    if strategy_name.startswith("production/"):
        return "production"

    resolved_category = _config_loader.resolve_strategy_category(strategy_name)
    if isinstance(resolved_category, str):
        return resolved_category
    return None


def _ensure_commented_map(parent: CommentedMap, key: str) -> CommentedMap:
    existing = parent.get(key)
    if isinstance(existing, CommentedMap):
        return existing
    created = CommentedMap()
    parent[key] = created
    return created


def _patch_mapping(target: CommentedMap, payload: dict[str, object]) -> None:
    for key in list(target.keys()):
        if key not in payload:
            del target[key]

    for key, value in payload.items():
        if isinstance(value, dict):
            child = target.get(key)
            if not isinstance(child, CommentedMap):
                child = CommentedMap()
                target[key] = child
            _patch_mapping(child, value)
            continue
        target[key] = value


def _validate_default_structured_request(
    request: DefaultConfigStructuredUpdateRequest,
) -> None:
    invalid_execution_keys = [
        key for key in request.execution.keys() if key not in ExecutionConfig.model_fields
    ]
    if invalid_execution_keys:
        raise HTTPException(
            status_code=400,
            detail=(
                "Unknown execution field(s): "
                + ", ".join(sorted(invalid_execution_keys))
            ),
        )

    try:
        ExecutionConfig.model_validate(request.execution)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    try:
        SharedConfig.model_validate(
            request.shared_config,
            context={"resolve_stock_codes": False},
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


def _build_optimization_diagnostics(
    issues: list[object],
) -> list[OptimizationDiagnosticResponse]:
    diagnostics: list[OptimizationDiagnosticResponse] = []
    for issue in issues:
        path = getattr(issue, "path", "")
        message = getattr(issue, "message", "")
        diagnostics.append(
            OptimizationDiagnosticResponse(path=str(path), message=str(message))
        )
    return diagnostics


def _build_strategy_optimization_state_response(
    strategy_name: str,
    *,
    persisted: bool,
    source: str,
    analysis,
) -> StrategyOptimizationStateResponse:
    return StrategyOptimizationStateResponse(
        strategy_name=strategy_name,
        persisted=persisted,
        source=source,  # type: ignore[arg-type]
        optimization=analysis.optimization,
        yaml_content=analysis.yaml_content,
        valid=analysis.valid,
        ready_to_run=analysis.ready_to_run,
        param_count=analysis.param_count,
        combinations=analysis.combinations,
        errors=_build_optimization_diagnostics(analysis.errors),
        warnings=_build_optimization_diagnostics(analysis.warnings),
        drift=_build_optimization_diagnostics(analysis.drift),
    )


@router.get(
    "/api/strategies/editor/reference",
    response_model=StrategyEditorReferenceResponse,
)
async def get_strategy_editor_reference() -> StrategyEditorReferenceResponse:
    """Metadata for the strategy visual authoring UI."""
    try:
        return build_strategy_editor_reference()
    except Exception as e:
        logger.exception("strategy editor reference error")
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.get(
    "/api/strategies/{strategy_name:path}/editor-context",
    response_model=StrategyEditorContextResponse,
)
async def get_strategy_editor_context(strategy_name: str) -> StrategyEditorContextResponse:
    """Structured editor context for one strategy."""
    try:
        config = _config_loader.load_strategy_config(strategy_name)
        category = _resolve_strategy_category(strategy_name) or "unknown"
        merged_shared_config = _config_loader.merge_shared_config(config)
        merged_execution = _config_loader.get_execution_config(config)
        return build_strategy_editor_context(
            strategy_name=strategy_name,
            category=category,
            raw_config=config,
            default_config=_config_loader.default_config,
            merged_shared_config=merged_shared_config,
            merged_execution_config=merged_execution,
        )
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=f"戦略が見つかりません: {strategy_name}") from e
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"strategy editor context error: {strategy_name}")
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.get(
    "/api/strategies/{strategy_name:path}/optimization",
    response_model=StrategyOptimizationStateResponse,
)
async def get_strategy_optimization(
    strategy_name: str,
) -> StrategyOptimizationStateResponse:
    """Fetch strategy-linked optimization state."""
    try:
        analysis = strategy_optimization_service.get_state(strategy_name)
        return _build_strategy_optimization_state_response(
            strategy_name,
            persisted=analysis.optimization is not None,
            source="saved",
            analysis=analysis,
        )
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=f"戦略が見つかりません: {strategy_name}") from e
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        logger.exception(f"strategy optimization get error: {strategy_name}")
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.post(
    "/api/strategies/{strategy_name:path}/optimization/draft",
    response_model=StrategyOptimizationStateResponse,
)
async def generate_strategy_optimization_draft_endpoint(
    strategy_name: str,
) -> StrategyOptimizationStateResponse:
    """Generate a strategy-linked optimization draft."""
    try:
        analysis = strategy_optimization_service.generate_draft(strategy_name)
        return _build_strategy_optimization_state_response(
            strategy_name,
            persisted=False,
            source="draft",
            analysis=analysis,
        )
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=f"戦略が見つかりません: {strategy_name}") from e
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        logger.exception(f"strategy optimization draft error: {strategy_name}")
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.put(
    "/api/strategies/{strategy_name:path}/optimization",
    response_model=StrategyOptimizationSaveResponse,
)
async def save_strategy_optimization(
    strategy_name: str,
    request: StrategyOptimizationSaveRequest,
) -> StrategyOptimizationSaveResponse:
    """Save strategy-linked optimization YAML onto the strategy file."""
    try:
        if not _config_loader.is_updatable_category(strategy_name):
            raise HTTPException(
                status_code=403,
                detail="experimental / production カテゴリのみ更新可能です",
            )

        analysis = strategy_optimization_service.save(
            strategy_name,
            request.yaml_content,
        )
        response = _build_strategy_optimization_state_response(
            strategy_name,
            persisted=True,
            source="saved",
            analysis=analysis,
        )
        return StrategyOptimizationSaveResponse(success=True, **response.model_dump())
    except HTTPException:
        raise
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=f"戦略が見つかりません: {strategy_name}") from e
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        logger.exception(f"strategy optimization save error: {strategy_name}")
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.delete(
    "/api/strategies/{strategy_name:path}/optimization",
    response_model=StrategyOptimizationDeleteResponse,
)
async def delete_strategy_optimization(
    strategy_name: str,
) -> StrategyOptimizationDeleteResponse:
    """Delete strategy-linked optimization block."""
    try:
        if not _config_loader.is_updatable_category(strategy_name):
            raise HTTPException(
                status_code=403,
                detail="experimental / production カテゴリのみ更新可能です",
            )
        strategy_optimization_service.delete(strategy_name)
        return StrategyOptimizationDeleteResponse(
            success=True,
            strategy_name=strategy_name,
        )
    except HTTPException:
        raise
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=f"戦略が見つかりません: {strategy_name}") from e
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        logger.exception(f"strategy optimization delete error: {strategy_name}")
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.get("/api/strategies", response_model=StrategyListResponse)
async def list_strategies() -> StrategyListResponse:
    """
    戦略一覧を取得

    全カテゴリの戦略メタデータを返却
    """
    try:
        metadata_list = _config_loader.get_strategy_metadata()

        strategies = []
        for m in metadata_list:
            screening_support, entry_decidability, screening_error, dataset_metadata = _resolve_screening_metadata(
                m.name
            )
            strategies.append(
                StrategyMetadataResponse(
                    name=m.name,
                    category=m.category,
                    display_name=None,  # メタデータには含まれない
                    description=None,
                    last_modified=m.mtime if hasattr(m, "mtime") else None,
                    screening_support=screening_support,
                    entry_decidability=entry_decidability,
                    screening_error=screening_error,
                    dataset_name=dataset_metadata.dataset_name,
                    dataset_preset=dataset_metadata.dataset_preset,
                    screening_default_markets=dataset_metadata.screening_default_markets,
                )
            )

        return StrategyListResponse(
            strategies=strategies,
            total=len(strategies),
        )

    except Exception as e:
        logger.exception("戦略一覧取得エラー")
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.get("/api/strategies/{strategy_name:path}", response_model=StrategyDetailResponse)
async def get_strategy_detail(strategy_name: str) -> StrategyDetailResponse:
    """
    戦略詳細を取得

    Args:
        strategy_name: 戦略名（例: 'range_break_v5', 'production/range_break_v5'）
    """
    try:
        # 戦略設定を読み込み
        config = _config_loader.load_strategy_config(strategy_name)

        # カテゴリを推定
        if "/" in strategy_name:
            category = strategy_name.split("/")[0]
            name_only = strategy_name.split("/")[1]
        else:
            category = "unknown"
            name_only = strategy_name

        # 実行情報を取得
        from src.domains.backtest.core.runner import BacktestRunner

        runner = BacktestRunner()
        execution_info = runner.get_execution_info(strategy_name)

        return StrategyDetailResponse(
            name=name_only,
            category=category,
            display_name=config.get("display_name"),
            description=config.get("description"),
            config=config,
            execution_info=execution_info,
        )

    except HTTPException:
        raise
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=f"戦略が見つかりません: {strategy_name}") from e
    except Exception as e:
        logger.exception(f"戦略詳細取得エラー: {strategy_name}")
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.post(
    "/api/strategies/{strategy_name:path}/validate",
    response_model=StrategyValidationResponse,
)
async def validate_strategy(
    strategy_name: str,
    request: StrategyValidationRequest | None = None,
) -> StrategyValidationResponse:
    """
    戦略設定を検証

    Args:
        strategy_name: 戦略名
        request: 検証する設定（省略時は既存の設定を検証）
    """
    errors: list[str] = []
    warnings: list[str] = []
    compiled_strategy = None

    try:
        # 設定を取得
        validating_request_config = bool(request and request.config)
        if request and request.config:
            config = request.config
        else:
            config = _config_loader.load_strategy_config(strategy_name)

        # 厳密バリデーション（深いネスト未知キー検出を含む）
        strict_valid, strict_errors = try_validate_strategy_config_dict_strict(config)
        if not strict_valid:
            errors.extend(strict_errors)
        else:
            try:
                validate_production_strategy_dataset_requirement(
                    category=_resolve_strategy_category(strategy_name),
                    config=config,
                    strategy_name=strategy_name,
                )
            except ValueError as exc:
                errors.append(str(exc))

        if not errors:
            try:
                compiled_strategy = compile_strategy_config(
                    strategy_name,
                    config,
                    config_loader=_config_loader,
                )
            except Exception as exc:
                errors.append(f"CompiledStrategyIR generation failed: {exc}")

        # 基本的な検証
        if "entry_filter_params" not in config and "exit_trigger_params" not in config:
            warnings.append(
                "entry_filter_paramsまたはexit_trigger_paramsが定義されていません"
            )

        # shared_configの検証
        shared_config = config.get("shared_config", {})
        if shared_config:
            if "dataset" in shared_config:
                errors.append(
                    "shared_config.dataset is no longer supported for normal runs; "
                    "use shared_config.universe_preset for PIT universe selection"
                )

            if "universe_preset" in shared_config:
                universe_preset = shared_config["universe_preset"]
                if not isinstance(universe_preset, str) or len(universe_preset) == 0:
                    errors.append("universe_presetは空でない文字列である必要があります")

            if "kelly_fraction" in shared_config:
                kf = shared_config["kelly_fraction"]
                if not isinstance(kf, (int, float)) or kf < 0 or kf > 2:
                    errors.append("kelly_fractionは0から2の間である必要があります")

        # 保存前プレビューの検証では、ディスク上の戦略読込に依存する execution_info
        # を混ぜると unsaved config が誤って invalid になる。
        if not validating_request_config:
            try:
                from src.domains.backtest.core.runner import BacktestRunner

                runner = BacktestRunner()
                execution_info = runner.get_execution_info(strategy_name)
                if "error" in execution_info:
                    errors.append(f"実行情報取得エラー: {execution_info['error']}")
            except Exception as e:
                warnings.append(f"実行情報の取得に失敗しました: {e}")

        return StrategyValidationResponse(
            valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            compiled_strategy=compiled_strategy if len(errors) == 0 else None,
        )

    except FileNotFoundError:
        return StrategyValidationResponse(
            valid=False,
            errors=[f"戦略が見つかりません: {strategy_name}"],
            warnings=[],
            compiled_strategy=None,
        )
    except Exception as e:
        logger.exception(f"戦略検証エラー: {strategy_name}")
        return StrategyValidationResponse(
            valid=False,
            errors=[str(e)],
            warnings=[],
            compiled_strategy=None,
        )


@router.put("/api/strategies/{strategy_name:path}", response_model=StrategyUpdateResponse)
async def update_strategy(
    strategy_name: str,
    request: StrategyUpdateRequest,
) -> StrategyUpdateResponse:
    """
    戦略設定を更新

    Args:
        strategy_name: 戦略名
        request: 更新する設定

    Note:
        experimental / production カテゴリのみ更新可能
    """
    try:
        # 更新可能なカテゴリかチェック
        if not _config_loader.is_updatable_category(strategy_name):
            raise HTTPException(
                status_code=403,
                detail="experimental / production カテゴリのみ更新可能です",
            )

        # production は「編集」のみ許可（新規作成は不可）
        if strategy_name.startswith("production/"):
            try:
                _config_loader.load_strategy_config(strategy_name)
            except FileNotFoundError as e:
                raise HTTPException(
                    status_code=404,
                    detail=f"戦略が見つかりません: {strategy_name}",
                ) from e

        # 設定を保存
        saved_path = _config_loader.save_strategy_config(
            strategy_name, request.config, force=True, allow_production=True
        )

        return StrategyUpdateResponse(
            success=True,
            strategy_name=strategy_name,
            path=str(saved_path),
        )

    except HTTPException:
        raise
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e)) from e
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        logger.exception(f"戦略更新エラー: {strategy_name}")
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.delete("/api/strategies/{strategy_name:path}", response_model=StrategyDeleteResponse)
async def delete_strategy(strategy_name: str) -> StrategyDeleteResponse:
    """
    戦略を削除

    Args:
        strategy_name: 戦略名

    Note:
        experimentalカテゴリのみ削除可能
    """
    try:
        # 編集可能なカテゴリかチェック
        if not _config_loader.is_editable_category(strategy_name):
            raise HTTPException(
                status_code=403,
                detail="experimentalカテゴリのみ削除可能です",
            )

        # 削除を実行
        _config_loader.delete_strategy(strategy_name)

        return StrategyDeleteResponse(
            success=True,
            strategy_name=strategy_name,
        )

    except HTTPException:
        raise
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e)) from e
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except Exception as e:
        logger.exception(f"戦略削除エラー: {strategy_name}")
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.post(
    "/api/strategies/{strategy_name:path}/duplicate",
    response_model=StrategyDuplicateResponse,
)
async def duplicate_strategy(
    strategy_name: str,
    request: StrategyDuplicateRequest,
) -> StrategyDuplicateResponse:
    """
    戦略を複製

    Args:
        strategy_name: 複製元の戦略名
        request: 複製先の新しい戦略名

    Note:
        複製先は常にexperimentalカテゴリ
    """
    try:
        # 複製を実行
        saved_path = _config_loader.duplicate_strategy(strategy_name, request.new_name)

        return StrategyDuplicateResponse(
            success=True,
            new_strategy_name=f"experimental/{request.new_name}",
            path=str(saved_path),
        )

    except HTTPException:
        raise
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except FileExistsError as e:
        raise HTTPException(status_code=409, detail=str(e)) from e
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        logger.exception(f"戦略複製エラー: {strategy_name}")
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.post(
    "/api/strategies/{strategy_name:path}/rename",
    response_model=StrategyRenameResponse,
)
async def rename_strategy(
    strategy_name: str,
    request: StrategyRenameRequest,
) -> StrategyRenameResponse:
    """
    戦略をリネーム

    Args:
        strategy_name: 現在の戦略名
        request: リネームリクエスト（新しい戦略名）

    Note:
        experimentalカテゴリのみリネーム可能
    """
    try:
        # 編集可能なカテゴリかチェック
        if not _config_loader.is_editable_category(strategy_name):
            raise HTTPException(
                status_code=403,
                detail="experimentalカテゴリのみリネーム可能です",
            )

        # リネームを実行
        new_path = _config_loader.rename_strategy(strategy_name, request.new_name)

        # 元の戦略名を抽出（カテゴリなし）
        old_name = strategy_name.split("/")[-1] if "/" in strategy_name else strategy_name

        return StrategyRenameResponse(
            success=True,
            old_name=old_name,
            new_name=request.new_name,
            new_path=str(new_path),
        )

    except HTTPException:
        raise
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e)) from e
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except FileExistsError as e:
        raise HTTPException(status_code=409, detail=str(e)) from e
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        logger.exception(f"戦略リネームエラー: {strategy_name}")
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.post(
    "/api/strategies/{strategy_name:path}/move",
    response_model=StrategyMoveResponse,
)
async def move_strategy(
    strategy_name: str,
    request: StrategyMoveRequest,
) -> StrategyMoveResponse:
    """
    戦略のカテゴリを移動

    Args:
        strategy_name: 移動元の戦略名
        request: 移動先カテゴリ

    Note:
        production / experimental / legacy 間の移動のみサポート
    """
    try:
        new_strategy_name, new_path = _config_loader.move_strategy(
            strategy_name, request.target_category
        )

        return StrategyMoveResponse(
            success=True,
            old_strategy_name=strategy_name,
            new_strategy_name=new_strategy_name,
            target_category=request.target_category,
            new_path=str(new_path),
        )

    except HTTPException:
        raise
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except FileExistsError as e:
        raise HTTPException(status_code=409, detail=str(e)) from e
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        logger.exception(f"戦略移動エラー: {strategy_name}")
        raise HTTPException(status_code=500, detail=str(e)) from e


# ============================================
# Default Config Endpoints
# ============================================


@router.get("/api/config/default", response_model=DefaultConfigResponse)
async def get_default_config() -> DefaultConfigResponse:
    """
    デフォルト設定をraw YAML文字列として取得

    コメントを保持するためYAML文字列をそのまま返却
    """
    try:
        default_path = _config_loader.get_default_config_path()
        if not default_path.exists():
            raise HTTPException(
                status_code=404,
                detail="default.yamlが見つかりません",
            )

        content = default_path.read_text(encoding="utf-8")
        return DefaultConfigResponse(content=content)

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("デフォルト設定取得エラー")
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.get(
    "/api/config/default/editor-context",
    response_model=DefaultConfigEditorContextResponse,
)
async def get_default_config_editor_context() -> DefaultConfigEditorContextResponse:
    """Structured context for default.yaml visual editing."""
    try:
        default_path = _config_loader.get_default_config_path()
        if not default_path.exists():
            raise HTTPException(
                status_code=404,
                detail="default.yamlが見つかりません",
            )

        raw_yaml = default_path.read_text(encoding="utf-8")
        raw_document = load_yaml_file(default_path)
        return build_default_config_editor_context(
            raw_yaml=raw_yaml,
            raw_document=raw_document,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("default config editor context error")
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.put("/api/config/default", response_model=DefaultConfigUpdateResponse)
async def update_default_config(
    request: DefaultConfigUpdateRequest,
) -> DefaultConfigUpdateResponse:
    """
    デフォルト設定を更新

    YAML文字列を受け取り、パース検証後にdefault.yamlに書き込み、
    ConfigLoaderのメモリ内デフォルト設定をリロード
    """
    try:
        from io import StringIO
        from tempfile import NamedTemporaryFile

        from ruamel.yaml import YAML, YAMLError

        # YAML構文検証
        ruamel_yaml = YAML()
        ruamel_yaml.preserve_quotes = True
        try:
            parsed = ruamel_yaml.load(StringIO(request.content))
        except YAMLError as e:
            raise HTTPException(
                status_code=400,
                detail=f"YAML構文エラー: {e}",
            ) from e

        if not isinstance(parsed, dict):
            raise HTTPException(
                status_code=400,
                detail="YAMLはオブジェクトである必要があります",
            )

        # "default"キーの存在チェック
        if "default" not in parsed:
            raise HTTPException(
                status_code=400,
                detail="YAMLに'default'キーが必要です",
            )

        if not isinstance(parsed["default"], dict):
            raise HTTPException(
                status_code=400,
                detail="'default'キーの値はオブジェクトである必要があります",
            )

        # Atomic write: temp file → rename
        default_path = _config_loader.get_default_config_write_path()
        default_path.parent.mkdir(parents=True, exist_ok=True)
        with NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=default_path.parent,
            suffix=".yaml.tmp",
            delete=False,
        ) as tmp:
            tmp.write(request.content)
            tmp_path = tmp.name

        from pathlib import Path

        Path(tmp_path).replace(default_path)

        # メモリ内デフォルト設定をリロード
        _config_loader.reload_default_config()

        logger.info("デフォルト設定更新成功")
        return DefaultConfigUpdateResponse(success=True)

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("デフォルト設定更新エラー")
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.put(
    "/api/config/default/structured",
    response_model=DefaultConfigUpdateResponse,
)
async def update_default_config_structured(
    request: DefaultConfigStructuredUpdateRequest,
) -> DefaultConfigUpdateResponse:
    """Patch default.yaml using structured execution/shared_config payloads."""
    try:
        _validate_default_structured_request(request)

        default_read_path = _config_loader.get_default_config_path()
        if not default_read_path.exists():
            raise HTTPException(
                status_code=404,
                detail="default.yamlが見つかりません",
            )
        default_write_path = _config_loader.get_default_config_write_path()
        default_write_path.parent.mkdir(parents=True, exist_ok=True)

        yaml = YAML()
        yaml.preserve_quotes = True
        document = yaml.load(StringIO(default_read_path.read_text(encoding="utf-8")))
        if not isinstance(document, CommentedMap):
            raise HTTPException(status_code=400, detail="default.yaml root must be a mapping")

        default_section = _ensure_commented_map(document, "default")
        parameters = _ensure_commented_map(default_section, "parameters")
        execution = _ensure_commented_map(default_section, "execution")
        shared_config = _ensure_commented_map(parameters, "shared_config")

        _patch_mapping(execution, request.execution)
        _patch_mapping(shared_config, request.shared_config)

        with default_write_path.open("w", encoding="utf-8") as handle:
            yaml.dump(document, handle)

        _config_loader.reload_default_config()
        return DefaultConfigUpdateResponse(success=True)
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("default structured update error")
        raise HTTPException(status_code=500, detail=str(e)) from e
