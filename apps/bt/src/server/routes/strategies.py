"""
Strategy Management Endpoints
"""

from fastapi import APIRouter, HTTPException
from loguru import logger

from src.server.schemas.strategy import (
    DefaultConfigResponse,
    DefaultConfigUpdateRequest,
    DefaultConfigUpdateResponse,
    StrategyDeleteResponse,
    StrategyDetailResponse,
    StrategyDuplicateRequest,
    StrategyDuplicateResponse,
    StrategyListResponse,
    StrategyMetadataResponse,
    StrategyRenameRequest,
    StrategyRenameResponse,
    StrategyUpdateRequest,
    StrategyUpdateResponse,
    StrategyValidationRequest,
    StrategyValidationResponse,
)
from src.lib.strategy_runtime.loader import ConfigLoader
from src.lib.strategy_runtime.models import try_validate_strategy_config_dict_strict

router = APIRouter(tags=["Strategies"])

# ConfigLoaderインスタンス
_config_loader = ConfigLoader()


@router.get("/api/strategies", response_model=StrategyListResponse)
async def list_strategies() -> StrategyListResponse:
    """
    戦略一覧を取得

    全カテゴリの戦略メタデータを返却
    """
    try:
        metadata_list = _config_loader.get_strategy_metadata()

        strategies = [
            StrategyMetadataResponse(
                name=m.name,
                category=m.category,
                display_name=None,  # メタデータには含まれない
                description=None,
                last_modified=m.mtime if hasattr(m, "mtime") else None,
            )
            for m in metadata_list
        ]

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
        from src.lib.backtest_core.runner import BacktestRunner

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

    try:
        # 設定を取得
        if request and request.config:
            config = request.config
        else:
            config = _config_loader.load_strategy_config(strategy_name)

        # 厳密バリデーション（深いネスト未知キー検出を含む）
        strict_valid, strict_errors = try_validate_strategy_config_dict_strict(config)
        if not strict_valid:
            errors.extend(strict_errors)

        # 基本的な検証
        if "entry_filter_params" not in config and "exit_trigger_params" not in config:
            warnings.append(
                "entry_filter_paramsまたはexit_trigger_paramsが定義されていません"
            )

        # shared_configの検証
        shared_config = config.get("shared_config", {})
        if shared_config:
            if "dataset" in shared_config:
                dataset = shared_config["dataset"]
                if not isinstance(dataset, str) or len(dataset) == 0:
                    errors.append("datasetは空でない文字列である必要があります")

            if "kelly_fraction" in shared_config:
                kf = shared_config["kelly_fraction"]
                if not isinstance(kf, (int, float)) or kf < 0 or kf > 2:
                    errors.append("kelly_fractionは0から2の間である必要があります")

        # 実行情報の取得を試みる
        try:
            from src.lib.backtest_core.runner import BacktestRunner

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
        )

    except FileNotFoundError:
        return StrategyValidationResponse(
            valid=False,
            errors=[f"戦略が見つかりません: {strategy_name}"],
            warnings=[],
        )
    except Exception as e:
        logger.exception(f"戦略検証エラー: {strategy_name}")
        return StrategyValidationResponse(
            valid=False,
            errors=[str(e)],
            warnings=[],
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
        experimentalカテゴリのみ更新可能
    """
    try:
        # 編集可能なカテゴリかチェック
        if not _config_loader.is_editable_category(strategy_name):
            raise HTTPException(
                status_code=403,
                detail="experimentalカテゴリのみ編集可能です",
            )

        # 設定を保存
        saved_path = _config_loader.save_strategy_config(
            strategy_name, request.config, force=True
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
        default_path = _config_loader.config_dir / "default.yaml"
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
        default_path = _config_loader.config_dir / "default.yaml"
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
