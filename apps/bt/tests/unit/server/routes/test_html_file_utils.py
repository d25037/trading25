"""server/routes/html_file_utils.py のテスト"""

import builtins
from datetime import datetime
from pathlib import Path

import pytest
from fastapi import HTTPException

from src.entrypoints.http.routes.html_file_utils import (
    _anchor_html_name_for_bundle_path,
    _existing_bundle_paths,
    _primary_bundle_path,
    delete_html_file,
    list_html_files_in_dir,
    parse_html_filename,
    read_html_file,
    rename_html_file,
)


# ===== parse_html_filename =====


class TestParseHtmlFilename:
    def test_valid_pattern(self):
        name, dt = parse_html_filename("my_dataset_20250115_143022.html")
        assert name == "my_dataset"
        assert dt == datetime(2025, 1, 15, 14, 30, 22)

    def test_multi_underscore_dataset(self):
        name, dt = parse_html_filename("range_break_v5_20250115_143022.html")
        assert name == "range_break_v5"
        assert dt is not None

    def test_invalid_date(self):
        # regex matches 8+6 digits, but strptime fails -> fallback to full name
        name, dt = parse_html_filename("data_99999999_999999.html")
        assert name == "data_99999999_999999"
        assert dt is None

    def test_invalid_date_no_regex_match(self):
        name, dt = parse_html_filename("data_abc_def.html")
        assert name == "data_abc_def"
        assert dt is None

    def test_no_match(self):
        name, dt = parse_html_filename("random_file.html")
        assert name == "random_file"
        assert dt is None

    def test_non_html(self):
        _name, dt = parse_html_filename("test_20250101_120000.txt")
        assert dt is None


# ===== list_html_files_in_dir =====


class TestListHtmlFilesInDir:
    def test_empty_dir(self, tmp_path):
        files, total = list_html_files_in_dir(tmp_path)
        assert files == []
        assert total == 0

    def test_nonexistent_dir(self, tmp_path):
        files, total = list_html_files_in_dir(tmp_path / "nonexistent")
        assert files == []
        assert total == 0

    def test_multiple_strategies(self, tmp_path):
        (tmp_path / "strat_a").mkdir()
        (tmp_path / "strat_a" / "data_20250101_120000.html").write_text("<html/>")
        (tmp_path / "strat_b").mkdir()
        (tmp_path / "strat_b" / "data_20250102_120000.html").write_text("<html/>")
        files, total = list_html_files_in_dir(tmp_path)
        assert total == 2
        assert len(files) == 2

    def test_strategy_filter(self, tmp_path):
        (tmp_path / "strat_a").mkdir()
        (tmp_path / "strat_a" / "data_20250101_120000.html").write_text("<html/>")
        (tmp_path / "strat_b").mkdir()
        (tmp_path / "strat_b" / "data_20250102_120000.html").write_text("<html/>")
        _files, total = list_html_files_in_dir(tmp_path, strategy="strat_a")
        assert total == 1

    def test_limit_parameter(self, tmp_path):
        (tmp_path / "s").mkdir()
        for i in range(5):
            (tmp_path / "s" / f"d_{20250101 + i}_120000.html").write_text("<html/>")
        files, total = list_html_files_in_dir(tmp_path, limit=2)
        assert total == 5
        assert len(files) == 2

    def test_sorted_descending(self, tmp_path):
        (tmp_path / "s").mkdir()
        (tmp_path / "s" / "d_20250101_120000.html").write_text("<html/>")
        (tmp_path / "s" / "d_20250301_120000.html").write_text("<html/>")
        files, _ = list_html_files_in_dir(tmp_path)
        assert files[0]["created_at"] >= files[1]["created_at"]

    def test_metrics_only_bundle_is_listed(self, tmp_path):
        strategy_dir = tmp_path / "s"
        strategy_dir.mkdir()
        (strategy_dir / "d_20250301_120000.metrics.json").write_text("{}")

        files, total = list_html_files_in_dir(tmp_path)

        assert total == 1
        assert files[0]["filename"] == "d_20250301_120000.html"
        assert files[0]["html_available"] is False

    def test_bundle_path_helpers(self, tmp_path):
        html_path = tmp_path / "bundle.html"
        metrics_path = tmp_path / "bundle.metrics.json"
        metrics_path.write_text("{}")

        assert _anchor_html_name_for_bundle_path(metrics_path) == "bundle.html"
        assert _anchor_html_name_for_bundle_path(Path("bundle.txt")) is None
        existing_paths = _existing_bundle_paths(html_path)
        assert existing_paths == [metrics_path]
        assert _primary_bundle_path(html_path, existing_paths) == metrics_path


# ===== read_html_file =====


class TestReadHtmlFile:
    def test_success(self, tmp_path):
        (tmp_path / "strat").mkdir()
        html_file = tmp_path / "strat" / "test.html"
        html_file.write_text("<html>hello</html>")
        result = read_html_file(tmp_path, "strat", "test.html")
        import base64

        decoded = base64.b64decode(result).decode("utf-8")
        assert decoded == "<html>hello</html>"

    def test_not_found(self, tmp_path):
        (tmp_path / "strat").mkdir()
        with pytest.raises(HTTPException) as exc_info:
            read_html_file(tmp_path, "strat", "missing.html")
        assert exc_info.value.status_code == 404

    def test_not_html_suffix(self, tmp_path):
        (tmp_path / "strat").mkdir()
        (tmp_path / "strat" / "test.txt").write_text("data")
        with pytest.raises(HTTPException) as exc_info:
            read_html_file(tmp_path, "strat", "test.txt")
        assert exc_info.value.status_code == 400

    def test_read_error_returns_500(self, tmp_path, monkeypatch):
        (tmp_path / "strat").mkdir()
        html_file = tmp_path / "strat" / "test.html"
        html_file.write_text("<html>hello</html>")
        original_open = builtins.open

        def _raise_open(*args, **kwargs):  # noqa: ANN002, ANN003
            if args and args[0] == html_file and len(args) > 1 and args[1] == "rb":
                raise OSError("boom")
            return original_open(*args, **kwargs)

        monkeypatch.setattr(builtins, "open", _raise_open)

        with pytest.raises(HTTPException) as exc_info:
            read_html_file(tmp_path, "strat", "test.html")
        assert exc_info.value.status_code == 500


# ===== rename_html_file =====


class TestRenameHtmlFile:
    def test_success(self, tmp_path):
        (tmp_path / "strat").mkdir()
        (tmp_path / "strat" / "old.html").write_text("data")
        rename_html_file(tmp_path, "strat", "old.html", "new.html")
        assert (tmp_path / "strat" / "new.html").exists()
        assert not (tmp_path / "strat" / "old.html").exists()

    def test_same_name_noop(self, tmp_path):
        (tmp_path / "strat").mkdir()
        (tmp_path / "strat" / "same.html").write_text("data")
        rename_html_file(tmp_path, "strat", "same.html", "same.html")
        assert (tmp_path / "strat" / "same.html").exists()

    def test_invalid_new_filename(self, tmp_path):
        with pytest.raises(HTTPException) as exc_info:
            rename_html_file(tmp_path, "strat", "old.html", "no_extension")
        assert exc_info.value.status_code == 400

    def test_invalid_pattern(self, tmp_path):
        with pytest.raises(HTTPException) as exc_info:
            rename_html_file(tmp_path, "strat", "old.html", "../bad.html")
        assert exc_info.value.status_code == 400

    def test_conflict_409(self, tmp_path):
        (tmp_path / "strat").mkdir()
        (tmp_path / "strat" / "a.html").write_text("a")
        (tmp_path / "strat" / "b.html").write_text("b")
        with pytest.raises(HTTPException) as exc_info:
            rename_html_file(tmp_path, "strat", "a.html", "b.html")
        assert exc_info.value.status_code == 409

    def test_not_found_404(self, tmp_path):
        (tmp_path / "strat").mkdir()
        with pytest.raises(HTTPException) as exc_info:
            rename_html_file(tmp_path, "strat", "missing.html", "new.html")
        assert exc_info.value.status_code == 404

    def test_non_html_old_file(self, tmp_path):
        with pytest.raises(HTTPException) as exc_info:
            rename_html_file(tmp_path, "strat", "old.txt", "new.html")
        assert exc_info.value.status_code == 400

    def test_renames_sibling_artifacts(self, tmp_path):
        strategy_dir = tmp_path / "strat"
        strategy_dir.mkdir()
        for suffix in (".html", ".metrics.json", ".manifest.json", ".report.json"):
            (strategy_dir / f"old{suffix}").write_text("data")

        rename_html_file(tmp_path, "strat", "old.html", "new.html")

        for suffix in (".html", ".metrics.json", ".manifest.json", ".report.json"):
            assert (strategy_dir / f"new{suffix}").exists()
            assert not (strategy_dir / f"old{suffix}").exists()

    def test_conflict_when_target_sibling_exists(self, tmp_path):
        strategy_dir = tmp_path / "strat"
        strategy_dir.mkdir()
        (strategy_dir / "old.html").write_text("html")
        (strategy_dir / "old.metrics.json").write_text("{}")
        (strategy_dir / "new.metrics.json").write_text("{}")

        with pytest.raises(HTTPException) as exc_info:
            rename_html_file(tmp_path, "strat", "old.html", "new.html")

        assert exc_info.value.status_code == 409
        assert (strategy_dir / "old.html").exists()
        assert (strategy_dir / "old.metrics.json").exists()

    def test_renames_metrics_only_bundle(self, tmp_path):
        strategy_dir = tmp_path / "strat"
        strategy_dir.mkdir()
        for suffix in (".metrics.json", ".manifest.json", ".report.json"):
            (strategy_dir / f"old{suffix}").write_text("data")

        rename_html_file(tmp_path, "strat", "old.html", "new.html")

        for suffix in (".metrics.json", ".manifest.json", ".report.json"):
            assert (strategy_dir / f"new{suffix}").exists()
            assert not (strategy_dir / f"old{suffix}").exists()

    def test_rename_permission_error_rolls_back(self, tmp_path, monkeypatch):
        strategy_dir = tmp_path / "strat"
        strategy_dir.mkdir()
        (strategy_dir / "old.html").write_text("html")
        (strategy_dir / "old.metrics.json").write_text("{}")
        original_rename = Path.rename

        def _rename(self: Path, target: Path):  # noqa: ANN001
            if self.name == "old.metrics.json":
                raise PermissionError("boom")
            return original_rename(self, target)

        monkeypatch.setattr(Path, "rename", _rename)

        with pytest.raises(HTTPException) as exc_info:
            rename_html_file(tmp_path, "strat", "old.html", "new.html")

        assert exc_info.value.status_code == 403
        assert (strategy_dir / "old.html").exists()
        assert (strategy_dir / "old.metrics.json").exists()
        assert not (strategy_dir / "new.html").exists()


# ===== delete_html_file =====


class TestDeleteHtmlFile:
    def test_success(self, tmp_path):
        (tmp_path / "strat").mkdir()
        (tmp_path / "strat" / "del.html").write_text("data")
        delete_html_file(tmp_path, "strat", "del.html")
        assert not (tmp_path / "strat" / "del.html").exists()

    def test_not_found_404(self, tmp_path):
        (tmp_path / "strat").mkdir()
        with pytest.raises(HTTPException) as exc_info:
            delete_html_file(tmp_path, "strat", "missing.html")
        assert exc_info.value.status_code == 404

    def test_non_html_400(self, tmp_path):
        with pytest.raises(HTTPException) as exc_info:
            delete_html_file(tmp_path, "strat", "file.txt")
        assert exc_info.value.status_code == 400

    def test_deletes_sibling_artifacts(self, tmp_path):
        strategy_dir = tmp_path / "strat"
        strategy_dir.mkdir()
        for suffix in (".html", ".metrics.json", ".manifest.json", ".report.json"):
            (strategy_dir / f"del{suffix}").write_text("data")

        delete_html_file(tmp_path, "strat", "del.html")

        for suffix in (".html", ".metrics.json", ".manifest.json", ".report.json"):
            assert not (strategy_dir / f"del{suffix}").exists()

    def test_deletes_metrics_only_bundle(self, tmp_path):
        strategy_dir = tmp_path / "strat"
        strategy_dir.mkdir()
        for suffix in (".metrics.json", ".manifest.json", ".report.json"):
            (strategy_dir / f"del{suffix}").write_text("data")

        delete_html_file(tmp_path, "strat", "del.html")

        for suffix in (".metrics.json", ".manifest.json", ".report.json"):
            assert not (strategy_dir / f"del{suffix}").exists()

    def test_delete_permission_error_returns_403(self, tmp_path, monkeypatch):
        strategy_dir = tmp_path / "strat"
        strategy_dir.mkdir()
        html_path = strategy_dir / "del.html"
        html_path.write_text("data")
        original_unlink = Path.unlink

        def _unlink(self: Path, *args, **kwargs):  # noqa: ANN002, ANN003
            if self == html_path:
                raise PermissionError("boom")
            return original_unlink(self, *args, **kwargs)

        monkeypatch.setattr(Path, "unlink", _unlink)

        with pytest.raises(HTTPException) as exc_info:
            delete_html_file(tmp_path, "strat", "del.html")
        assert exc_info.value.status_code == 403
