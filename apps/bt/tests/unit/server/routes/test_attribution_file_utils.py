"""server/routes/attribution_file_utils.py のテスト"""

import json
from datetime import datetime
from unittest.mock import patch

import pytest
from fastapi import HTTPException

from src.server.routes.attribution_file_utils import (
    list_attribution_files_in_dir,
    parse_attribution_filename,
    read_attribution_file,
    validate_attribution_filename,
    validate_attribution_strategy_param,
)


class TestParseAttributionFilename:
    def test_valid_pattern(self):
        job_id, dt = parse_attribution_filename("attribution_20260115_143022_job-1.json")
        assert job_id == "job-1"
        assert dt == datetime(2026, 1, 15, 14, 30, 22)

    def test_invalid_pattern(self):
        job_id, dt = parse_attribution_filename("random.json")
        assert job_id is None
        assert dt is None

    def test_invalid_datetime_returns_job_id_only(self):
        job_id, dt = parse_attribution_filename("attribution_99999999_999999_job-x.json")
        assert job_id == "job-x"
        assert dt is None


class TestValidateAttributionParams:
    def test_validate_strategy_allows_nested(self):
        assert validate_attribution_strategy_param("experimental/range_break_v18") == "experimental/range_break_v18"

    def test_validate_strategy_rejects_path_traversal(self):
        with pytest.raises(HTTPException) as exc_info:
            validate_attribution_strategy_param("../../etc")
        assert exc_info.value.status_code == 400

    def test_validate_strategy_rejects_empty_and_backslash_and_absolute(self):
        for value in ("", r"experimental\rb", "/absolute/path"):
            with pytest.raises(HTTPException) as exc_info:
                validate_attribution_strategy_param(value)
            assert exc_info.value.status_code == 400

    def test_validate_strategy_rejects_invalid_segment_chars(self):
        with pytest.raises(HTTPException) as exc_info:
            validate_attribution_strategy_param("experimental/range break")
        assert exc_info.value.status_code == 400

    def test_validate_filename_success(self):
        validate_attribution_filename("attribution_20260115_143022_job-1.json")

    def test_validate_filename_rejects_non_json(self):
        with pytest.raises(HTTPException) as exc_info:
            validate_attribution_filename("bad.txt")
        assert exc_info.value.status_code == 400

    def test_validate_filename_rejects_empty_and_invalid_chars(self):
        for value in ("", "bad name.json"):
            with pytest.raises(HTTPException) as exc_info:
                validate_attribution_filename(value)
            assert exc_info.value.status_code == 400


class TestListAttributionFilesInDir:
    def test_empty_dir(self, tmp_path):
        files, total = list_attribution_files_in_dir(tmp_path)
        assert files == []
        assert total == 0

    def test_nonexistent_dir(self, tmp_path):
        files, total = list_attribution_files_in_dir(tmp_path / "does-not-exist")
        assert files == []
        assert total == 0

    def test_collects_nested_strategy_paths(self, tmp_path):
        target = tmp_path / "experimental" / "range_break_v18"
        target.mkdir(parents=True)
        (target / "attribution_20260112_120000_job-a.json").write_text("{}")

        files, total = list_attribution_files_in_dir(tmp_path)
        assert total == 1
        assert files[0]["strategy_name"] == "experimental/range_break_v18"
        assert files[0]["job_id"] == "job-a"

    def test_strategy_filter(self, tmp_path):
        a = tmp_path / "experimental" / "range_break_v18"
        b = tmp_path / "experimental" / "other"
        a.mkdir(parents=True)
        b.mkdir(parents=True)
        (a / "attribution_20260112_120000_job-a.json").write_text("{}")
        (b / "attribution_20260112_120000_job-b.json").write_text("{}")

        files, total = list_attribution_files_in_dir(tmp_path, strategy="experimental/range_break_v18")
        assert total == 1
        assert files[0]["strategy_name"] == "experimental/range_break_v18"

    def test_limit_and_sort_descending(self, tmp_path):
        target = tmp_path / "experimental" / "range_break_v18"
        target.mkdir(parents=True)
        (target / "attribution_20260112_120000_job-a.json").write_text("{}")
        (target / "attribution_20260113_120000_job-b.json").write_text("{}")
        (target / "attribution_20260114_120000_job-c.json").write_text("{}")

        files, total = list_attribution_files_in_dir(tmp_path, limit=2)
        assert total == 3
        assert len(files) == 2
        assert files[0]["filename"] == "attribution_20260114_120000_job-c.json"

    def test_invalid_strategy_filter_raises_400(self, tmp_path):
        with pytest.raises(HTTPException) as exc_info:
            list_attribution_files_in_dir(tmp_path, strategy="..\\etc")
        assert exc_info.value.status_code == 400

    def test_strategy_filter_nonexistent_target_returns_empty(self, tmp_path):
        files, total = list_attribution_files_in_dir(tmp_path, strategy="experimental/missing")
        assert files == []
        assert total == 0

    def test_skips_root_file_and_non_file_json_entries_and_uses_mtime_fallback(self, tmp_path):
        # root-level file is intentionally skipped (strategy_name becomes ".")
        (tmp_path / "attribution_20260112_120000_root.json").write_text("{}")
        # directory ending with .json should be ignored by is_file() guard
        (tmp_path / "fake.json").mkdir()
        # invalid timestamp pattern triggers mtime fallback branch
        target = tmp_path / "experimental" / "range_break_v18"
        target.mkdir(parents=True)
        (target / "attribution_99999999_999999_job-a.json").write_text("{}")

        files, total = list_attribution_files_in_dir(tmp_path)
        assert total == 1
        assert files[0]["strategy_name"] == "experimental/range_break_v18"


class TestReadAttributionFile:
    def test_success(self, tmp_path):
        target = tmp_path / "experimental" / "range_break_v18"
        target.mkdir(parents=True)
        payload = {"saved_at": "2026-01-12T12:00:00+00:00", "strategy": {"name": "experimental/range_break_v18"}}
        (target / "attribution_20260112_120000_job-a.json").write_text(json.dumps(payload), encoding="utf-8")

        result = read_attribution_file(
            tmp_path,
            "experimental/range_break_v18",
            "attribution_20260112_120000_job-a.json",
        )
        assert result["strategy"]["name"] == "experimental/range_break_v18"

    def test_not_found(self, tmp_path):
        with pytest.raises(HTTPException) as exc_info:
            read_attribution_file(tmp_path, "experimental/range_break_v18", "missing.json")
        assert exc_info.value.status_code == 404

    def test_invalid_filename_rejects_traversal(self, tmp_path):
        with pytest.raises(HTTPException) as exc_info:
            read_attribution_file(tmp_path, "experimental/range_break_v18", "..secret.json")
        assert exc_info.value.status_code == 400

    def test_invalid_path_when_json_name_is_directory(self, tmp_path):
        target = tmp_path / "experimental" / "range_break_v18" / "as_dir.json"
        target.mkdir(parents=True)

        with pytest.raises(HTTPException) as exc_info:
            read_attribution_file(tmp_path, "experimental/range_break_v18", "as_dir.json")
        assert exc_info.value.status_code == 400

    def test_invalid_json_returns_500(self, tmp_path):
        target = tmp_path / "experimental" / "range_break_v18"
        target.mkdir(parents=True)
        (target / "attribution_20260112_120000_job-a.json").write_text("{invalid", encoding="utf-8")

        with pytest.raises(HTTPException) as exc_info:
            read_attribution_file(
                tmp_path,
                "experimental/range_break_v18",
                "attribution_20260112_120000_job-a.json",
            )
        assert exc_info.value.status_code == 500

    def test_read_os_error_returns_500(self, tmp_path):
        target = tmp_path / "experimental" / "range_break_v18"
        target.mkdir(parents=True)
        filepath = target / "attribution_20260112_120000_job-a.json"
        filepath.write_text("{}", encoding="utf-8")

        with patch("pathlib.Path.read_text", side_effect=OSError("permission denied")):
            with pytest.raises(HTTPException) as exc_info:
                read_attribution_file(
                    tmp_path,
                    "experimental/range_break_v18",
                    "attribution_20260112_120000_job-a.json",
                )
        assert exc_info.value.status_code == 500

    def test_non_object_json_returns_500(self, tmp_path):
        target = tmp_path / "experimental" / "range_break_v18"
        target.mkdir(parents=True)
        (target / "attribution_20260112_120000_job-a.json").write_text("[1,2,3]", encoding="utf-8")

        with pytest.raises(HTTPException) as exc_info:
            read_attribution_file(
                tmp_path,
                "experimental/range_break_v18",
                "attribution_20260112_120000_job-a.json",
            )
        assert exc_info.value.status_code == 500
