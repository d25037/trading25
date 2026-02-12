"""Unit tests for scripts/export_openapi.py using stdlib unittest."""

from __future__ import annotations

import importlib.util
import io
import json
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch


def _load_module():
    bt_root = Path(__file__).resolve().parents[3]
    module_path = bt_root / "scripts" / "export_openapi.py"
    spec = importlib.util.spec_from_file_location("export_openapi", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Failed to load export_openapi module")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestExportOpenApi(unittest.TestCase):
    def test_build_openapi_schema_uses_create_app(self) -> None:
        module = _load_module()

        fake_app_module = types.ModuleType("src.server.app")

        class _FakeApp:
            def openapi(self) -> dict[str, str]:
                return {"openapi": "3.1.0"}

        fake_app_module.create_app = lambda: _FakeApp()

        with patch.dict(sys.modules, {"src.server.app": fake_app_module}):
            schema = module.build_openapi_schema()

        self.assertEqual(schema, {"openapi": "3.1.0"})

    def test_write_schema_writes_stdout_when_output_is_none(self) -> None:
        module = _load_module()
        buffer = io.StringIO()

        with patch.object(module.sys, "stdout", buffer):
            module.write_schema({"openapi": "3.1.0"}, None)

        self.assertEqual(buffer.getvalue(), '{\n  "openapi": "3.1.0"\n}\n')

    def test_write_schema_writes_file_when_output_path_provided(self) -> None:
        module = _load_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            output = Path(tmpdir) / "openapi.json"
            module.write_schema({"openapi": "3.1.0"}, output)
            self.assertTrue(output.exists())
            self.assertEqual(json.loads(output.read_text()), {"openapi": "3.1.0"})

    def test_parse_args_parses_output_option(self) -> None:
        module = _load_module()
        with patch.object(sys, "argv", ["export_openapi.py", "--output", "/tmp/schema.json"]):
            args = module.parse_args()
        self.assertEqual(str(args.output), "/tmp/schema.json")


if __name__ == "__main__":
    unittest.main()
