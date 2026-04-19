from __future__ import annotations

from src.shared.research_notebook_viewer import build_bundle_viewer_controls


class _FakeText:
    def __init__(self, *, value: str, label: str) -> None:
        self.value = value
        self.label = label


class _FakeUi:
    def text(self, *, value: str, label: str) -> _FakeText:
        return _FakeText(value=value, label=label)


class _FakeMo:
    def __init__(self) -> None:
        self.ui = _FakeUi()

    def md(self, text: str) -> dict[str, str]:
        return {"kind": "md", "text": text}

    def hstack(self, items: list[object]) -> dict[str, object]:
        return {"kind": "hstack", "items": items}

    def vstack(self, items: list[object]) -> dict[str, object]:
        return {"kind": "vstack", "items": items}


def test_build_bundle_viewer_controls_includes_docs_and_bundle_surface_notes() -> None:
    mo = _FakeMo()

    run_id, bundle_path, controls_view = build_bundle_viewer_controls(
        mo,
        latest_run_id="run-123",
        latest_bundle_path_str="/tmp/research/run-123",
        runner_path="apps/bt/scripts/research/run_demo.py",
        docs_readme_path="apps/bt/docs/experiments/demo/README.md",
        extra_note_lines=["- extra note"],
    )

    assert run_id.value == "run-123"
    assert bundle_path.value == "/tmp/research/run-123"
    note_text = controls_view["items"][0]["text"]
    assert "apps/bt/scripts/research/run_demo.py" in note_text
    assert "apps/bt/docs/experiments/demo/README.md" in note_text
    assert "summary.json" in note_text
    assert "- extra note" in note_text
