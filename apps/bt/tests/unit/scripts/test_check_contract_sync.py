"""Contract tests for scripts/check-contract-sync.sh."""

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[5]
SCRIPT = REPO_ROOT / "scripts" / "check-contract-sync.sh"


def _script_text() -> str:
    return SCRIPT.read_text()


def test_contract_sync_uses_one_portable_run_local_directory() -> None:
    script = _script_text()

    assert 'mktemp -d "${TMPDIR:-/tmp}/bt-contract-sync.XXXXXX"' in script
    assert 'trap \'rm -rf "${tmp_dir}"\' EXIT' in script
    assert 'tmp_openapi="${tmp_dir}/bt-openapi.json"' in script
    assert 'tmp_openapi_norm="${tmp_dir}/bt-openapi-normalized.json"' in script
    assert 'tmp_snapshot_norm="${tmp_dir}/bt-openapi-snapshot-normalized.json"' in script
    assert "XXXXXX.json" not in script


def test_contract_sync_checks_types_without_writing_generated_file() -> None:
    script = _script_text()

    assert "bt:generate-types -- --check" in script
    assert "generated_types_path=" not in script
    assert 'git -C "${repo_root}" diff --exit-code' not in script
    assert "Run: bun run --filter @trading25/contracts bt:sync" in script
