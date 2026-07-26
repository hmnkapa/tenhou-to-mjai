import asyncio
import json
from pathlib import Path

import ms.protocol_pb2 as pb
import pytest

import sync_account_uuids as sync


class FakeLobby:
    def __init__(
        self,
        pages: dict[int, list[str]],
        total_count: int,
        *,
        fail_at: int | None = None,
        error_code: int = 0,
    ):
        self.pages = pages
        self.total_count = total_count
        self.fail_at = fail_at
        self.error_code = error_code
        self.starts: list[int] = []

    async def fetch_game_record_list(
        self, request: pb.ReqGameRecordList
    ) -> pb.ResGameRecordList:
        self.starts.append(request.start)
        if request.start == self.fail_at:
            raise RuntimeError("simulated RPC interruption")

        response = pb.ResGameRecordList(total_count=self.total_count)
        response.error.code = self.error_code
        for uuid in self.pages.get(request.start, []):
            response.record_list.add(uuid=uuid)
        return response


def read_lines(path: Path) -> list[str]:
    return path.read_text(encoding="utf-8").splitlines()


def write_state(path: Path, watermark: str | None) -> None:
    path.write_text(
        json.dumps({"version": 1, "watermark_uuid": watermark}) + "\n",
        encoding="utf-8",
    )


def test_default_and_custom_env_paths(tmp_path):
    default_args = sync.build_parser().parse_args([])
    assert default_args.env_file == sync.DEFAULT_ENV_FILE

    custom = tmp_path / "account.env"
    custom_args = sync.build_parser().parse_args(["--env-file", str(custom)])
    assert custom_args.env_file == custom


def test_load_account_config(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text(
        "MAJSOUL_USERNAME=test@example.com\n"
        "MAJSOUL_PASSWORD=secret-value\n"
        "MAJSOUL_SERVER=cn\n",
        encoding="utf-8",
    )

    config = sync.load_account_config(env_file)

    assert config.username == "test@example.com"
    assert config.password == "secret-value"
    assert config.server == "cn"


def test_missing_config_does_not_leak_present_secret(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text(
        "MAJSOUL_PASSWORD=do-not-print-this\n",
        encoding="utf-8",
    )

    with pytest.raises(sync.ConfigError) as exc_info:
        sync.load_account_config(env_file)

    message = str(exc_info.value)
    assert "MAJSOUL_USERNAME" in message
    assert "do-not-print-this" not in message


def test_whitespace_password_is_missing(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text(
        "MAJSOUL_USERNAME=test@example.com\n"
        'MAJSOUL_PASSWORD="   "\n',
        encoding="utf-8",
    )

    with pytest.raises(sync.ConfigError) as exc_info:
        sync.load_account_config(env_file)

    assert "MAJSOUL_PASSWORD" in str(exc_info.value)


def test_unsupported_server_does_not_leak_credentials(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text(
        "MAJSOUL_USERNAME=private@example.com\n"
        "MAJSOUL_PASSWORD=do-not-print-this\n"
        "MAJSOUL_SERVER=en\n",
        encoding="utf-8",
    )

    with pytest.raises(sync.ConfigError) as exc_info:
        sync.load_account_config(env_file)

    message = str(exc_info.value)
    assert "MAJSOUL_SERVER=cn" in message
    assert "private@example.com" not in message
    assert "do-not-print-this" not in message


def test_initial_sync_fetches_all_pages_oldest_first(tmp_path):
    output = tmp_path / "todo.txt"
    state = tmp_path / "todo.txt.state.json"
    lobby = FakeLobby(
        {
            0: ["u4", "u3"],
            2: ["u2", "u1"],
        },
        total_count=4,
    )

    summary = asyncio.run(
        sync.sync_uuid_file(lobby, output, state, page_size=2)
    )

    assert read_lines(output) == ["u1", "u2", "u3", "u4"]
    assert json.loads(state.read_text()) == {
        "version": 1,
        "watermark_uuid": "u4",
    }
    assert lobby.starts == [0, 2]
    assert summary.added == 4
    assert summary.total == 4
    assert not summary.reached_watermark


def test_incremental_sync_stops_at_watermark_and_appends_only_new(tmp_path):
    output = tmp_path / "todo.txt"
    state = tmp_path / "todo.txt.state.json"
    output.write_text("u1\nu2\nu3\nu4\n", encoding="utf-8")
    write_state(state, "u4")
    lobby = FakeLobby(
        {
            0: ["u6", "u5"],
            2: ["u4", "u3"],
            4: ["u2", "u1"],
        },
        total_count=6,
    )

    summary = asyncio.run(
        sync.sync_uuid_file(lobby, output, state, page_size=2)
    )

    assert read_lines(output) == ["u1", "u2", "u3", "u4", "u5", "u6"]
    assert lobby.starts == [0, 2]
    assert summary.added == 2
    assert summary.reached_watermark
    assert json.loads(state.read_text())["watermark_uuid"] == "u6"


def test_existing_and_remote_duplicates_are_written_once(tmp_path):
    output = tmp_path / "todo.txt"
    state = tmp_path / "todo.txt.state.json"
    output.write_text("u1\nu1\nu2\n", encoding="utf-8")
    lobby = FakeLobby(
        {
            0: ["u4", "u3"],
            2: ["u3", "u2"],
            4: ["u1"],
        },
        total_count=5,
    )

    summary = asyncio.run(
        sync.sync_uuid_file(lobby, output, state, page_size=2)
    )

    assert read_lines(output) == ["u1", "u2", "u3", "u4"]
    assert summary.added == 2


def test_empty_history_creates_empty_output_and_null_watermark(tmp_path):
    output = tmp_path / "todo.txt"
    state = tmp_path / "todo.txt.state.json"
    lobby = FakeLobby({0: []}, total_count=0)

    summary = asyncio.run(
        sync.sync_uuid_file(lobby, output, state, page_size=2)
    )

    assert output.read_text(encoding="utf-8") == ""
    assert json.loads(state.read_text())["watermark_uuid"] is None
    assert summary.added == 0
    assert summary.total == 0


def test_corrupt_state_falls_back_to_full_scan(tmp_path, capsys):
    output = tmp_path / "todo.txt"
    state = tmp_path / "todo.txt.state.json"
    output.write_text("u1\n", encoding="utf-8")
    state.write_text("{broken", encoding="utf-8")
    lobby = FakeLobby({0: ["u2", "u1"]}, total_count=2)

    asyncio.run(sync.sync_uuid_file(lobby, output, state, page_size=2))

    assert read_lines(output) == ["u1", "u2"]
    assert "ignoring unreadable state file" in capsys.readouterr().err
    assert json.loads(state.read_text())["watermark_uuid"] == "u2"


def test_missing_watermark_in_output_falls_back_to_full_scan(tmp_path, capsys):
    output = tmp_path / "todo.txt"
    state = tmp_path / "todo.txt.state.json"
    output.write_text("u1\n", encoding="utf-8")
    write_state(state, "missing")
    lobby = FakeLobby(
        {
            0: ["u3", "u2"],
            2: ["u1"],
        },
        total_count=3,
    )

    asyncio.run(sync.sync_uuid_file(lobby, output, state, page_size=2))

    assert lobby.starts == [0, 2]
    assert read_lines(output) == ["u1", "u2", "u3"]
    assert "performing a full scan" in capsys.readouterr().err


def test_rpc_interruption_preserves_output_and_state(tmp_path):
    output = tmp_path / "todo.txt"
    state = tmp_path / "todo.txt.state.json"
    output.write_text("u1\n", encoding="utf-8")
    write_state(state, "u1")
    old_output = output.read_bytes()
    old_state = state.read_bytes()
    lobby = FakeLobby(
        {
            0: ["u3", "u2"],
        },
        total_count=3,
        fail_at=2,
    )

    with pytest.raises(RuntimeError, match="simulated RPC interruption"):
        asyncio.run(sync.sync_uuid_file(lobby, output, state, page_size=2))

    assert output.read_bytes() == old_output
    assert state.read_bytes() == old_state


def test_state_write_failure_is_safe_to_retry(tmp_path, monkeypatch):
    output = tmp_path / "todo.txt"
    state = tmp_path / "todo.txt.state.json"
    output.write_text("u1\n", encoding="utf-8")
    write_state(state, "u1")
    lobby = FakeLobby({0: ["u2", "u1"]}, total_count=2)

    real_write_state = sync.write_state_atomic

    def fail_state_write(_path, _watermark):
        raise OSError("simulated state write failure")

    monkeypatch.setattr(sync, "write_state_atomic", fail_state_write)
    with pytest.raises(sync.SyncError, match="rerun safely"):
        asyncio.run(sync.sync_uuid_file(lobby, output, state, page_size=2))

    assert read_lines(output) == ["u1", "u2"]
    assert json.loads(state.read_text())["watermark_uuid"] == "u1"

    monkeypatch.setattr(sync, "write_state_atomic", real_write_state)
    retry_lobby = FakeLobby({0: ["u2", "u1"]}, total_count=2)
    summary = asyncio.run(
        sync.sync_uuid_file(retry_lobby, output, state, page_size=2)
    )

    assert read_lines(output) == ["u1", "u2"]
    assert summary.added == 0
    assert json.loads(state.read_text())["watermark_uuid"] == "u2"


def test_rpc_error_code_preserves_files(tmp_path):
    output = tmp_path / "todo.txt"
    state = tmp_path / "todo.txt.state.json"
    output.write_text("u1\n", encoding="utf-8")
    write_state(state, "u1")
    lobby = FakeLobby({0: []}, total_count=0, error_code=151)

    with pytest.raises(sync.SyncError, match="error code 151"):
        asyncio.run(sync.sync_uuid_file(lobby, output, state, page_size=2))

    assert read_lines(output) == ["u1"]
    assert json.loads(state.read_text())["watermark_uuid"] == "u1"


def test_output_and_state_cannot_overwrite_env_file(tmp_path, capsys):
    env_file = tmp_path / ".env"

    output_result = sync.main(
        ["--env-file", str(env_file), "--output", str(env_file)]
    )
    assert output_result == 2
    assert "--output must not overwrite" in capsys.readouterr().err

    state_result = sync.main(
        ["--env-file", str(env_file), "--state", str(env_file)]
    )
    assert state_result == 2
    assert "--state must not overwrite" in capsys.readouterr().err
