#!/usr/bin/env python3
"""Incrementally synchronize game UUIDs from one Majsoul account."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import hmac
import json
import os
import sys
import tempfile
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol
from urllib.parse import urlsplit

import aiohttp
import ms.protocol_pb2 as pb
from dotenv import dotenv_values
from ms.base import MSRPCChannel
from ms.rpc import Lobby, Route
from tensoul.downloader import MajsoulLoginError


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_ENV_FILE = PROJECT_ROOT / ".env"
DEFAULT_OUTPUT_FILE = Path("todo.txt")
PAGE_SIZE = 30
STATE_VERSION = 1
MAJSOUL_ORIGIN = "https://game.maj-soul.com"
MAJSOUL_ROUTE_AUTHORITIES = tuple(
    f"route-{number}.maj-soul.com" for number in range(2, 7)
)
MAJSOUL_PRODUCT_VERSION = "4.0.45"
MAJSOUL_RESOURCE_VERSION = "0.16.257"
MAJSOUL_CLIENT_VERSION_STRING = f"WebGL_2022-{MAJSOUL_RESOURCE_VERSION}"
MAJSOUL_LOGIN_BEAT_CONTRACT = "DF2vkXCnfeXp4WoGrBGNcJBufZiMN3uP"
MAJSOUL_CURRENCY_PLATFORMS = (1, 2, 5, 6, 8, 10, 11)

# ReqRequestConnection.platform is field 6 in the current protocol.  The
# ms-api version pulled in by tensoul predates that field, but protobuf keeps
# unknown fields when parsing and serializing a message.
_ROUTE_PLATFORM_WEB_FIELD = b"\x32\x03Web"


class ConfigError(Exception):
    """Invalid or missing local configuration."""


class SyncError(Exception):
    """The remote history could not be synchronized safely."""


class RecordListClient(Protocol):
    async def fetch_game_record_list(
        self, request: pb.ReqGameRecordList
    ) -> pb.ResGameRecordList: ...


@dataclass(frozen=True)
class AccountConfig:
    username: str
    password: str
    server: str


@dataclass(frozen=True)
class FetchResult:
    head_uuid: str | None
    uuids_newest_first: list[str]
    pages_fetched: int
    reached_watermark: bool


@dataclass(frozen=True)
class SyncSummary:
    added: int
    total: int
    pages_fetched: int
    reached_watermark: bool


@dataclass(frozen=True)
class GatewayRoute:
    route_id: str
    domain: str


def build_route_request(
    route_id: str,
    *,
    timestamp: int | None = None,
) -> pb.ReqRequestConnection:
    """Build the current Unity client's route handshake."""
    request = pb.ReqRequestConnection(
        type=2,
        route_id=route_id,
        timestamp=int(time.time()) if timestamp is None else timestamp,
    )
    request.ParseFromString(
        request.SerializeToString() + _ROUTE_PLATFORM_WEB_FIELD
    )
    return request


def build_login_request(
    username: str,
    password: str,
    *,
    random_key: str | None = None,
) -> pb.ReqLogin:
    """Build the password-login request used by the current CN WebGL client."""
    request = pb.ReqLogin(
        account=username,
        password=hmac.new(
            b"lailai",
            password.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest(),
        reconnect=False,
        random_key=random_key or str(uuid.uuid4()),
        gen_access_token=True,
        currency_platforms=MAJSOUL_CURRENCY_PLATFORMS,
        type=0,
        client_version_string=MAJSOUL_CLIENT_VERSION_STRING,
        tag="cn",
    )
    request.client_version.package = MAJSOUL_PRODUCT_VERSION
    request.client_version.resource = MAJSOUL_RESOURCE_VERSION

    request.device.platform = "pc"
    request.device.hardware = "pc"
    request.device.os = "mac" if sys.platform == "darwin" else "windows"
    request.device.os_version = "macOS" if sys.platform == "darwin" else "win11"
    request.device.is_browser = True
    request.device.software = "Chrome"
    request.device.sale_platform = "web"
    request.device.screen_width = 1920
    request.device.screen_height = 1080
    request.device.user_agent = "Mozilla/5.0"
    request.device.screen_type = 1
    return request


def select_gateway_route(
    payload: object,
    *,
    preferred_authority: str | None = None,
) -> GatewayRoute:
    """Choose and validate a gateway returned by the official route API."""
    try:
        routes = payload["data"]["routes"]  # type: ignore[index]
    except (KeyError, TypeError):
        raise SyncError("Majsoul gateway discovery returned malformed data") from None

    if not isinstance(routes, list):
        raise SyncError("Majsoul gateway discovery returned malformed data")

    valid: list[GatewayRoute] = []
    for candidate in routes:
        if not isinstance(candidate, dict):
            continue
        route_id = str(candidate.get("id") or "").strip()
        domain = str(candidate.get("domain") or "").strip()
        parsed = urlsplit(f"//{domain}")
        if (
            not route_id
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.path
            or parsed.query
            or parsed.fragment
        ):
            continue
        valid.append(GatewayRoute(route_id=route_id, domain=domain))

    if not valid:
        raise SyncError("Majsoul gateway discovery returned no usable routes")

    return next(
        (
            route
            for route in valid
            if urlsplit(f"//{route.domain}").hostname
            == preferred_authority
        ),
        valid[0],
    )


async def discover_gateway() -> GatewayRoute:
    """Race the official route APIs and return the first usable gateway."""

    async def fetch_one(
        session: aiohttp.ClientSession,
        authority: str,
    ) -> tuple[str, object]:
        routes_url = f"https://{authority}/api/clientgate/routes"
        async with session.get(
            routes_url,
            params={
                "platform": "Web",
                "version": MAJSOUL_PRODUCT_VERSION,
                "lang": "chs_t",
            },
        ) as response:
            response.raise_for_status()
            return authority, await response.json(content_type=None)

    timeout = aiohttp.ClientTimeout(total=10, connect=5)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        tasks = [
            asyncio.create_task(fetch_one(session, authority))
            for authority in MAJSOUL_ROUTE_AUTHORITIES
        ]
        try:
            for completed in asyncio.as_completed(tasks):
                try:
                    authority, payload = await completed
                    return select_gateway_route(
                        payload,
                        preferred_authority=authority,
                    )
                except (aiohttp.ClientError, asyncio.TimeoutError, ValueError):
                    continue
                except SyncError:
                    continue
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    raise SyncError("failed to discover a current Majsoul gateway")


class CurrentMajsoulAccountClient:
    """Small current-protocol client for account UUID synchronization."""

    def __init__(self) -> None:
        self.channel: MSRPCChannel | None = None
        self.lobby: Lobby | None = None
        self.route: Route | None = None
        self._sustain_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        gateway = await discover_gateway()

        self.channel = MSRPCChannel(f"wss://{gateway.domain}/gateway")
        self.lobby = Lobby(self.channel)
        self.route = Route(self.channel)

        try:
            await self.channel.connect(MAJSOUL_ORIGIN)
            route_response = await self.route.request_connection(
                build_route_request(gateway.route_id)
            )
            if route_response.error.code:
                raise SyncError(
                    "Majsoul route handshake failed with error code "
                    f"{route_response.error.code}"
                )
        except BaseException:
            await self.close()
            raise

    async def login(self, username: str, password: str) -> None:
        if self.lobby is None or self.route is None:
            raise SyncError("Majsoul client was not started")

        response = await self.lobby.login(
            build_login_request(username, password)
        )
        if not response.access_token:
            raise MajsoulLoginError(response)

        login_success = await self.lobby.login_success(pb.ReqCommon())
        if login_success.error.code:
            raise SyncError(
                "Majsoul loginSuccess failed with error code "
                f"{login_success.error.code}"
            )

        login_beat = await self.lobby.login_beat(
            pb.ReqLoginBeat(contract=MAJSOUL_LOGIN_BEAT_CONTRACT)
        )
        if login_beat.error.code:
            raise SyncError(
                "Majsoul loginBeat failed with error code "
                f"{login_beat.error.code}"
            )

        self._sustain_task = asyncio.create_task(self._sustain())

    async def _sustain(self, ping_interval: float = 4) -> None:
        assert self.channel is not None
        assert self.route is not None
        try:
            while True:
                await asyncio.sleep(ping_interval)
                await self.channel._ws.ping()
                response = await self.route.heartbeat(
                    pb.ReqHeartbeat(
                        delay=0,
                        no_operation_counter=0,
                        platform=11,
                        network_quality=0,
                    )
                )
                if response.error.code:
                    return
        except asyncio.CancelledError:
            raise
        except Exception:
            return

    async def close(self) -> None:
        if self._sustain_task is not None:
            self._sustain_task.cancel()
            try:
                await self._sustain_task
            except asyncio.CancelledError:
                pass
            self._sustain_task = None

        if self.channel is not None:
            try:
                await self.channel.close()
            except (AttributeError, asyncio.CancelledError):
                pass
            except Exception:
                pass
            self.channel = None


def load_account_config(env_file: Path) -> AccountConfig:
    """Read account credentials from an explicit .env file."""
    if not env_file.is_file():
        raise ConfigError(f"configuration file not found: {env_file}")

    try:
        values = dotenv_values(env_file)
    except Exception as exc:
        raise ConfigError(f"failed to read configuration file: {env_file}") from exc

    username = (values.get("MAJSOUL_USERNAME") or "").strip()
    password = values.get("MAJSOUL_PASSWORD") or ""
    server = (values.get("MAJSOUL_SERVER") or "cn").strip().lower()

    missing = [
        key
        for key, value in (
            ("MAJSOUL_USERNAME", username),
            ("MAJSOUL_PASSWORD", password.strip()),
        )
        if not value
    ]
    if missing:
        raise ConfigError(f"missing required setting(s): {', '.join(missing)}")

    if server != "cn":
        raise ConfigError("only MAJSOUL_SERVER=cn is supported")

    return AccountConfig(username=username, password=password, server=server)


def default_state_path(output_file: Path) -> Path:
    return Path(f"{output_file}.state.json")


def describe_login_error(exc: MajsoulLoginError) -> str:
    """Return safe login diagnostics without rendering the protobuf response."""
    code: int | None = None
    if exc.args:
        response = exc.args[0]
        error = getattr(response, "error", None)
        candidate = getattr(error, "code", None)
        if isinstance(candidate, int) and candidate:
            code = candidate

    if code == 151:
        return (
            "Majsoul login failed (RPC error code 151: the server rejected the "
            "client version as outdated; the web protocol may have changed)"
        )
    if code is not None:
        return f"Majsoul login failed (RPC error code {code})"
    return "Majsoul login failed (the server returned no access token)"


def load_existing_uuids(output_file: Path) -> list[str]:
    """Load UUID lines, preserving the first occurrence of each UUID."""
    if not output_file.exists():
        return []

    content = output_file.read_text(encoding="utf-8")
    seen: set[str] = set()
    uuids: list[str] = []
    for line in content.splitlines():
        uuid = line.strip()
        if uuid and uuid not in seen:
            seen.add(uuid)
            uuids.append(uuid)
    return uuids


def load_watermark(
    state_file: Path, existing_uuids: set[str]
) -> tuple[str | None, str | None]:
    """Return (watermark, warning); invalid state safely falls back to a full scan."""
    if not state_file.exists():
        return None, None

    try:
        data = json.loads(state_file.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return None, f"ignoring unreadable state file: {state_file}"

    if not isinstance(data, dict) or data.get("version") != STATE_VERSION:
        return None, f"ignoring unsupported state file: {state_file}"

    watermark = data.get("watermark_uuid")
    if watermark is None:
        return None, None
    if not isinstance(watermark, str) or not watermark.strip():
        return None, f"ignoring invalid state file: {state_file}"

    watermark = watermark.strip()
    if watermark not in existing_uuids:
        return (
            None,
            "state watermark is absent from the UUID file; performing a full scan",
        )
    return watermark, None


async def fetch_uuids_since(
    lobby: RecordListClient,
    watermark_uuid: str | None,
    *,
    page_size: int = PAGE_SIZE,
) -> FetchResult:
    """Fetch newest-first UUIDs until the previous successful watermark."""
    if page_size <= 0:
        raise ValueError("page_size must be positive")

    offset = 0
    pages_fetched = 0
    head_uuid: str | None = None
    reached_watermark = False
    seen: set[str] = set()
    fetched: list[str] = []

    while True:
        request = pb.ReqGameRecordList(start=offset, count=page_size, type=0)
        response = await lobby.fetch_game_record_list(request)
        pages_fetched += 1

        if response.error.code:
            raise SyncError(
                f"fetchGameRecordList failed with error code {response.error.code}"
            )

        records = list(response.record_list)
        total_count = int(response.total_count)

        if offset == 0 and records:
            candidate = records[0].uuid.strip()
            if not candidate:
                raise SyncError("the newest game record has an empty UUID")
            head_uuid = candidate

        if not records:
            if offset < total_count:
                raise SyncError("game-record pagination returned an unexpected empty page")
            break

        for record in records:
            uuid = record.uuid.strip()
            if not uuid:
                raise SyncError("game-record pagination returned an empty UUID")
            if watermark_uuid is not None and uuid == watermark_uuid:
                reached_watermark = True
                break
            if uuid not in seen:
                seen.add(uuid)
                fetched.append(uuid)

        if reached_watermark:
            break

        offset += len(records)
        if offset >= total_count:
            break

    return FetchResult(
        head_uuid=head_uuid,
        uuids_newest_first=fetched,
        pages_fetched=pages_fetched,
        reached_watermark=reached_watermark,
    )


def _atomic_write_text(path: Path, content: str) -> None:
    """Write a UTF-8 file via fsync and same-directory atomic replacement."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
    )
    temp_path = Path(temp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, path)
    except BaseException:
        try:
            temp_path.unlink()
        except FileNotFoundError:
            pass
        raise


def write_uuid_file_atomic(output_file: Path, uuids: list[str]) -> None:
    content = "".join(f"{uuid}\n" for uuid in uuids)
    _atomic_write_text(output_file, content)


def write_state_atomic(state_file: Path, watermark_uuid: str | None) -> None:
    content = json.dumps(
        {
            "version": STATE_VERSION,
            "watermark_uuid": watermark_uuid,
        },
        ensure_ascii=False,
        separators=(",", ":"),
    )
    _atomic_write_text(state_file, content + "\n")


async def sync_uuid_file(
    lobby: RecordListClient,
    output_file: Path,
    state_file: Path,
    *,
    page_size: int = PAGE_SIZE,
) -> SyncSummary:
    """Fetch and atomically commit new UUIDs and the next watermark."""
    existing = load_existing_uuids(output_file)
    existing_set = set(existing)
    watermark, warning = load_watermark(state_file, existing_set)
    if warning:
        print(f"Warning: {warning}", file=sys.stderr)

    result = await fetch_uuids_since(
        lobby,
        watermark,
        page_size=page_size,
    )

    new_newest_first = [
        uuid for uuid in result.uuids_newest_first if uuid not in existing_set
    ]
    new_oldest_first = list(reversed(new_newest_first))
    final_uuids = existing + new_oldest_first

    write_uuid_file_atomic(output_file, final_uuids)
    try:
        write_state_atomic(state_file, result.head_uuid)
    except Exception as exc:
        raise SyncError(
            "the UUID file was updated but its sync state was not; rerun safely"
        ) from exc

    return SyncSummary(
        added=len(new_oldest_first),
        total=len(final_uuids),
        pages_fetched=result.pages_fetched,
        reached_watermark=result.reached_watermark,
    )


async def sync_from_account(
    config: AccountConfig,
    output_file: Path,
    state_file: Path,
) -> SyncSummary:
    """Connect, log in, synchronize, and always close the RPC channel."""
    client = CurrentMajsoulAccountClient()
    await client.start()
    try:
        await client.login(config.username, config.password)
        assert client.lobby is not None
        return await sync_uuid_file(
            client.lobby,
            output_file,
            state_file,
        )
    finally:
        await client.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Incrementally synchronize Majsoul account game UUIDs"
    )
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        default=DEFAULT_OUTPUT_FILE,
        help="UUID output file (default: todo.txt)",
    )
    parser.add_argument(
        "--state",
        type=Path,
        help="sync state file (default: <output>.state.json)",
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        default=DEFAULT_ENV_FILE,
        help="account configuration file (default: project-root .env)",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    state_file = args.state or default_state_path(args.output)

    if args.output.resolve() == state_file.resolve():
        print("Error: --output and --state must be different files", file=sys.stderr)
        return 2
    if args.output.resolve() == args.env_file.resolve():
        print("Error: --output must not overwrite the configuration file", file=sys.stderr)
        return 2
    if state_file.resolve() == args.env_file.resolve():
        print("Error: --state must not overwrite the configuration file", file=sys.stderr)
        return 2

    try:
        config = load_account_config(args.env_file)
        summary = asyncio.run(
            sync_from_account(
                config,
                args.output,
                state_file,
            )
        )
    except ConfigError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 2
    except MajsoulLoginError as exc:
        print(f"Error: {describe_login_error(exc)}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("Interrupted; rerun the command to resume safely", file=sys.stderr)
        return 130
    except Exception as exc:
        print(f"Error: UUID synchronization failed: {exc}", file=sys.stderr)
        return 1

    print(
        "UUID synchronization complete: "
        f"{summary.added} new, {summary.total} stored, "
        f"{summary.pages_fetched} page(s) checked"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
