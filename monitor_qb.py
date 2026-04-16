import argparse
import copy
import datetime
import html
import json
import logging
import os
import queue
import re
import signal
import subprocess
import sys
import threading
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from http.cookiejar import CookieJar
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any


SCRIPT_DIR = Path(__file__).resolve().parent
SETTINGS_FILE = SCRIPT_DIR / "settings.json"
DEFAULT_SETTINGS = {
    "qbittorrent": {
        "host": "http://192.168.0.205:3117",
        "username": "admin",
        "password": "H0rseshoe",
    },
    "listener": {
        "host": "127.0.0.1",
        "port": 7981,
        "workers": 4,
    },
    "jobs": {
        "directory": r"D:\Auto\upload\jobs",
    },
    "logging": {
        "path": r"C:\auto\upload\webhook_monitor.log",
        "level": "all",
        "maxSizeMb": 10,
    },
    "uploaderLogging": {
        "path": r"C:\auto\upload\torrent_upload.log",
        "level": "all",
        "maxSizeMb": 10,
    },
    "uploader": {
        "mode": "connect-test",
        "maxRunHours": 6,
        "scheduledStartTimeLocal": "01:00",
        "pauseMonitorProcessing": False,
        "pauseSetAtUtc": "",
    },
    "statusDashboard": {
        "enabled": True,
        "host": "192.168.0.205",
        "port": 7982,
        "refreshSeconds": 10,
        "logTailLines": 150,
        "maxListedJobs": 25,
    },
}


@dataclass(frozen=True)
class AppConfig:
    qbit_host: str
    qbit_username: str
    qbit_password: str
    job_dir: Path
    listener_host: str
    listener_port: int
    settings_path: Path


SENTINEL: dict[str, Any] = {"__shutdown__": True}
LOGGER = logging.getLogger("webhook_monitor")


def utc_now() -> datetime.datetime:
    return datetime.datetime.now(datetime.UTC)


def format_utc_timestamp(value: datetime.datetime) -> str:
    return value.astimezone(datetime.UTC).isoformat().replace("+00:00", "Z")


def format_duration(delta: datetime.timedelta) -> str:
    total_seconds = int(delta.total_seconds())
    sign = "-" if total_seconds < 0 else ""
    remaining = abs(total_seconds)
    days, remaining = divmod(remaining, 86400)
    hours, remaining = divmod(remaining, 3600)
    minutes, seconds = divmod(remaining, 60)

    parts: list[str] = []
    if days:
        parts.append(f"{days}d")
    if hours:
        parts.append(f"{hours}h")
    if minutes:
        parts.append(f"{minutes}m")
    if seconds or not parts:
        parts.append(f"{seconds}s")
    return f"{sign}{' '.join(parts[:3])}"


def format_bytes(size: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    value = float(size)
    for unit in units:
        if value < 1024 or unit == units[-1]:
            return f"{value:.1f} {unit}"
        value /= 1024
    return f"{size} B"


def summarize_text(lines: list[str], fallback: str) -> str:
    if not lines:
        return fallback
    return "\n".join(lines)


class MonitorRuntimeState:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._state: dict[str, Any] = {
            "monitorStartedAtUtc": format_utc_timestamp(utc_now()),
            "lastWebhookReceivedAtUtc": "",
            "lastQueuedTorrentHash": "",
            "lastProcessedTorrentHash": "",
            "lastProcessedAtUtc": "",
            "lastCreatedJobAtUtc": "",
            "lastCreatedJobPath": "",
            "lastCreatedTorrentHash": "",
            "processingPaused": False,
            "nextUploadStartLocal": "",
            "uploaderActive": False,
            "uploaderMode": "",
            "uploaderPid": 0,
            "uploaderStartedAtUtc": "",
            "uploaderDeadlineUtc": "",
            "uploaderLastFinishedAtUtc": "",
            "uploaderLastExitCode": None,
            "workerStates": {},
        }

    def update(self, **kwargs: Any) -> None:
        with self._lock:
            self._state.update(kwargs)

    def set_worker_state(self, worker_name: str, status: str, torrent_hash: str = "") -> None:
        with self._lock:
            worker_states = dict(self._state.get("workerStates", {}))
            worker_states[worker_name] = {
                "status": status,
                "torrentHash": torrent_hash,
                "updatedAtUtc": format_utc_timestamp(utc_now()),
            }
            self._state["workerStates"] = worker_states

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return copy.deepcopy(self._state)


def is_process_running(pid: int) -> bool:
    if pid <= 0:
        return False

    if os.name == "nt":
        try:
            completed = subprocess.run(
                ["tasklist", "/FI", f"PID eq {pid}", "/FO", "CSV", "/NH"],
                capture_output=True,
                text=True,
                check=False,
            )
        except OSError:
            return False
        output = completed.stdout.strip()
        return bool(output) and "No tasks are running" not in output and "INFO:" not in output

    try:
        os.kill(pid, 0)
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def load_json_file(path: Path) -> dict[str, Any] | None:
    try:
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def tail_file_lines(path: Path, line_limit: int) -> list[str]:
    if line_limit <= 0 or not path.exists() or not path.is_file():
        return []

    try:
        with path.open("r", encoding="utf-8", errors="replace") as handle:
            lines = handle.readlines()
    except OSError:
        return []
    return [line.rstrip("\r\n") for line in lines[-line_limit:]]


def parse_log_timestamp_prefix(line: str) -> datetime.datetime | None:
    if len(line) < 19:
        return None

    try:
        naive_value = datetime.datetime.strptime(line[:19], "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None
    return naive_value.replace(tzinfo=datetime.UTC)


def collect_recent_activity(settings: dict[str, Any], line_limit: int) -> list[str]:
    log_sources = [
        ("monitor", Path(str(settings.get("logging", {}).get("path", DEFAULT_SETTINGS["logging"]["path"])))),
        (
            "uploader",
            Path(
                str(
                    settings.get("uploaderLogging", {}).get(
                        "path", DEFAULT_SETTINGS["uploaderLogging"]["path"]
                    )
                )
            ),
        ),
    ]
    merged_lines: list[tuple[datetime.datetime, int, str]] = []
    order = 0
    for source_name, path in log_sources:
        for line in tail_file_lines(path, line_limit):
            timestamp = parse_log_timestamp_prefix(line) or datetime.datetime.min.replace(
                tzinfo=datetime.UTC
            )
            merged_lines.append((timestamp, order, f"[{source_name}] {line}"))
            order += 1

    merged_lines.sort(key=lambda item: (item[0], item[1]))
    return [line for _timestamp, _order, line in merged_lines[-line_limit:]]


def job_size_from_payload(payload: dict[str, Any]) -> tuple[int, int, int, str | None]:
    downloaded_files = payload.get("downloadedFiles")
    total_size_bytes = 0
    existing_files = 0
    missing_files = 0

    if isinstance(downloaded_files, list) and any(
        isinstance(item, str) and item.strip() for item in downloaded_files
    ):
        seen_paths: set[str] = set()
        for raw_path in downloaded_files:
            if not isinstance(raw_path, str) or not raw_path.strip():
                continue
            file_path = Path(raw_path)
            normalized = str(file_path).lower()
            if normalized in seen_paths:
                continue
            seen_paths.add(normalized)
            if file_path.exists() and file_path.is_file():
                total_size_bytes += file_path.stat().st_size
                existing_files += 1
            else:
                missing_files += 1
        return total_size_bytes, existing_files, missing_files, None

    parent_value = str(payload.get("parentPath", "")).strip()
    if not parent_value:
        return 0, 0, 0, "Missing parentPath and downloadedFiles."

    parent_path = Path(parent_value)
    if not parent_path.exists():
        return 0, 0, 0, f"Parent path missing: {parent_path}"
    if parent_path.is_file():
        return parent_path.stat().st_size, 1, 0, None

    try:
        for file_path in parent_path.rglob("*"):
            if file_path.is_file():
                total_size_bytes += file_path.stat().st_size
                existing_files += 1
    except OSError as exc:
        return total_size_bytes, existing_files, missing_files, str(exc)

    return total_size_bytes, existing_files, missing_files, None


def summarize_job_file(job_file_path: Path, max_job_size_bytes: int) -> dict[str, Any]:
    payload = load_json_file(job_file_path)
    if payload is None:
        return {
            "jobFileName": job_file_path.name,
            "torrentName": job_file_path.stem,
            "torrentHash": "",
            "createdAtUtc": "",
            "totalSizeBytes": 0,
            "existingFileCount": 0,
            "missingFileCount": 0,
            "tooBig": False,
            "problem": "Unable to read job JSON.",
        }

    total_size_bytes, existing_files, missing_files, problem = job_size_from_payload(payload)
    torrent_name = str(payload.get("torrentName") or payload.get("releaseTitle") or job_file_path.stem)
    return {
        "jobFileName": job_file_path.name,
        "torrentName": torrent_name,
        "torrentHash": str(payload.get("torrentHash", "")),
        "createdAtUtc": str(payload.get("createdAtUtc", "")),
        "totalSizeBytes": total_size_bytes,
        "existingFileCount": existing_files,
        "missingFileCount": missing_files,
        "tooBig": total_size_bytes > max_job_size_bytes,
        "problem": problem,
    }


def load_active_upload_markers(job_dir: Path) -> list[dict[str, Any]]:
    active_uploads: list[dict[str, Any]] = []
    for marker_path in sorted(job_dir.glob("*.json.running")):
        payload = load_json_file(marker_path)
        if payload is None:
            continue

        pid = 0
        try:
            pid = int(payload.get("pid", 0))
        except Exception:
            pid = 0
        if not is_process_running(pid):
            try:
                marker_path.unlink()
            except OSError:
                pass
            continue

        payload["markerFileName"] = marker_path.name
        payload["jobFileName"] = str(payload.get("jobFileName") or marker_path.name.removesuffix(".running"))
        active_uploads.append(payload)

    active_uploads.sort(
        key=lambda item: str(item.get("torrentName") or item.get("jobFileName") or "").lower()
    )
    return active_uploads


class SizeLimitedFileHandler(logging.FileHandler):
    def __init__(self, filename: str, max_bytes: int) -> None:
        self.max_bytes = max_bytes
        Path(filename).parent.mkdir(parents=True, exist_ok=True)
        super().__init__(filename, mode="a", encoding="utf-8")

    def emit(self, record: logging.LogRecord) -> None:
        if os.path.exists(self.baseFilename) and os.path.getsize(self.baseFilename) >= self.max_bytes:
            if self.stream:
                self.stream.close()
            self.stream = open(self.baseFilename, "w", encoding=self.encoding)
        super().emit(record)


class QBittorrentClient:
    def __init__(self, host: str, username: str, password: str) -> None:
        self.base_url = host.rstrip("/")
        self.username = username
        self.password = password
        self.cookie_jar = CookieJar()
        self.opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(self.cookie_jar)
        )

    def _request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        form: dict[str, Any] | None = None,
    ) -> bytes:
        url = f"{self.base_url}{path}"
        if params:
            url = f"{url}?{urllib.parse.urlencode(params)}"

        data = None
        headers = {
            "Origin": self.base_url,
            "Referer": f"{self.base_url}/",
        }
        if form is not None:
            data = urllib.parse.urlencode(form).encode("utf-8")
            headers["Content-Type"] = "application/x-www-form-urlencoded"

        request = urllib.request.Request(url, data=data, method=method, headers=headers)
        with self.opener.open(request, timeout=15) as response:
            return response.read()

    def login(self) -> None:
        response = self._request(
            "POST",
            "/api/v2/auth/login",
            form={"username": self.username, "password": self.password},
        )
        if response.decode("utf-8", errors="replace").strip() != "Ok.":
            raise RuntimeError("qBittorrent login failed")

    def get_torrent(self, torrent_hash: str) -> dict[str, Any]:
        payload = self._request(
            "GET",
            "/api/v2/torrents/info",
            params={"hashes": torrent_hash},
        )
        torrents = json.loads(payload.decode("utf-8"))
        if not torrents:
            raise RuntimeError(f"No torrent found for hash {torrent_hash}")
        return torrents[0]

    def get_torrent_files(self, torrent_hash: str) -> list[dict[str, Any]]:
        payload = self._request(
            "GET",
            "/api/v2/torrents/files",
            params={"hash": torrent_hash},
        )
        return json.loads(payload.decode("utf-8"))

    def export_torrent_file(self, torrent_hash: str) -> bytes:
        return self._request(
            "GET",
            "/api/v2/torrents/export",
            params={"hash": torrent_hash},
        )


class WebhookRequestHandler(BaseHTTPRequestHandler):
    app_queue: queue.Queue[dict[str, Any]]
    active_hashes: set[str]
    active_hashes_lock: threading.Lock

    def do_POST(self) -> None:
        content_length = int(self.headers.get("Content-Length", "0"))
        if content_length <= 0:
            LOGGER.warning("Rejected webhook with empty request body")
            self._send_response(400, "Request body is required.\n")
            return

        body = self.rfile.read(content_length)
        try:
            payload = json.loads(body.decode("utf-8"))
        except json.JSONDecodeError:
            LOGGER.warning("Rejected webhook with invalid JSON body")
            self._send_response(400, "Request body must be valid JSON.\n")
            return

        torrent_hash = payload.get("downloadId")
        self.server.runtime_state.update(
            lastWebhookReceivedAtUtc=format_utc_timestamp(utc_now()),
            lastQueuedTorrentHash=str(torrent_hash or ""),
        )
        if not torrent_hash:
            if isinstance(payload, dict):
                payload_keys = ", ".join(sorted(str(key) for key in payload.keys()))
                LOGGER.info(
                    "Ignoring webhook without downloadId. Top-level keys: %s",
                    payload_keys or "<none>",
                )
            else:
                LOGGER.info(
                    "Ignoring webhook without downloadId. Payload type: %s",
                    type(payload).__name__,
                )
            self._send_response(200, "Ignored webhook without downloadId.\n")
            return

        if job_exists_for_torrent(Path(self.server.config.job_dir), torrent_hash):
            LOGGER.warning("Skipped webhook for existing torrent job %s", torrent_hash)
            self._send_response(200, f"Job already exists for torrent {torrent_hash}\n")
            return

        with self.active_hashes_lock:
            if torrent_hash in self.active_hashes:
                LOGGER.warning("Skipped duplicate queued webhook for torrent %s", torrent_hash)
                self._send_response(200, f"Torrent {torrent_hash} is already queued\n")
                return
            self.active_hashes.add(torrent_hash)

        self.app_queue.put(payload)
        LOGGER.info("Queued webhook for torrent %s", torrent_hash)
        print(f"Queued webhook for torrent {torrent_hash}", flush=True)
        self._send_response(202, f"Queued torrent {torrent_hash}\n")

    def do_GET(self) -> None:
        if self.path == "/health":
            self._send_response(200, "OK\n")
            return
        self._send_response(405, "Use POST requests.\n")

    def log_message(self, format: str, *args: Any) -> None:
        return

    def _send_response(self, status: int, message: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.end_headers()
        self.wfile.write(message.encode("utf-8"))


def merge_settings(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            merge_settings(base[key], value)
        else:
            base[key] = value
    return base


def load_settings(settings_path: Path) -> dict[str, Any]:
    settings = json.loads(json.dumps(DEFAULT_SETTINGS))
    if not settings_path.exists():
        return settings

    with settings_path.open("r", encoding="utf-8") as handle:
        file_settings = json.load(handle)

    if not isinstance(file_settings, dict):
        raise RuntimeError("settings.json must contain a JSON object.")

    return merge_settings(settings, file_settings)


def parse_log_level(value: str) -> int:
    normalized = value.strip().lower()
    if normalized == "all":
        return logging.INFO
    if normalized == "errors":
        return logging.WARNING
    raise RuntimeError("logging.level must be 'all' or 'errors'.")


def configure_logging(settings: dict[str, Any]) -> None:
    logging_settings = settings.get("logging", {})
    log_path = logging_settings.get("path", DEFAULT_SETTINGS["logging"]["path"])
    max_size_mb = int(logging_settings.get("maxSizeMb", DEFAULT_SETTINGS["logging"]["maxSizeMb"]))
    if max_size_mb <= 0:
        raise RuntimeError("logging.maxSizeMb must be greater than 0.")

    log_level = parse_log_level(
        str(logging_settings.get("level", DEFAULT_SETTINGS["logging"]["level"]))
    )

    handler = SizeLimitedFileHandler(log_path, max_size_mb * 1024 * 1024)
    handler.setFormatter(
        logging.Formatter(
            "%(asctime)s %(levelname)s %(threadName)s %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )

    LOGGER.handlers.clear()
    LOGGER.addHandler(handler)
    LOGGER.setLevel(log_level)
    LOGGER.propagate = False
    LOGGER.info("Logging initialized at %s with level %s and max size %s MB", log_path, logging.getLevelName(log_level), max_size_mb)


def parse_local_time(value: str) -> datetime.time:
    try:
        parsed = datetime.time.fromisoformat(value)
    except ValueError as exc:
        raise RuntimeError(
            "uploader.scheduledStartTimeLocal must be a local time like HH:MM or HH:MM:SS."
        ) from exc

    if parsed.tzinfo is not None:
        raise RuntimeError("uploader.scheduledStartTimeLocal must not include a timezone.")

    return parsed.replace(microsecond=0)


def parse_utc_timestamp(value: str) -> datetime.datetime | None:
    normalized = value.strip()
    if not normalized:
        return None

    if normalized.endswith("Z"):
        normalized = f"{normalized[:-1]}+00:00"

    parsed = datetime.datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=datetime.UTC)
    return parsed.astimezone(datetime.UTC)


def validate_uploader_settings(settings: dict[str, Any]) -> None:
    uploader_settings = settings.get("uploader", {})
    parse_local_time(
        str(
            uploader_settings.get(
                "scheduledStartTimeLocal",
                DEFAULT_SETTINGS["uploader"]["scheduledStartTimeLocal"],
            )
        )
    )
    max_run_hours = float(
        uploader_settings.get(
            "maxRunHours",
            DEFAULT_SETTINGS["uploader"]["maxRunHours"],
        )
    )
    if max_run_hours <= 0:
        raise RuntimeError("uploader.maxRunHours must be greater than 0.")


def validate_status_dashboard_settings(settings: dict[str, Any]) -> None:
    dashboard_settings = settings.get("statusDashboard", {})
    if not bool(
        dashboard_settings.get(
            "enabled",
            DEFAULT_SETTINGS["statusDashboard"]["enabled"],
        )
    ):
        return

    port = int(
        dashboard_settings.get(
            "port",
            DEFAULT_SETTINGS["statusDashboard"]["port"],
        )
    )
    refresh_seconds = int(
        dashboard_settings.get(
            "refreshSeconds",
            DEFAULT_SETTINGS["statusDashboard"]["refreshSeconds"],
        )
    )
    log_tail_lines = int(
        dashboard_settings.get(
            "logTailLines",
            DEFAULT_SETTINGS["statusDashboard"]["logTailLines"],
        )
    )
    max_listed_jobs = int(
        dashboard_settings.get(
            "maxListedJobs",
            DEFAULT_SETTINGS["statusDashboard"]["maxListedJobs"],
        )
    )
    if port <= 0 or port > 65535:
        raise RuntimeError("statusDashboard.port must be between 1 and 65535.")
    if refresh_seconds <= 0:
        raise RuntimeError("statusDashboard.refreshSeconds must be greater than 0.")
    if log_tail_lines <= 0:
        raise RuntimeError("statusDashboard.logTailLines must be greater than 0.")
    if max_listed_jobs <= 0:
        raise RuntimeError("statusDashboard.maxListedJobs must be greater than 0.")


def load_settings_document(settings_path: Path) -> dict[str, Any]:
    if not settings_path.exists():
        return {}

    with settings_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)

    if not isinstance(payload, dict):
        raise RuntimeError("settings.json must contain a JSON object.")

    return payload


def write_settings_document(settings_path: Path, payload: dict[str, Any]) -> None:
    temp_path = settings_path.with_name(
        f".{settings_path.stem}.{os.getpid()}.{threading.get_ident()}.tmp"
    )
    temp_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    os.replace(temp_path, settings_path)


def compute_next_upload_target(
        now_local: datetime.datetime,
        scheduled_time: datetime.time,
) -> datetime.datetime:
    tzinfo = now_local.tzinfo
    today_target = datetime.datetime.combine(now_local.date(), scheduled_time)
    if tzinfo is not None:
        today_target = today_target.replace(tzinfo=tzinfo)
        if now_local < today_target:
                return today_target
    next_target = datetime.datetime.combine(
        now_local.date() + datetime.timedelta(days=1),
        scheduled_time,
    )
    if tzinfo is not None:
        next_target = next_target.replace(tzinfo=tzinfo)
    return next_target


def format_local_timestamp(value: datetime.datetime | None) -> str:
        if value is None:
                return ""
        return value.strftime("%Y-%m-%d %H:%M:%S")


def compute_status_snapshot(
        config: AppConfig,
        runtime_state: MonitorRuntimeState,
        work_queue: queue.Queue[dict[str, Any]],
        active_hashes: set[str],
        active_hashes_lock: threading.Lock,
        processing_allowed: threading.Event,
) -> dict[str, Any]:
        settings = load_settings(config.settings_path)
        uploader_settings = settings.get("uploader", {})
        dashboard_settings = settings.get("statusDashboard", {})
        scheduled_time = parse_local_time(
                str(
                        uploader_settings.get(
                                "scheduledStartTimeLocal",
                                DEFAULT_SETTINGS["uploader"]["scheduledStartTimeLocal"],
                        )
                )
        )
        max_run_hours = float(
                uploader_settings.get(
                        "maxRunHours",
                        DEFAULT_SETTINGS["uploader"]["maxRunHours"],
                )
        )
        max_job_size_bytes = int(
                float(
                        uploader_settings.get(
                                "maxJobSizeGb",
                                0,
                        )
                )
                * 1024**3
        )
        refresh_seconds = int(
                dashboard_settings.get(
                        "refreshSeconds",
                        DEFAULT_SETTINGS["statusDashboard"]["refreshSeconds"],
                )
        )
        log_tail_lines = int(
                dashboard_settings.get(
                        "logTailLines",
                        DEFAULT_SETTINGS["statusDashboard"]["logTailLines"],
                )
        )
        max_listed_jobs = int(
                dashboard_settings.get(
                        "maxListedJobs",
                        DEFAULT_SETTINGS["statusDashboard"]["maxListedJobs"],
                )
        )

        runtime_snapshot = runtime_state.snapshot()
        now_utc = utc_now()
        now_local = datetime.datetime.now().astimezone()
        next_upload_target = compute_next_upload_target(now_local, scheduled_time)
        pause_requested = bool(
                uploader_settings.get(
                        "pauseMonitorProcessing",
                        DEFAULT_SETTINGS["uploader"]["pauseMonitorProcessing"],
                )
        )
        pause_started = parse_utc_timestamp(
                str(
                        uploader_settings.get(
                                "pauseSetAtUtc",
                                DEFAULT_SETTINGS["uploader"]["pauseSetAtUtc"],
                        )
                )
        )
        nominal_deadline = (
                pause_started + datetime.timedelta(hours=max_run_hours)
                if pause_started is not None
                else None
        )

        active_uploads = load_active_upload_markers(config.job_dir)
        inferred_uploader_pid = 0
        inferred_uploader_started_at = ""
        if active_uploads:
            active_pids = [int(item.get("pid", 0) or 0) for item in active_uploads if int(item.get("pid", 0) or 0) > 0]
            if active_pids:
                inferred_uploader_pid = active_pids[0]
            started_candidates = [str(item.get("startedAtUtc") or "") for item in active_uploads if str(item.get("startedAtUtc") or "")]
            if started_candidates:
                inferred_uploader_started_at = min(started_candidates)
        active_job_names = {str(item.get("jobFileName", "")) for item in active_uploads}
        job_summaries = [
                summarize_job_file(job_file_path, max_job_size_bytes)
                for job_file_path in sorted(config.job_dir.glob("*.json"))
        ]
        oversized_jobs = [job for job in job_summaries if job.get("tooBig")]
        pending_jobs = [
                job for job in job_summaries if not job.get("tooBig") and job.get("jobFileName") not in active_job_names
        ]

        with active_hashes_lock:
                queued_hash_count = len(active_hashes)

        effective_uploader_active = bool(active_uploads) or bool(runtime_snapshot.get("uploaderActive")) or pause_requested
        effective_uploader_pid = int(runtime_snapshot.get("uploaderPid") or inferred_uploader_pid or 0)
        effective_uploader_started_at = str(runtime_snapshot.get("uploaderStartedAtUtc") or inferred_uploader_started_at or "")
        status_label = "Uploading" if effective_uploader_active else "Monitoring"
        upload_window_remaining = ""
        upload_window_overdue = ""
        if nominal_deadline is not None:
                if now_utc <= nominal_deadline:
                        upload_window_remaining = format_duration(nominal_deadline - now_utc)
                else:
                        upload_window_overdue = format_duration(now_utc - nominal_deadline)

        activity_lines = collect_recent_activity(settings, log_tail_lines)

        return {
                "generatedAtUtc": format_utc_timestamp(now_utc),
                "generatedAtLocal": format_local_timestamp(now_local),
                "statusLabel": status_label,
                "refreshSeconds": refresh_seconds,
                "listener": {
                        "host": config.listener_host,
                        "port": config.listener_port,
                },
                "dashboard": {
                        "host": str(
                                dashboard_settings.get(
                                        "host",
                                        DEFAULT_SETTINGS["statusDashboard"]["host"],
                                )
                        ),
                        "port": int(
                                dashboard_settings.get(
                                        "port",
                                        DEFAULT_SETTINGS["statusDashboard"]["port"],
                                )
                        ),
                },
                "counts": {
                        "queuedWebhooks": work_queue.qsize(),
                        "queuedTorrentHashes": queued_hash_count,
                        "pendingJobs": len(pending_jobs),
                        "oversizedJobs": len(oversized_jobs),
                        "activeUploads": len(active_uploads),
                },
                "timeInfo": {
                        "monitorStartedAtUtc": runtime_snapshot.get("monitorStartedAtUtc", ""),
                        "nextUploadStartLocal": format_local_timestamp(next_upload_target),
                        "timeUntilNextUpload": format_duration(next_upload_target - now_local),
                        "uploaderPauseStartedAtUtc": format_utc_timestamp(pause_started) if pause_started else "",
                        "nominalUploadDeadlineUtc": format_utc_timestamp(nominal_deadline) if nominal_deadline else "",
                        "nominalUploadTimeRemaining": upload_window_remaining,
                        "nominalUploadTimeOverdue": upload_window_overdue,
                        "uploaderLastFinishedAtUtc": str(runtime_snapshot.get("uploaderLastFinishedAtUtc") or ""),
                        "lastWebhookReceivedAtUtc": str(runtime_snapshot.get("lastWebhookReceivedAtUtc") or ""),
                        "lastCreatedJobAtUtc": str(runtime_snapshot.get("lastCreatedJobAtUtc") or ""),
                },
                "uploader": {
                        "mode": str(
                                uploader_settings.get(
                                        "mode",
                                        DEFAULT_SETTINGS["uploader"]["mode"],
                                )
                        ),
                        "pauseRequested": pause_requested,
                        "processingPaused": not processing_allowed.is_set(),
                        "maxRunHours": max_run_hours,
                        "continuePastTimeoutJobCount": int(
                                uploader_settings.get(
                                        "continuePastTimeoutJobCount",
                                        DEFAULT_SETTINGS["uploader"].get("continuePastTimeoutJobCount", 0),
                                )
                        ),
                        "uploaderActive": effective_uploader_active,
                        "uploaderPid": effective_uploader_pid,
                        "uploaderStartedAtUtc": effective_uploader_started_at,
                        "uploaderDeadlineUtc": str(runtime_snapshot.get("uploaderDeadlineUtc") or ""),
                        "uploaderLastExitCode": runtime_snapshot.get("uploaderLastExitCode"),
                },
                "workers": runtime_snapshot.get("workerStates", {}),
                "activeUploads": active_uploads[:max_listed_jobs],
                "pendingJobs": pending_jobs[:max_listed_jobs],
                "oversizedJobs": oversized_jobs[:max_listed_jobs],
                "activityLines": activity_lines,
        }


def render_dashboard_html(status: dict[str, Any]) -> str:
        def escape(value: Any) -> str:
                return html.escape(str(value))

        def render_job_rows(rows: list[dict[str, Any]], empty_message: str) -> str:
                if not rows:
                        return f'<div class="empty-state">{escape(empty_message)}</div>'

                rendered: list[str] = []
                for row in rows:
                        size_text = format_bytes(int(row.get("totalSizeBytes", 0)))
                        details = [
                                f"Size {size_text}",
                                f"Files {int(row.get('existingFileCount', 0))}",
                        ]
                        missing_count = int(row.get("missingFileCount", 0))
                        if missing_count:
                                details.append(f"Missing {missing_count}")
                        problem = str(row.get("problem") or "")
                        if problem:
                                details.append(problem)
                        rendered.append(
                                "".join(
                                        [
                                                '<div class="job-row">',
                                    '<div class="row-main">',
                                    f'<div class="job-title">{escape(row.get("torrentName", row.get("jobFileName", "")))}</div>',
                                    f'<div class="job-meta">{escape(" | ".join(details))}</div>',
                                    '</div>',
                                    f'<div class="job-file">{escape(row.get("jobFileName", ""))}</div>',
                                                '</div>',
                                        ]
                                )
                        )
                return "".join(rendered)

        active_uploads = status.get("activeUploads", [])
        if active_uploads:
                active_upload_markup = []
                for upload in active_uploads:
                        percent_complete = int(upload.get("percentComplete") or 0)
                        current_file_percent = int(upload.get("currentFilePercent") or 0)
                        started_at = str(upload.get("startedAtUtc") or "")
                        updated_at = str(upload.get("lastUpdatedUtc") or "")
                        phase = str(upload.get("phase") or upload.get("status") or "running")
                        message = str(upload.get("message") or "")
                        meta_parts = [
                                f"Overall {percent_complete}%",
                                f"Content {format_bytes(int(upload.get("transferredBytes") or 0))} / {format_bytes(int(upload.get("totalBytes") or 0))}",
                                f"Files {int(upload.get("completedFiles") or 0)} / {int(upload.get("totalFiles") or 0)}",
                        ]
                        if started_at:
                                meta_parts.append(f"Started {started_at}")
                        if updated_at:
                                meta_parts.append(f"Updated {updated_at}")
                        active_upload_markup.append(
                                "".join(
                                        [
                                                '<div class="upload-card">',
                                    '<div class="upload-header">',
                                    f'<div class="upload-title">{escape(upload.get("torrentName") or upload.get("jobFileName") or "Upload")}</div>',
                                    f'<div class="upload-phase">{escape(phase)}</div>',
                                    '</div>',
                                    '<div class="progress-track compact"><div class="progress-fill" style="width:',
                                    escape(percent_complete),
                                    '%"></div></div>',
                                    '<div class="upload-grid">',
                                    f'<div class="upload-meta">{escape(" | ".join(meta_parts))}</div>',
                                    f'<div class="upload-file">Current: {escape(upload.get("currentFileName") or "idle")}</div>',
                                    f'<div class="upload-file">File progress: {escape(current_file_percent)}%</div>',
                                    f'<div class="upload-message">{escape(message or "Running")}</div>',
                                    '</div>',
                                                '</div>',
                                        ]
                                )
                        )
                active_upload_section = "".join(active_upload_markup)
        else:
                active_upload_section = '<div class="empty-state">No active uploads right now.</div>'

        worker_markup = []
        for worker_name, worker_state in sorted(status.get("workers", {}).items()):
                worker_markup.append(
                        "".join(
                                [
                                        '<div class="worker-row">',
                        f'<span class="worker-name">{escape(worker_name)}</span>',
                        f'<span class="worker-status">{escape(worker_state.get("status", "idle"))}</span>',
                        f'<span class="worker-hash">{escape(worker_state.get("torrentHash", ""))}</span>',
                                        '</div>',
                                ]
                        )
                )
        worker_section = "".join(worker_markup) or '<div class="empty-state">No worker activity recorded yet.</div>'
        activity_text = summarize_text(list(reversed(list(status.get("activityLines", [])))), "No activity yet.")

        time_info = status.get("timeInfo", {})
        uploader_info = status.get("uploader", {})
        counts = status.get("counts", {})
        return """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta http-equiv="refresh" content='""" + escape(status.get("refreshSeconds", 10)) + """'>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Tor Monitor Status</title>
    <style>
        :root {
            color-scheme: dark;
            --bg: #0d1b1e;
            --panel: #13262c;
            --panel-alt: #17323a;
            --line: #2c4a53;
            --text: #edf6f4;
            --muted: #9db8b1;
            --accent: #57c785;
            --warn: #f2b84b;
            --danger: #ef6b73;
            --shadow: rgba(0, 0, 0, 0.24);
        }
        * { box-sizing: border-box; }
        body {
            margin: 0;
            font-family: "Segoe UI", "Trebuchet MS", sans-serif;
            background: radial-gradient(circle at top left, #16333b 0%, var(--bg) 48%), linear-gradient(180deg, #0b1619 0%, var(--bg) 100%);
            color: var(--text);
        }
        .page {
            max-width: 1680px;
            margin: 0 auto;
            padding: 24px;
        }
        .hero, .panel {
            background: rgba(19, 38, 44, 0.92);
            border: 1px solid var(--line);
            border-radius: 18px;
            box-shadow: 0 20px 40px var(--shadow);
        }
        .hero {
            padding: 24px;
            margin-bottom: 18px;
        }
        .hero h1 {
            margin: 0 0 8px;
            font-size: 32px;
            letter-spacing: 0.03em;
        }
        .hero-subtitle {
            color: var(--muted);
            font-size: 15px;
        }
        .status-badge {
            display: inline-flex;
            align-items: center;
            gap: 10px;
            margin-top: 14px;
            padding: 8px 14px;
            border-radius: 999px;
            font-weight: 700;
            background: rgba(87, 199, 133, 0.12);
            border: 1px solid rgba(87, 199, 133, 0.35);
        }
        .status-badge.uploading {
            background: rgba(242, 184, 75, 0.14);
            border-color: rgba(242, 184, 75, 0.4);
        }
        .grid {
            display: grid;
            gap: 18px;
        }
        .summary-grid {
            grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
            margin-bottom: 18px;
        }
        .panel {
            padding: 18px;
        }
        .metric-label {
            color: var(--muted);
            font-size: 13px;
            text-transform: uppercase;
            letter-spacing: 0.08em;
        }
        .metric-value {
            margin-top: 8px;
            font-size: 28px;
            font-weight: 700;
        }
        .metric-detail {
            margin-top: 10px;
            color: var(--muted);
            font-size: 14px;
            line-height: 1.5;
        }
        .main-stack {
            margin-bottom: 18px;
        }
        .section-title {
            margin: 0 0 14px;
            font-size: 18px;
            letter-spacing: 0.03em;
        }
        .upload-card, .job-row, .worker-row {
            padding: 10px 12px;
            border-radius: 12px;
            background: var(--panel-alt);
            border: 1px solid rgba(157, 184, 177, 0.12);
            margin-bottom: 8px;
        }
        .upload-title, .job-title {
            font-size: 15px;
            font-weight: 700;
            margin-bottom: 4px;
        }
        .upload-phase {
            color: var(--warn);
            font-size: 12px;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            text-align: right;
        }
        .upload-meta, .job-meta, .job-file, .upload-file, .upload-message, .worker-status, .worker-hash {
            color: var(--muted);
            font-size: 12px;
            line-height: 1.35;
            word-break: break-word;
        }
        .upload-header, .job-row {
            display: grid;
            gap: 10px;
            align-items: start;
        }
        .upload-header {
            grid-template-columns: minmax(0, 1fr) auto;
            margin-bottom: 6px;
        }
        .upload-grid {
            display: grid;
            grid-template-columns: 1.3fr 1fr 120px 1fr;
            gap: 10px;
            align-items: start;
        }
        .job-row {
            grid-template-columns: minmax(0, 1.45fr) minmax(260px, 0.95fr);
        }
        .row-main {
            min-width: 0;
        }
        .progress-track {
            margin: 8px 0;
            width: 100%;
            height: 12px;
            border-radius: 999px;
            background: rgba(255, 255, 255, 0.08);
            overflow: hidden;
        }
        .progress-track.compact {
            height: 10px;
            margin: 6px 0 8px;
        }
        .progress-fill {
            height: 100%;
            border-radius: 999px;
            background: linear-gradient(90deg, #2fce93 0%, #7ae69d 100%);
        }
        .log-box {
            min-height: 320px;
            max-height: 460px;
            overflow-y: auto;
            white-space: pre-wrap;
            font-family: Consolas, "Courier New", monospace;
            font-size: 12px;
            line-height: 1.45;
            background: #091114;
            border: 1px solid var(--line);
            border-radius: 14px;
            padding: 14px;
            color: #d8e4df;
        }
        .details-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
            gap: 10px 18px;
            margin-bottom: 16px;
        }
        .subsection-title {
            margin: 16px 0 10px;
            color: var(--muted);
            font-size: 12px;
            text-transform: uppercase;
            letter-spacing: 0.08em;
        }
        .worker-row {
            display: grid;
            grid-template-columns: 110px 130px minmax(0, 1fr);
            gap: 10px;
            align-items: center;
        }
        .worker-name {
            font-weight: 700;
        }
        .empty-state {
            padding: 16px;
            border-radius: 14px;
            background: rgba(255, 255, 255, 0.03);
            color: var(--muted);
            border: 1px dashed rgba(157, 184, 177, 0.22);
        }
        @media (max-width: 1200px) {
            .upload-grid,
            .job-row {
                grid-template-columns: 1fr;
            }
        }
        @media (max-width: 720px) {
            .page {
                padding: 14px;
            }
            .hero h1 {
                font-size: 26px;
            }
            .summary-grid,
            .details-grid,
            .worker-row {
                grid-template-columns: 1fr;
            }
        }
    </style>
</head>
<body>
    <div class="page">
        <section class="hero">
            <h1>Tor Monitor Status</h1>
            <div class="hero-subtitle">Updated """ + escape(status.get("generatedAtLocal", "")) + """ local. Auto refresh every """ + escape(status.get("refreshSeconds", 10)) + """s.</div>
            <div class="status-badge """ + ("uploading" if status.get("statusLabel") == "Uploading" else "") + """>""" + escape(status.get("statusLabel", "Unknown")) + """</div>
        </section>

        <section class="grid summary-grid">
            <article class="panel">
                <div class="metric-label">Next Upload Window</div>
                <div class="metric-value">""" + escape(time_info.get("nextUploadStartLocal", "")) + """</div>
                <div class="metric-detail">Starts in """ + escape(time_info.get("timeUntilNextUpload", "")) + """.</div>
            </article>
            <article class="panel">
                <div class="metric-label">Webhook Queue</div>
                <div class="metric-value">""" + escape(counts.get("queuedWebhooks", 0)) + """</div>
                <div class="metric-detail">Queued unique hashes: """ + escape(counts.get("queuedTorrentHashes", 0)) + """</div>
            </article>
            <article class="panel">
                <div class="metric-label">Upload Jobs</div>
                <div class="metric-value">""" + escape(counts.get("pendingJobs", 0)) + """ pending</div>
                <div class="metric-detail">Active uploads: """ + escape(counts.get("activeUploads", 0)) + """. Oversized jobs: """ + escape(counts.get("oversizedJobs", 0)) + """.</div>
            </article>
            <article class="panel">
                <div class="metric-label">Upload Runtime</div>
                <div class="metric-value">""" + escape(time_info.get("nominalUploadTimeRemaining", "Idle" ) or ("Over by " + str(time_info.get("nominalUploadTimeOverdue", "")) if time_info.get("nominalUploadTimeOverdue") else "Idle")) + """</div>
                <div class="metric-detail">Pause flag: """ + escape("on" if uploader_info.get("pauseRequested") else "off") + """. Continue past timeout jobs: """ + escape(uploader_info.get("continuePastTimeoutJobCount", 0)) + """.</div>
            </article>
        </section>

        <section class="grid main-stack">
            <article class="panel">
                <h2 class="section-title">Active Uploads</h2>
                """ + active_upload_section + """
            </article>
            <article class="panel">
                <h2 class="section-title">Pending Upload Jobs</h2>
                """ + render_job_rows(list(status.get("pendingJobs", [])), "No pending upload jobs.") + """
            </article>
            <article class="panel">
                <h2 class="section-title">Oversized Jobs</h2>
                """ + render_job_rows(list(status.get("oversizedJobs", [])), "No jobs exceed the automatic upload limit.") + """
            </article>
            <article class="panel">
                <h2 class="section-title">Monitor Details</h2>
                <div class="details-grid">
                    <div class="metric-detail">Listener: """ + escape(status.get("listener", {}).get("host", "")) + ":" + escape(status.get("listener", {}).get("port", "")) + """</div>
                    <div class="metric-detail">Dashboard: """ + escape(status.get("dashboard", {}).get("host", "")) + ":" + escape(status.get("dashboard", {}).get("port", "")) + """</div>
                    <div class="metric-detail">Monitor started: """ + escape(time_info.get("monitorStartedAtUtc", "")) + """</div>
                    <div class="metric-detail">Last webhook: """ + escape(time_info.get("lastWebhookReceivedAtUtc", "n/a")) + """</div>
                    <div class="metric-detail">Last job written: """ + escape(time_info.get("lastCreatedJobAtUtc", "n/a")) + """</div>
                    <div class="metric-detail">Uploader started: """ + escape(uploader_info.get("uploaderStartedAtUtc", "n/a")) + """</div>
                    <div class="metric-detail">Uploader PID: """ + escape(uploader_info.get("uploaderPid", 0)) + """</div>
                    <div class="metric-detail">Last uploader exit code: """ + escape(uploader_info.get("uploaderLastExitCode", "n/a")) + """</div>
                </div>
                <div class="subsection-title">Worker Activity</div>
                """ + worker_section + """
            </article>
            <article class="panel">
                <h2 class="section-title">Recent Activity</h2>
                <div class="log-box">""" + escape(activity_text) + """</div>
            </article>
        </section>
    </div>
</body>
</html>
"""


class StatusRequestHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
                if self.path == "/health":
                        self._send_text(200, "OK\n")
                        return

                if self.path == "/api/status":
                        payload = compute_status_snapshot(
                                self.server.config,
                                self.server.runtime_state,
                                self.server.work_queue,
                                self.server.active_hashes,
                                self.server.active_hashes_lock,
                                self.server.processing_allowed,
                        )
                        self._send_json(200, payload)
                        return

                if self.path in {"/", "/index.html"}:
                        payload = compute_status_snapshot(
                                self.server.config,
                                self.server.runtime_state,
                                self.server.work_queue,
                                self.server.active_hashes,
                                self.server.active_hashes_lock,
                                self.server.processing_allowed,
                        )
                        self._send_html(200, render_dashboard_html(payload))
                        return

                self._send_text(404, "Not found.\n")

        def log_message(self, format: str, *args: Any) -> None:
                return

        def _send_text(self, status: int, message: str) -> None:
                self.send_response(status)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                self.end_headers()
                self.wfile.write(message.encode("utf-8"))

        def _send_html(self, status: int, message: str) -> None:
                encoded = message.encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(encoded)))
                self.end_headers()
                self.wfile.write(encoded)

        def _send_json(self, status: int, payload: dict[str, Any]) -> None:
                encoded = json.dumps(payload, indent=2).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(encoded)))
                self.end_headers()
                self.wfile.write(encoded)


def update_monitor_pause_flag(settings_path: Path, paused: bool, pause_set_at_utc: str = "") -> None:
    payload = load_settings_document(settings_path)
    uploader_settings = payload.setdefault("uploader", {})
    if not isinstance(uploader_settings, dict):
        raise RuntimeError("settings.json uploader section must be a JSON object.")

    uploader_settings["pauseMonitorProcessing"] = paused
    uploader_settings["pauseSetAtUtc"] = pause_set_at_utc if paused else ""
    write_settings_document(settings_path, payload)


def monitor_pause_loop(
    settings_path: Path,
    processing_allowed: threading.Event,
    stop_event: threading.Event,
    runtime_state: MonitorRuntimeState,
) -> None:
    last_paused_state: bool | None = None
    fallback_pause_started_at: datetime.datetime | None = None

    while not stop_event.is_set():
        try:
            settings = load_settings(settings_path)
            uploader_settings = settings.get("uploader", {})
            paused = bool(
                uploader_settings.get(
                    "pauseMonitorProcessing",
                    DEFAULT_SETTINGS["uploader"]["pauseMonitorProcessing"],
                )
            )
            max_run_hours = float(
                uploader_settings.get(
                    "maxRunHours",
                    DEFAULT_SETTINGS["uploader"]["maxRunHours"],
                )
            )
            pause_started = parse_utc_timestamp(
                str(
                    uploader_settings.get(
                        "pauseSetAtUtc",
                        DEFAULT_SETTINGS["uploader"]["pauseSetAtUtc"],
                    )
                )
            )

            if paused:
                now_utc = datetime.datetime.now(datetime.UTC)
                if pause_started is None:
                    if fallback_pause_started_at is None:
                        fallback_pause_started_at = now_utc
                    effective_pause_started = fallback_pause_started_at
                else:
                    fallback_pause_started_at = pause_started
                    effective_pause_started = pause_started

                timeout = datetime.timedelta(hours=max_run_hours + 2)
                if now_utc - effective_pause_started >= timeout:
                    LOGGER.warning(
                        "Uploader pause flag exceeded %.2f hours; clearing flag and resuming webhook processing.",
                        max_run_hours + 2,
                    )
                    update_monitor_pause_flag(settings_path, False)
                    paused = False
                    fallback_pause_started_at = None

            if paused:
                processing_allowed.clear()
                runtime_state.update(processingPaused=True)
                if last_paused_state is not True:
                    LOGGER.info("Webhook processing paused by uploader flag.")
            else:
                processing_allowed.set()
                fallback_pause_started_at = None
                runtime_state.update(processingPaused=False)
                if last_paused_state is not False:
                    LOGGER.info("Webhook processing resumed.")

            last_paused_state = paused
        except Exception:
            LOGGER.exception("Failed to refresh uploader pause state.")

        stop_event.wait(10)


def scheduler_loop(
    settings_path: Path,
    stop_event: threading.Event,
    runtime_state: MonitorRuntimeState,
) -> None:
    uploader_path = SCRIPT_DIR / "upload_qb.py"
    last_launch_date: datetime.date | None = None
    announced_target: datetime.datetime | None = None

    while not stop_event.is_set():
        try:
            settings = load_settings(settings_path)
            uploader_settings = settings.get("uploader", {})
            scheduled_time = parse_local_time(
                str(
                    uploader_settings.get(
                        "scheduledStartTimeLocal",
                        DEFAULT_SETTINGS["uploader"]["scheduledStartTimeLocal"],
                    )
                )
            )
            uploader_mode = str(
                uploader_settings.get(
                    "mode",
                    DEFAULT_SETTINGS["uploader"]["mode"],
                )
            )
            pause_requested = bool(
                uploader_settings.get(
                    "pauseMonitorProcessing",
                    DEFAULT_SETTINGS["uploader"]["pauseMonitorProcessing"],
                )
            )

            now_local = datetime.datetime.now()
            today_target = datetime.datetime.combine(now_local.date(), scheduled_time)
            launch_window_end = today_target + datetime.timedelta(minutes=1)
            runtime_state.update(
                nextUploadStartLocal=format_local_timestamp(
                    compute_next_upload_target(now_local, scheduled_time)
                )
            )

            if last_launch_date != now_local.date() and today_target <= now_local < launch_window_end:
                if pause_requested:
                    LOGGER.warning(
                        "Skipped scheduled uploader start at %s because pause flag is already active.",
                        now_local.strftime("%Y-%m-%d %H:%M:%S"),
                    )
                else:
                    LOGGER.info(
                        "Starting scheduled uploader at %s using mode %s.",
                        now_local.strftime("%Y-%m-%d %H:%M:%S"),
                        uploader_mode,
                    )
                    started_at_utc = utc_now()
                    process = subprocess.Popen(
                        [sys.executable, str(uploader_path)],
                        cwd=str(SCRIPT_DIR),
                        text=True,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                    )
                    runtime_state.update(
                        uploaderActive=True,
                        uploaderMode=uploader_mode,
                        uploaderPid=process.pid,
                        uploaderStartedAtUtc=format_utc_timestamp(started_at_utc),
                        uploaderDeadlineUtc=format_utc_timestamp(
                            started_at_utc
                            + datetime.timedelta(
                                hours=float(
                                    uploader_settings.get(
                                        "maxRunHours",
                                        DEFAULT_SETTINGS["uploader"]["maxRunHours"],
                                    )
                                )
                            )
                        ),
                    )
                    stdout_text, stderr_text = process.communicate()
                    runtime_state.update(
                        uploaderActive=False,
                        uploaderPid=0,
                        uploaderLastExitCode=process.returncode,
                        uploaderLastFinishedAtUtc=format_utc_timestamp(utc_now()),
                    )
                    if stdout_text.strip():
                        LOGGER.info("Scheduled uploader stdout:\n%s", stdout_text.strip())
                    if stderr_text.strip():
                        LOGGER.warning("Scheduled uploader stderr:\n%s", stderr_text.strip())
                    if process.returncode == 0:
                        LOGGER.info("Scheduled uploader completed successfully.")
                    else:
                        LOGGER.warning(
                            "Scheduled uploader exited with code %s.",
                            process.returncode,
                        )

                last_launch_date = now_local.date()
                announced_target = None
                continue

            if last_launch_date == now_local.date() or now_local >= launch_window_end:
                next_target = datetime.datetime.combine(
                    now_local.date() + datetime.timedelta(days=1),
                    scheduled_time,
                )
            else:
                next_target = today_target

            if announced_target != next_target:
                LOGGER.info(
                    "Next uploader run scheduled for %s local time.",
                    next_target.strftime("%Y-%m-%d %H:%M:%S"),
                )
                announced_target = next_target
        except Exception:
            LOGGER.exception("Scheduled uploader loop failed.")

        stop_event.wait(30)


def load_event_payload(json_path: Path) -> dict[str, Any]:
    with json_path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def build_file_paths(
    torrent: dict[str, Any], torrent_files: list[dict[str, Any]]
) -> tuple[str, str, list[str]]:
    content_path = (torrent.get("content_path") or "").strip()
    save_path = (torrent.get("save_path") or "").strip()

    downloaded_paths: list[str] = []
    single_file_match = (
        len(torrent_files) == 1
        and content_path
        and os.path.basename(content_path) == os.path.basename(torrent_files[0].get("name", ""))
    )

    for file_item in torrent_files:
        relative_name = file_item.get("name", "")
        if single_file_match:
            absolute_path = content_path
        elif content_path:
            normalized_relative = relative_name.replace("/", os.sep).replace("\\", os.sep)
            content_name = os.path.basename(os.path.normpath(content_path))
            if normalized_relative == content_name or normalized_relative.startswith(
                f"{content_name}{os.sep}"
            ):
                absolute_path = os.path.join(os.path.dirname(content_path), normalized_relative)
            else:
                absolute_path = os.path.join(content_path, normalized_relative)
        elif save_path:
            absolute_path = os.path.join(save_path, relative_name)
        else:
            absolute_path = relative_name
        downloaded_paths.append(os.path.normpath(absolute_path))

    if single_file_match and content_path:
        parent_path = os.path.dirname(content_path)
    elif content_path:
        parent_path = os.path.normpath(content_path)
    else:
        parent_path = os.path.normpath(save_path)

    return content_path, parent_path, downloaded_paths


def sanitize_filename(value: str) -> str:
    sanitized = re.sub(r"[^A-Za-z0-9._-]+", "_", value).strip("._")
    return sanitized or "torrent"


def job_exists_for_torrent(job_dir: Path, torrent_hash: str) -> bool:
    if not job_dir.exists():
        return False
    return any(job_dir.glob(f"*_{torrent_hash}.json")) or any(
        job_dir.glob(f"*_{torrent_hash}_*.json")
    ) or any(job_dir.glob(f"*_{torrent_hash}.torrent")) or any(
        job_dir.glob(f"*_{torrent_hash}_*.torrent")
    )


def find_existing_job_file(job_dir: Path, torrent_hash: str) -> Path | None:
    if not job_dir.exists():
        return None

    matches = list(job_dir.glob(f"*_{torrent_hash}.json")) + list(
        job_dir.glob(f"*_{torrent_hash}_*.json")
    )
    if not matches:
        return None

    return max(matches, key=lambda path: path.stat().st_mtime)


def write_job_files(
    config: AppConfig,
    payload: dict[str, Any],
    torrent: dict[str, Any],
    torrent_hash: str,
    torrent_bytes: bytes,
    content_path: str,
    parent_path: str,
    downloaded_paths: list[str],
) -> tuple[Path, Path]:
    config.job_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.datetime.now(datetime.UTC).strftime("%Y%m%dT%H%M%S%fZ")
    torrent_name = (
        torrent.get("name")
        or payload.get("release", {}).get("releaseTitle")
        or torrent_hash
    )
    base_name = f"{timestamp}_{sanitize_filename(torrent_name)}_{torrent_hash}"

    torrent_file_path = config.job_dir / f"{base_name}.torrent"
    torrent_file_path.write_bytes(torrent_bytes)

    job_file_path = config.job_dir / f"{base_name}.json"
    job_payload = {
        "createdAtUtc": timestamp,
        "torrentHash": torrent_hash,
        "torrentName": torrent.get("name"),
        "torrentFilePath": str(torrent_file_path),
        "contentPath": content_path,
        "parentPath": parent_path,
        "downloadedFiles": downloaded_paths,
        "sourcePath": payload.get("sourcePath"),
        "destinationPath": payload.get("destinationPath"),
        "releaseTitle": payload.get("release", {}).get("releaseTitle"),
        "downloadClient": payload.get("downloadClient"),
        "downloadClientType": payload.get("downloadClientType"),
        "eventType": payload.get("eventType"),
        "webhookPayload": payload,
        "qbittorrent": {
            "host": config.qbit_host,
            "username": config.qbit_username,
            "password": config.qbit_password,
            "torrentHash": torrent_hash,
            "apiVersion": "v2",
            "endpoints": {
                "login": "/api/v2/auth/login",
                "torrentInfo": "/api/v2/torrents/info",
                "torrentFiles": "/api/v2/torrents/files",
                "torrentExport": "/api/v2/torrents/export",
            },
        },
    }
    job_file_path.write_text(json.dumps(job_payload, indent=2), encoding="utf-8")
    LOGGER.info(
        "Wrote job artifacts for %s: torrent=%s json=%s",
        torrent_hash,
        torrent_file_path,
        job_file_path,
    )

    return torrent_file_path, job_file_path


def resolve_torrent_artifacts(
    payload: dict[str, Any],
    config: AppConfig,
) -> tuple[str, str, Path, Path, list[str]]:
    torrent_hash = payload.get("downloadId")
    if not torrent_hash:
        raise RuntimeError("JSON payload is missing downloadId.")

    client = QBittorrentClient(
        config.qbit_host,
        config.qbit_username,
        config.qbit_password,
    )
    client.login()
    torrent = client.get_torrent(torrent_hash)
    torrent_files = client.get_torrent_files(torrent_hash)
    torrent_bytes = client.export_torrent_file(torrent_hash)

    content_path, parent_path, downloaded_paths = build_file_paths(torrent, torrent_files)
    torrent_file_path, job_file_path = write_job_files(
        config,
        payload,
        torrent,
        torrent_hash,
        torrent_bytes,
        content_path,
        parent_path,
        downloaded_paths,
    )
    return torrent_hash, content_path, torrent_file_path, job_file_path, downloaded_paths


def process_payload(
    payload: dict[str, Any],
    config: AppConfig,
    runtime_state: MonitorRuntimeState | None = None,
) -> None:
    torrent_hash = payload.get("downloadId")
    if torrent_hash and job_exists_for_torrent(config.job_dir, torrent_hash):
        existing_job = find_existing_job_file(config.job_dir, torrent_hash)
        if existing_job is not None:
            LOGGER.warning("Skipped duplicate processing for torrent %s using existing job %s", torrent_hash, existing_job)
            print(f"Torrent hash: {torrent_hash}", flush=True)
            print(f"Job already exists: {existing_job}", flush=True)
            return

    torrent_hash, content_path, torrent_file_path, job_file_path, downloaded_paths = (
        resolve_torrent_artifacts(payload, config)
    )
    if runtime_state is not None:
        processed_at = format_utc_timestamp(utc_now())
        runtime_state.update(
            lastCreatedJobAtUtc=processed_at,
            lastCreatedJobPath=str(job_file_path),
            lastCreatedTorrentHash=torrent_hash,
            lastProcessedTorrentHash=torrent_hash,
            lastProcessedAtUtc=processed_at,
        )
    print(f"Torrent hash: {torrent_hash}", flush=True)
    print(f"Torrent file path: {torrent_file_path}", flush=True)
    print(f"Content path: {content_path or 'Unknown'}", flush=True)
    print(f"Job file: {job_file_path}", flush=True)
    print("Downloaded files:", flush=True)
    for downloaded_path in downloaded_paths:
        print(f"- {downloaded_path}", flush=True)
    LOGGER.info("Completed processing for torrent %s", torrent_hash)


def worker_loop(
    work_queue: queue.Queue[dict[str, Any]],
    config: AppConfig,
    worker_name: str,
    runtime_state: MonitorRuntimeState,
    active_hashes: set[str],
    active_hashes_lock: threading.Lock,
    processing_allowed: threading.Event,
    stop_event: threading.Event,
) -> None:
    while True:
        payload = work_queue.get()
        try:
            if payload is SENTINEL or payload.get("__shutdown__"):
                LOGGER.info("%s received shutdown sentinel", worker_name)
                runtime_state.set_worker_state(worker_name, "stopped")
                return
            while not processing_allowed.wait(timeout=1):
                if stop_event.is_set():
                    processing_allowed.set()
                    break
            torrent_hash = payload.get("downloadId", "unknown")
            runtime_state.set_worker_state(worker_name, "queued", str(torrent_hash))
            if job_exists_for_torrent(config.job_dir, torrent_hash):
                LOGGER.warning("%s skipped existing job for %s", worker_name, torrent_hash)
                print(f"{worker_name} skipping existing job for {torrent_hash}", flush=True)
                runtime_state.set_worker_state(worker_name, "duplicate-skip", str(torrent_hash))
                continue
            LOGGER.info("%s processing %s", worker_name, torrent_hash)
            print(f"{worker_name} processing {torrent_hash}", flush=True)
            runtime_state.set_worker_state(worker_name, "processing", str(torrent_hash))
            process_payload(payload, config, runtime_state)
            LOGGER.info("%s completed %s", worker_name, torrent_hash)
            print(f"{worker_name} completed {torrent_hash}", flush=True)
            runtime_state.set_worker_state(worker_name, "idle")
        except Exception as exc:
            LOGGER.exception("%s failed while processing webhook", worker_name)
            print(f"{worker_name} failed: {exc}", flush=True)
            runtime_state.set_worker_state(
                worker_name,
                "error",
                str(payload.get("downloadId", "")) if isinstance(payload, dict) else "",
            )
        finally:
            torrent_hash = payload.get("downloadId") if isinstance(payload, dict) else None
            if torrent_hash:
                with active_hashes_lock:
                    active_hashes.discard(torrent_hash)
            work_queue.task_done()


def install_signal_handlers(
    servers: list[ThreadingHTTPServer],
    stop_event: threading.Event,
) -> dict[int, Any]:
    previous_handlers: dict[int, Any] = {}

    def handle_signal(signum: int, _frame: Any) -> None:
        signal_name = signal.Signals(signum).name
        LOGGER.warning("Received %s, shutting down", signal_name)
        print(f"Received {signal_name}, shutting down.", flush=True)
        stop_event.set()
        for server in servers:
            shutdown_thread = threading.Thread(target=server.shutdown, daemon=True)
            shutdown_thread.start()

    for signum in (signal.SIGINT, signal.SIGTERM):
        try:
            previous_handlers[signum] = signal.getsignal(signum)
            signal.signal(signum, handle_signal)
        except (AttributeError, ValueError):
            continue

    return previous_handlers


def restore_signal_handlers(previous_handlers: dict[int, Any]) -> None:
    for signum, handler in previous_handlers.items():
        try:
            signal.signal(signum, handler)
        except (AttributeError, ValueError):
            continue


def run_server(
    host: str,
    port: int,
    worker_count: int,
    config: AppConfig,
) -> None:
    work_queue: queue.Queue[dict[str, Any]] = queue.Queue()
    active_hashes: set[str] = set()
    active_hashes_lock = threading.Lock()
    runtime_state = MonitorRuntimeState()
    WebhookRequestHandler.app_queue = work_queue
    WebhookRequestHandler.active_hashes = active_hashes
    WebhookRequestHandler.active_hashes_lock = active_hashes_lock

    server = ThreadingHTTPServer((host, port), WebhookRequestHandler)
    server.config = config
    server.runtime_state = runtime_state
    server.work_queue = work_queue
    server.active_hashes = active_hashes
    server.active_hashes_lock = active_hashes_lock
    stop_event = threading.Event()
    processing_allowed = threading.Event()
    server.processing_allowed = processing_allowed
    workers: list[threading.Thread] = []
    settings = load_settings(config.settings_path)
    dashboard_settings = settings.get("statusDashboard", {})
    status_server_enabled = bool(
        dashboard_settings.get(
            "enabled",
            DEFAULT_SETTINGS["statusDashboard"]["enabled"],
        )
    )
    status_server: ThreadingHTTPServer | None = None
    status_thread: threading.Thread | None = None
    pause_monitor = threading.Thread(
        target=monitor_pause_loop,
        args=(config.settings_path, processing_allowed, stop_event, runtime_state),
        name="pause-monitor",
        daemon=True,
    )
    scheduler = threading.Thread(
        target=scheduler_loop,
        args=(config.settings_path, stop_event, runtime_state),
        name="uploader-scheduler",
        daemon=True,
    )

    processing_allowed.set()
    pause_monitor.start()
    scheduler.start()

    if status_server_enabled:
        status_host = str(
            dashboard_settings.get(
                "host",
                DEFAULT_SETTINGS["statusDashboard"]["host"],
            )
        )
        status_port = int(
            dashboard_settings.get(
                "port",
                DEFAULT_SETTINGS["statusDashboard"]["port"],
            )
        )
        status_server = ThreadingHTTPServer((status_host, status_port), StatusRequestHandler)
        status_server.config = config
        status_server.runtime_state = runtime_state
        status_server.work_queue = work_queue
        status_server.active_hashes = active_hashes
        status_server.active_hashes_lock = active_hashes_lock
        status_server.processing_allowed = processing_allowed
        status_thread = threading.Thread(
            target=status_server.serve_forever,
            name="status-dashboard",
            daemon=True,
        )
        status_thread.start()
        LOGGER.info("Status dashboard listening on %s:%s", status_host, status_port)
        print(f"Status dashboard available at http://{status_host}:{status_port}", flush=True)

    for index in range(worker_count):
        worker = threading.Thread(
            target=worker_loop,
            args=(
                work_queue,
                config,
                f"worker-{index + 1}",
                runtime_state,
                active_hashes,
                active_hashes_lock,
                processing_allowed,
                stop_event,
            ),
        )
        worker.start()
        workers.append(worker)

    print(
        (
            f"Listening for HTTP POST on {host}:{port} with {worker_count} worker"
            f"{'s' if worker_count != 1 else ''}"
        ),
        flush=True,
    )
    LOGGER.info("Server listening on %s:%s with %s workers", host, port, worker_count)
    previous_handlers = install_signal_handlers(
        [server] + ([status_server] if status_server is not None else []),
        stop_event,
    )
    try:
        server.serve_forever()
    finally:
        stop_event.set()
        processing_allowed.set()
        server.shutdown()
        server.server_close()
        if status_server is not None:
            status_server.shutdown()
            status_server.server_close()
        for _ in workers:
            work_queue.put(SENTINEL)
        work_queue.join()
        for worker in workers:
            worker.join(timeout=5)
        pause_monitor.join(timeout=5)
        scheduler.join(timeout=5)
        if status_thread is not None:
            status_thread.join(timeout=5)
        restore_signal_handlers(previous_handlers)
        LOGGER.info("Server stopped")
        print("Server stopped.", flush=True)


def parse_args(settings: dict[str, Any]) -> argparse.Namespace:
    qbit_settings = settings.get("qbittorrent", {})
    listener_settings = settings.get("listener", {})
    job_settings = settings.get("jobs", {})

    parser = argparse.ArgumentParser(
        description=(
            "Process Sonarr-style webhook JSON and export qBittorrent job files. "
            "Pass a JSON file for one-shot processing, or omit it to run the webhook listener."
        )
    )
    parser.add_argument(
        "json_file",
        nargs="?",
        help="Optional path to a Sonarr JSON payload file for one-shot processing.",
    )
    parser.add_argument(
        "--qbit-host",
        default=qbit_settings.get("host", DEFAULT_SETTINGS["qbittorrent"]["host"]),
        help="Base URL of the qBittorrent Web UI.",
    )
    parser.add_argument(
        "--qbit-username",
        default=qbit_settings.get(
            "username", DEFAULT_SETTINGS["qbittorrent"]["username"]
        ),
        help="qBittorrent username.",
    )
    parser.add_argument(
        "--qbit-password",
        default=os.environ.get("QBITTORRENT_PASSWORD")
        or qbit_settings.get("password", DEFAULT_SETTINGS["qbittorrent"]["password"]),
        help="qBittorrent password. CLI overrides environment, environment overrides settings.json.",
    )
    parser.add_argument(
        "--job-dir",
        default=job_settings.get("directory", DEFAULT_SETTINGS["jobs"]["directory"]),
        help="Directory where per-torrent job artifacts will be written.",
    )
    parser.add_argument(
        "--listen-host",
        default=listener_settings.get("host", DEFAULT_SETTINGS["listener"]["host"]),
        help="Host interface for webhook listener mode.",
    )
    parser.add_argument(
        "--listen-port",
        type=int,
        default=listener_settings.get("port", DEFAULT_SETTINGS["listener"]["port"]),
        help="Port for webhook listener mode.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=listener_settings.get(
            "workers", DEFAULT_SETTINGS["listener"]["workers"]
        ),
        help="Number of background workers for queued webhook processing.",
    )
    return parser.parse_args()


def main() -> int:
    try:
        settings = load_settings(SETTINGS_FILE)
        configure_logging(settings)
        validate_uploader_settings(settings)
        validate_status_dashboard_settings(settings)
    except OSError as exc:
        print(f"Unable to read settings file {SETTINGS_FILE}: {exc}")
        return 1
    except json.JSONDecodeError as exc:
        print(f"Invalid JSON in settings file {SETTINGS_FILE}: {exc}")
        return 1
    except RuntimeError as exc:
        print(str(exc))
        return 1

    args = parse_args(settings)

    if not args.qbit_password:
        print("Missing qBittorrent password. Pass --qbit-password or set QBITTORRENT_PASSWORD.")
        return 1

    config = AppConfig(
        qbit_host=args.qbit_host,
        qbit_username=args.qbit_username,
        qbit_password=args.qbit_password,
        job_dir=Path(args.job_dir),
        listener_host=args.listen_host,
        listener_port=args.listen_port,
        settings_path=SETTINGS_FILE,
    )

    try:
        if args.json_file:
            payload = load_event_payload(Path(args.json_file))
            process_payload(payload, config)
            return 0

        run_server(args.listen_host, args.listen_port, args.workers, config)
        return 0
    except urllib.error.HTTPError as exc:
        LOGGER.exception("HTTP error from qBittorrent API")
        print(f"HTTP error from qBittorrent API: {exc.code} {exc.reason}")
        return 1
    except urllib.error.URLError as exc:
        LOGGER.exception("Unable to reach qBittorrent API")
        print(f"Unable to reach qBittorrent API: {exc.reason}")
        return 1
    except RuntimeError as exc:
        LOGGER.exception("Runtime error")
        print(str(exc))
        return 1


if __name__ == "__main__":
    sys.exit(main())