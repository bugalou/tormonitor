import argparse
import concurrent.futures
import datetime
import json
import logging
import os
import posixpath
import smtplib
import ssl
import subprocess
import threading
import time
from dataclasses import dataclass
from email.message import EmailMessage
from pathlib import Path, PureWindowsPath
from typing import Any

import paramiko
from monitor_qb import QBittorrentClient as BaseQBittorrentClient


SCRIPT_DIR = Path(__file__).resolve().parent
SETTINGS_FILE = SCRIPT_DIR / "settings.json"
DEFAULT_SETTINGS = {
    "qbittorrent": {
        "host": "http://192.168.0.205:3117",
        "username": "admin",
        "password": "H0rseshoe",
    },
    "jobs": {
        "directory": r"D:\Auto\upload\jobs",
    },
    "ssh": {
        "host": "ftl1.seedit4.me",
        "port": 2090,
        "username": "seedit4me",
        "password": "btQLo!955Alx",
        "connectTimeoutSec": 20,
    },
    "uploader": {
        "mode": "connect-test",
        "localDownloadRootPath": r"D:\Auto\Download",
        "remoteContentPath": "/home/seedit4me/torrents/qbittorrent",
        "remoteTorrentPath": "/home/seedit4me/torrents/torrentfiles",
        "maxJobSizeGb": 150,
        "maxRunHours": 6,
        "continuePastTimeoutJobCount": 0,
        "scheduledStartTimeLocal": "01:00",
        "maxConcurrentJobs": 2,
        "overwriteRemoteExisting": False,
        "resumePartialUploads": True,
        "retryAttempts": 3,
        "retryDelaySec": 5,
        "pauseMonitorProcessing": False,
        "pauseSetAtUtc": "",
        "verifyUploads": True,
    },
    "uploaderLogging": {
        "path": r"C:\auto\upload\torrent_upload.log",
        "level": "all",
        "maxSizeMb": 10,
    },
    "smtp": {
        "server": "smtp.oneillhouse.lan",
        "port": 25,
        "encryption": "none",
        "username": "",
        "password": "",
        "fromAddress": "torrent_uploader@oneillhouse.net",
        "to": "9013423498@vtext.com",
    },
}
VALID_MODES = {"connect-test", "stage", "full"}
PRINT_LOCK = threading.Lock()
LOGGER = logging.getLogger("torrent_uploader")
UPLOAD_CHUNK_SIZE_BYTES = 8 * 1024 * 1024
RUNNING_MARKER_SUFFIX = ".running"
OVERSIZED_ALERT_MARKER_SUFFIX = ".oversized-alerted"


class MissingLocalFileError(FileNotFoundError):
    pass


class UploadCancelledError(RuntimeError):
    pass


class JobAlreadyRunningError(RuntimeError):
    pass


@dataclass(frozen=True)
class SSHConfig:
    host: str
    port: int
    username: str
    password: str
    connect_timeout_sec: int


@dataclass(frozen=True)
class QBittorrentConfig:
    host: str
    username: str
    password: str


@dataclass(frozen=True)
class SMTPConfig:
    server: str
    port: int
    encryption: str
    username: str
    password: str
    from_address: str
    recipients: tuple[str, ...]

    @property
    def enabled(self) -> bool:
        return bool(self.server and self.from_address and self.recipients)


@dataclass(frozen=True)
class UploaderConfig:
    mode: str
    jobs_dir: Path
    local_download_root_path: Path
    remote_content_path: str
    remote_torrent_path: str
    max_job_size_bytes: int
    max_run_hours: float
    continue_past_timeout_job_count: int
    max_concurrent_jobs: int
    overwrite_remote_existing: bool
    resume_partial_uploads: bool
    retry_attempts: int
    retry_delay_sec: int
    verify_uploads: bool
    ssh: SSHConfig
    qbittorrent: QBittorrentConfig
    smtp: SMTPConfig


@dataclass(frozen=True)
class UploadJob:
    job_file_path: Path
    torrent_hash: str
    torrent_name: str
    torrent_file_path: Path
    parent_path: Path
    downloaded_files: tuple[Path, ...]
    total_size_bytes: int

    @property
    def remote_content_root_name(self) -> str:
        return self.parent_path.name


@dataclass(frozen=True)
class JobRuntimeControl:
    cancel_event: threading.Event
    status_tracker: "JobStatusTracker | None" = None


def running_marker_path(job_file_path: Path) -> Path:
    return job_file_path.with_name(f"{job_file_path.name}{RUNNING_MARKER_SUFFIX}")


def oversized_alert_marker_path(job_file_path: Path) -> Path:
    return job_file_path.with_name(f"{job_file_path.name}{OVERSIZED_ALERT_MARKER_SUFFIX}")


def format_utc_timestamp(value: datetime.datetime) -> str:
    return value.astimezone(datetime.UTC).isoformat().replace("+00:00", "Z")


def write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    temp_path = path.with_name(f".{path.name}.{os.getpid()}.{threading.get_ident()}.tmp")
    temp_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    os.replace(temp_path, path)


class JobStatusTracker:
    def __init__(self, marker_path: Path, job: UploadJob, mode: str) -> None:
        self.marker_path = marker_path
        self.job = job
        self.mode = mode
        self.total_bytes = max(job.total_size_bytes, 1)
        self.total_files = len(job.downloaded_files)
        self.completed_bytes = 0
        self.completed_files = 0
        self.current_file_name = ""
        self.current_file_bytes = 0
        self.current_file_transferred = 0
        self.current_remote_path = ""
        self._lock = threading.Lock()
        self._write_locked(
            {
                "status": "running",
                "phase": "starting",
                "mode": mode,
                "torrentHash": job.torrent_hash,
                "torrentName": job.torrent_name,
                "jobFileName": job.job_file_path.name,
                "jobFilePath": str(job.job_file_path),
                "torrentFileName": job.torrent_file_path.name,
                "totalBytes": job.total_size_bytes,
                "transferredBytes": 0,
                "percentComplete": 0,
                "totalFiles": self.total_files,
                "completedFiles": 0,
                "currentFileName": "",
                "currentFileBytes": 0,
                "currentFileTransferredBytes": 0,
                "currentFilePercent": 0,
                "currentRemotePath": "",
                "message": f"Picked up for {mode} upload",
            }
        )

    def _write_locked(self, updates: dict[str, Any]) -> None:
        payload = load_marker_payload(self.marker_path) or {}
        payload.setdefault("pid", os.getpid())
        payload.setdefault("startedAtUtc", format_utc_timestamp(datetime.datetime.now(datetime.UTC)))
        payload.update(updates)
        payload["lastUpdatedUtc"] = format_utc_timestamp(datetime.datetime.now(datetime.UTC))
        try:
            write_json_atomic(self.marker_path, payload)
        except OSError:
            LOGGER.warning("Failed to update running marker %s", self.marker_path)

    def update_phase(self, phase: str, message: str) -> None:
        with self._lock:
            self._write_locked(
                {
                    "status": "running",
                    "phase": phase,
                    "message": message,
                }
            )

    def start_content_file(
        self,
        local_path: Path,
        remote_path: str,
        file_size: int,
        start_offset: int,
    ) -> None:
        with self._lock:
            self.current_file_name = local_path.name
            self.current_file_bytes = file_size
            self.current_file_transferred = start_offset
            self.current_remote_path = remote_path
            overall_transferred = min(self.total_bytes, self.completed_bytes + min(start_offset, file_size))
            percent_complete = int((overall_transferred / self.total_bytes) * 100)
            current_percent = int((min(start_offset, file_size) / max(file_size, 1)) * 100)
            self._write_locked(
                {
                    "status": "running",
                    "phase": "uploading-content",
                    "currentFileName": local_path.name,
                    "currentFileBytes": file_size,
                    "currentFileTransferredBytes": min(start_offset, file_size),
                    "currentFilePercent": current_percent,
                    "currentRemotePath": remote_path,
                    "transferredBytes": overall_transferred,
                    "percentComplete": percent_complete,
                    "completedFiles": self.completed_files,
                    "message": f"Uploading {local_path.name}",
                }
            )

    def update_content_progress(
        self,
        transferred: int,
        file_size: int,
    ) -> None:
        with self._lock:
            self.current_file_transferred = min(transferred, file_size)
            overall_transferred = min(self.total_bytes, self.completed_bytes + self.current_file_transferred)
            percent_complete = int((overall_transferred / self.total_bytes) * 100)
            current_percent = int((self.current_file_transferred / max(file_size, 1)) * 100)
            self._write_locked(
                {
                    "status": "running",
                    "phase": "uploading-content",
                    "currentFileTransferredBytes": self.current_file_transferred,
                    "currentFilePercent": current_percent,
                    "transferredBytes": overall_transferred,
                    "percentComplete": percent_complete,
                    "completedFiles": self.completed_files,
                    "message": f"Uploading {self.current_file_name}",
                }
            )

    def complete_content_file(self, local_path: Path, remote_path: str, file_size: int) -> None:
        with self._lock:
            self.completed_bytes = min(self.total_bytes, self.completed_bytes + file_size)
            self.completed_files = min(self.total_files, self.completed_files + 1)
            percent_complete = int((self.completed_bytes / self.total_bytes) * 100)
            self.current_file_name = local_path.name
            self.current_file_bytes = file_size
            self.current_file_transferred = file_size
            self.current_remote_path = remote_path
            self._write_locked(
                {
                    "status": "running",
                    "phase": "uploading-content",
                    "currentFileName": local_path.name,
                    "currentFileBytes": file_size,
                    "currentFileTransferredBytes": file_size,
                    "currentFilePercent": 100,
                    "currentRemotePath": remote_path,
                    "transferredBytes": self.completed_bytes,
                    "percentComplete": percent_complete,
                    "completedFiles": self.completed_files,
                    "message": f"Uploaded {local_path.name}",
                }
            )

    def start_torrent_upload(self, torrent_file: Path, remote_path: str) -> None:
        with self._lock:
            self.current_file_name = torrent_file.name
            self.current_file_bytes = torrent_file.stat().st_size if torrent_file.exists() else 0
            self.current_file_transferred = 0
            self.current_remote_path = remote_path
            self._write_locked(
                {
                    "status": "running",
                    "phase": "uploading-torrent",
                    "currentFileName": torrent_file.name,
                    "currentFileBytes": self.current_file_bytes,
                    "currentFileTransferredBytes": 0,
                    "currentFilePercent": 0,
                    "currentRemotePath": remote_path,
                    "transferredBytes": self.completed_bytes,
                    "percentComplete": int((self.completed_bytes / self.total_bytes) * 100),
                    "completedFiles": self.completed_files,
                    "message": f"Uploading torrent metadata {torrent_file.name}",
                }
            )

    def update_torrent_progress(self, transferred: int, file_size: int) -> None:
        with self._lock:
            current_percent = int((min(transferred, file_size) / max(file_size, 1)) * 100)
            self._write_locked(
                {
                    "status": "running",
                    "phase": "uploading-torrent",
                    "currentFileTransferredBytes": min(transferred, file_size),
                    "currentFilePercent": current_percent,
                    "transferredBytes": self.completed_bytes,
                    "percentComplete": int((self.completed_bytes / self.total_bytes) * 100),
                    "completedFiles": self.completed_files,
                    "message": f"Uploading torrent metadata {self.current_file_name}",
                }
            )

    def complete_torrent_upload(self) -> None:
        with self._lock:
            self._write_locked(
                {
                    "status": "running",
                    "phase": "post-upload",
                    "currentFileTransferredBytes": self.current_file_bytes,
                    "currentFilePercent": 100,
                    "transferredBytes": self.completed_bytes,
                    "percentComplete": int((self.completed_bytes / self.total_bytes) * 100),
                    "completedFiles": self.completed_files,
                    "message": "Content upload complete. Starting cleanup.",
                }
            )

    def mark_local_cleanup(self) -> None:
        self.update_phase("local-cleanup", "Removing local files and job artifacts")

    def mark_qb_cleanup(self) -> None:
        self.update_phase("qbittorrent-cleanup", "Cleaning up qBittorrent state")

    def mark_cancelled(self, message: str) -> None:
        with self._lock:
            self._write_locked({"status": "cancelled", "phase": "cancelled", "message": message})

    def mark_failed(self, message: str) -> None:
        with self._lock:
            self._write_locked({"status": "failed", "phase": "failed", "message": message})

    def mark_completed(self) -> None:
        with self._lock:
            self._write_locked(
                {
                    "status": "completed",
                    "phase": "completed",
                    "transferredBytes": self.completed_bytes,
                    "percentComplete": int((self.completed_bytes / self.total_bytes) * 100),
                    "completedFiles": self.completed_files,
                    "message": "Upload and cleanup complete",
                }
            )


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


def load_marker_payload(marker_path: Path) -> dict[str, Any] | None:
    try:
        return json.loads(marker_path.read_text(encoding="utf-8"))
    except Exception:
        return None


def get_active_running_marker_pid(job_file_path: Path) -> int | None:
    marker_path = running_marker_path(job_file_path)
    if not marker_path.exists():
        return None

    payload = load_marker_payload(marker_path)
    pid = 0
    if isinstance(payload, dict):
        try:
            pid = int(payload.get("pid", 0))
        except Exception:
            pid = 0

    if is_process_running(pid):
        return pid

    try:
        marker_path.unlink()
    except OSError:
        pass
    return None


def acquire_job_running_marker(job_file_path: Path) -> Path:
    marker_path = running_marker_path(job_file_path)
    payload = {
        "pid": os.getpid(),
        "startedAtUtc": datetime.datetime.now(datetime.UTC).isoformat().replace("+00:00", "Z"),
    }

    while True:
        try:
            with marker_path.open("x", encoding="utf-8") as handle:
                json.dump(payload, handle, indent=2)
            return marker_path
        except FileExistsError:
            active_pid = get_active_running_marker_pid(job_file_path)
            if active_pid is not None:
                raise JobAlreadyRunningError(
                    f"Upload already running for {job_file_path.name} in PID {active_pid}"
                )


def release_job_running_marker(marker_path: Path) -> None:
    try:
        marker_path.unlink()
    except OSError:
        pass


def should_alert_for_oversized_job(job_file_path: Path) -> bool:
    marker_path = oversized_alert_marker_path(job_file_path)
    if marker_path.exists():
        return False

    try:
        marker_path.write_text(
            datetime.datetime.now(datetime.UTC).isoformat().replace("+00:00", "Z"),
            encoding="utf-8",
        )
    except OSError:
        return False
    return True


def discover_live_content_files(parent_path: Path) -> tuple[Path, ...]:
    if not parent_path.exists():
        raise RuntimeError(f"Parent path not found: {parent_path}")

    if parent_path.is_file():
        return (parent_path,)

    live_files = tuple(sorted(path for path in parent_path.rglob("*") if path.is_file()))
    if not live_files:
        raise RuntimeError(f"No content files found under parent path: {parent_path}")

    return live_files


def resolve_torrent_file_path(job_file_path: Path, payload: dict[str, Any]) -> Path:
    sibling_torrent = job_file_path.with_suffix(".torrent")
    if sibling_torrent.exists():
        return sibling_torrent

    configured_path = Path(payload["torrentFilePath"])
    return configured_path


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


class SSHSession:
    def __init__(self, config: SSHConfig) -> None:
        self.config = config
        self.client: paramiko.SSHClient | None = None
        self.sftp: paramiko.SFTPClient | None = None

    def _connect(self) -> None:
        self.close()
        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.client.connect(
            hostname=self.config.host,
            port=self.config.port,
            username=self.config.username,
            password=self.config.password,
            timeout=self.config.connect_timeout_sec,
            auth_timeout=self.config.connect_timeout_sec,
            banner_timeout=self.config.connect_timeout_sec,
        )
        transport = self.client.get_transport()
        if transport is not None:
            transport.set_keepalive(30)
            transport_socket = getattr(transport, "sock", None)
            if transport_socket is not None:
                try:
                    transport_socket.settimeout(self.config.connect_timeout_sec)
                except Exception:
                    pass
        self.sftp = self.client.open_sftp()

    def __enter__(self) -> "SSHSession":
        self._connect()
        return self

    def ensure_connected(self) -> None:
        transport = self.client.get_transport() if self.client is not None else None
        if self.sftp is None or transport is None or not transport.is_active():
            self._connect()

    def reconnect(self) -> None:
        self._connect()

    def close(self) -> None:
        if self.sftp is not None:
            try:
                self.sftp.close()
            except Exception:
                pass
            self.sftp = None
        if self.client is not None:
            try:
                self.client.close()
            except Exception:
                pass
            self.client = None

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.close()


class QBittorrentClient(BaseQBittorrentClient):
    def stop_torrent(self, torrent_hash: str) -> None:
        self._request(
            "POST",
            "/api/v2/torrents/stop",
            form={"hashes": torrent_hash},
        )

    def delete_torrent(self, torrent_hash: str, delete_files: bool = False) -> None:
        self._request(
            "POST",
            "/api/v2/torrents/delete",
            form={
                "hashes": torrent_hash,
                "deleteFiles": "true" if delete_files else "false",
            },
        )


class Notifier:
    def __init__(self, config: SMTPConfig) -> None:
        self.config = config

    def send(self, message: str) -> None:
        if not self.config.enabled:
            return

        short_message = message.strip().replace("\n", " ")[:280]
        email = EmailMessage()
        email["Subject"] = "torrent_uploader"
        email["From"] = self.config.from_address
        email["To"] = ", ".join(self.config.recipients)
        email.set_content(short_message)

        try:
            encryption = self.config.encryption.lower()
            if encryption == "ssl":
                with smtplib.SMTP_SSL(
                    self.config.server,
                    self.config.port,
                    timeout=20,
                    context=ssl.create_default_context(),
                ) as server:
                    if self.config.username:
                        server.login(self.config.username, self.config.password)
                    server.send_message(email)
            else:
                with smtplib.SMTP(self.config.server, self.config.port, timeout=20) as server:
                    if encryption == "starttls":
                        server.starttls(context=ssl.create_default_context())
                    elif encryption != "none":
                        raise RuntimeError(
                            "smtp.encryption must be one of: none, starttls, ssl"
                        )
                    if self.config.username:
                        server.login(self.config.username, self.config.password)
                    server.send_message(email)
            LOGGER.info("Sent notification: %s", short_message)
        except Exception:
            LOGGER.exception("Failed to send notification: %s", short_message)


class UploadProgress:
    def __init__(
        self,
        job_name: str,
        file_name: str,
        total_bytes: int,
        progress_callback: Any | None = None,
    ) -> None:
        self.job_name = job_name
        self.file_name = file_name
        self.total_bytes = max(total_bytes, 1)
        self._last_bucket = -1
        self.progress_callback = progress_callback

    def callback(self, transferred: int, total: int) -> None:
        effective_total = total or self.total_bytes
        percent = int((transferred / max(effective_total, 1)) * 100)
        bucket = 100 if percent >= 100 else (percent // 10) * 10
        if bucket <= self._last_bucket:
            return
        self._last_bucket = bucket
        if self.progress_callback is not None:
            self.progress_callback(transferred, effective_total)
        print_status(
            f"[{self.job_name}] {self.file_name}: {bucket}% ({format_bytes(transferred)}/{format_bytes(effective_total)})"
        )


def print_status(message: str) -> None:
    with PRINT_LOCK:
        print(message, flush=True)


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


def update_monitor_pause_flag(settings_path: Path, paused: bool) -> None:
    payload = load_settings_document(settings_path)
    uploader_settings = payload.setdefault("uploader", {})
    if not isinstance(uploader_settings, dict):
        raise RuntimeError("settings.json uploader section must be a JSON object.")

    uploader_settings["pauseMonitorProcessing"] = paused
    uploader_settings["pauseSetAtUtc"] = (
        datetime.datetime.now(datetime.UTC).isoformat().replace("+00:00", "Z")
        if paused
        else ""
    )
    write_settings_document(settings_path, payload)


def parse_log_level(value: str) -> int:
    normalized = value.strip().lower()
    if normalized == "all":
        return logging.INFO
    if normalized == "errors":
        return logging.WARNING
    raise RuntimeError("uploaderLogging.level must be 'all' or 'errors'.")


def configure_logging(settings: dict[str, Any]) -> None:
    logging_settings = settings.get("uploaderLogging", {})
    log_path = logging_settings.get("path", DEFAULT_SETTINGS["uploaderLogging"]["path"])
    max_size_mb = int(
        logging_settings.get("maxSizeMb", DEFAULT_SETTINGS["uploaderLogging"]["maxSizeMb"])
    )
    if max_size_mb <= 0:
        raise RuntimeError("uploaderLogging.maxSizeMb must be greater than 0.")

    log_level = parse_log_level(
        str(logging_settings.get("level", DEFAULT_SETTINGS["uploaderLogging"]["level"]))
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
    LOGGER.info(
        "Uploader logging initialized at %s with level %s and max size %s MB",
        log_path,
        logging.getLevelName(log_level),
        max_size_mb,
    )


def parse_args(settings: dict[str, Any]) -> argparse.Namespace:
    jobs_settings = settings.get("jobs", {})
    qbit_settings = settings.get("qbittorrent", {})
    ssh_settings = settings.get("ssh", {})
    uploader_settings = settings.get("uploader", {})

    parser = argparse.ArgumentParser(
        description="Process upload job JSON files in staged phases for remote torrent upload."
    )
    parser.add_argument(
        "--mode",
        choices=sorted(VALID_MODES),
        default=uploader_settings.get("mode", DEFAULT_SETTINGS["uploader"]["mode"]),
        help="Execution mode. connect-test is non-destructive. stage uploads without cleanup.",
    )
    parser.add_argument(
        "--job-dir",
        default=jobs_settings.get("directory", DEFAULT_SETTINGS["jobs"]["directory"]),
        help="Directory containing upload job JSON files.",
    )
    parser.add_argument(
        "--max-job-size-gb",
        type=float,
        default=uploader_settings.get(
            "maxJobSizeGb", DEFAULT_SETTINGS["uploader"]["maxJobSizeGb"]
        ),
        help="Skip jobs whose content files exceed this total size.",
    )
    parser.add_argument(
        "--max-run-hours",
        type=float,
        default=uploader_settings.get(
            "maxRunHours", DEFAULT_SETTINGS["uploader"]["maxRunHours"]
        ),
        help="Maximum runtime for stage/full mode before active jobs are canceled or allowed to overrun.",
    )
    parser.add_argument(
        "--continue-past-timeout-job-count",
        type=int,
        default=uploader_settings.get(
            "continuePastTimeoutJobCount",
            DEFAULT_SETTINGS["uploader"]["continuePastTimeoutJobCount"],
        ),
        help="Number of active jobs allowed to continue running past the runtime limit until they complete.",
    )
    parser.add_argument(
        "--max-concurrent-jobs",
        type=int,
        default=uploader_settings.get(
            "maxConcurrentJobs", DEFAULT_SETTINGS["uploader"]["maxConcurrentJobs"]
        ),
        help="Maximum number of jobs to upload at once.",
    )
    parser.add_argument(
        "--local-download-root-path",
        default=uploader_settings.get(
            "localDownloadRootPath",
            DEFAULT_SETTINGS["uploader"]["localDownloadRootPath"],
        ),
        help="Local torrent download root used to preserve relative content paths on the remote server.",
    )
    parser.add_argument(
        "--remote-content-path",
        default=uploader_settings.get(
            "remoteContentPath", DEFAULT_SETTINGS["uploader"]["remoteContentPath"]
        ),
        help="Remote base path for uploaded content files.",
    )
    parser.add_argument(
        "--remote-torrent-path",
        default=uploader_settings.get(
            "remoteTorrentPath", DEFAULT_SETTINGS["uploader"]["remoteTorrentPath"]
        ),
        help="Remote base path for uploaded torrent files.",
    )
    parser.add_argument(
        "--verify-uploads",
        action=argparse.BooleanOptionalAction,
        default=bool(
            uploader_settings.get("verifyUploads", DEFAULT_SETTINGS["uploader"]["verifyUploads"])
        ),
        help="Verify remote file sizes after upload.",
    )
    parser.add_argument(
        "--overwrite-remote-existing",
        action=argparse.BooleanOptionalAction,
        default=bool(
            uploader_settings.get(
                "overwriteRemoteExisting",
                DEFAULT_SETTINGS["uploader"]["overwriteRemoteExisting"],
            )
        ),
        help="Overwrite existing remote files. When false, matching files are skipped and mismatches fail.",
    )
    parser.add_argument(
        "--resume-partial-uploads",
        action=argparse.BooleanOptionalAction,
        default=bool(
            uploader_settings.get(
                "resumePartialUploads",
                DEFAULT_SETTINGS["uploader"]["resumePartialUploads"],
            )
        ),
        help="Resume partial remote files when their size is smaller than the local file.",
    )
    parser.add_argument(
        "--retry-attempts",
        type=int,
        default=int(
            uploader_settings.get(
                "retryAttempts",
                DEFAULT_SETTINGS["uploader"]["retryAttempts"],
            )
        ),
        help="Number of retry attempts after the initial upload failure.",
    )
    parser.add_argument(
        "--retry-delay-sec",
        type=int,
        default=int(
            uploader_settings.get(
                "retryDelaySec",
                DEFAULT_SETTINGS["uploader"]["retryDelaySec"],
            )
        ),
        help="Seconds to wait before retrying a failed upload attempt.",
    )
    parser.add_argument(
        "--ssh-host",
        default=ssh_settings.get("host", DEFAULT_SETTINGS["ssh"]["host"]),
        help="SSH host name.",
    )
    parser.add_argument(
        "--ssh-port",
        type=int,
        default=ssh_settings.get("port", DEFAULT_SETTINGS["ssh"]["port"]),
        help="SSH port number.",
    )
    parser.add_argument(
        "--ssh-username",
        default=ssh_settings.get("username", DEFAULT_SETTINGS["ssh"]["username"]),
        help="SSH username.",
    )
    parser.add_argument(
        "--ssh-password",
        default=os.environ.get("TORRENT_UPLOADER_SSH_PASSWORD")
        or ssh_settings.get("password", DEFAULT_SETTINGS["ssh"]["password"]),
        help="SSH password. CLI overrides environment, environment overrides settings.json.",
    )
    parser.add_argument(
        "--ssh-timeout",
        type=int,
        default=ssh_settings.get(
            "connectTimeoutSec", DEFAULT_SETTINGS["ssh"]["connectTimeoutSec"]
        ),
        help="SSH connect timeout in seconds.",
    )
    parser.add_argument(
        "--qbit-host",
        default=qbit_settings.get("host", DEFAULT_SETTINGS["qbittorrent"]["host"]),
        help="Base URL of the qBittorrent Web UI.",
    )
    parser.add_argument(
        "--qbit-username",
        default=qbit_settings.get("username", DEFAULT_SETTINGS["qbittorrent"]["username"]),
        help="qBittorrent username.",
    )
    parser.add_argument(
        "--qbit-password",
        default=os.environ.get("QBITTORRENT_PASSWORD")
        or qbit_settings.get("password", DEFAULT_SETTINGS["qbittorrent"]["password"]),
        help="qBittorrent password. CLI overrides environment, environment overrides settings.json.",
    )
    return parser.parse_args()


def parse_recipients(raw_value: str) -> tuple[str, ...]:
    return tuple(part.strip() for part in raw_value.split(",") if part.strip())


def build_config(args: argparse.Namespace, settings: dict[str, Any]) -> UploaderConfig:
    if args.max_job_size_gb <= 0:
        raise RuntimeError("max job size must be greater than 0.")
    if args.max_run_hours <= 0:
        raise RuntimeError("max run hours must be greater than 0.")
    if args.continue_past_timeout_job_count < 0:
        raise RuntimeError("continue past timeout job count must be 0 or greater.")
    if args.max_concurrent_jobs <= 0:
        raise RuntimeError("max concurrent jobs must be greater than 0.")
    if args.retry_attempts < 0:
        raise RuntimeError("retry attempts must be 0 or greater.")
    if args.retry_delay_sec < 0:
        raise RuntimeError("retry delay must be 0 or greater.")

    smtp_settings = settings.get("smtp", {})
    ssh_config = SSHConfig(
        host=args.ssh_host,
        port=args.ssh_port,
        username=args.ssh_username,
        password=args.ssh_password,
        connect_timeout_sec=args.ssh_timeout,
    )
    qbit_config = QBittorrentConfig(
        host=args.qbit_host,
        username=args.qbit_username,
        password=args.qbit_password,
    )
    smtp_config = SMTPConfig(
        server=str(smtp_settings.get("server", DEFAULT_SETTINGS["smtp"]["server"])),
        port=int(smtp_settings.get("port", DEFAULT_SETTINGS["smtp"]["port"])),
        encryption=str(
            smtp_settings.get("encryption", DEFAULT_SETTINGS["smtp"]["encryption"])
        ),
        username=str(smtp_settings.get("username", DEFAULT_SETTINGS["smtp"]["username"])),
        password=str(
            os.environ.get("TORRENT_UPLOADER_SMTP_PASSWORD")
            or smtp_settings.get("password", DEFAULT_SETTINGS["smtp"]["password"])
        ),
        from_address=str(
            smtp_settings.get("fromAddress", DEFAULT_SETTINGS["smtp"]["fromAddress"])
        ),
        recipients=parse_recipients(
            str(smtp_settings.get("to", DEFAULT_SETTINGS["smtp"]["to"]))
        ),
    )
    return UploaderConfig(
        mode=args.mode,
        jobs_dir=Path(args.job_dir),
        local_download_root_path=Path(args.local_download_root_path),
        remote_content_path=args.remote_content_path,
        remote_torrent_path=args.remote_torrent_path,
        max_job_size_bytes=int(args.max_job_size_gb * 1024**3),
        max_run_hours=float(args.max_run_hours),
        continue_past_timeout_job_count=int(args.continue_past_timeout_job_count),
        max_concurrent_jobs=args.max_concurrent_jobs,
        overwrite_remote_existing=bool(args.overwrite_remote_existing),
        resume_partial_uploads=bool(args.resume_partial_uploads),
        retry_attempts=int(args.retry_attempts),
        retry_delay_sec=int(args.retry_delay_sec),
        verify_uploads=bool(args.verify_uploads),
        ssh=ssh_config,
        qbittorrent=qbit_config,
        smtp=smtp_config,
    )


def format_bytes(size: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    value = float(size)
    for unit in units:
        if value < 1024 or unit == units[-1]:
            return f"{value:.1f} {unit}"
        value /= 1024
    return f"{size} B"


def resolve_recorded_downloaded_files(payload: dict[str, Any]) -> tuple[Path, ...]:
    raw_paths = payload.get("downloadedFiles")
    if not isinstance(raw_paths, list):
        return ()

    resolved_files: list[Path] = []
    seen_paths: set[str] = set()
    missing_paths: list[str] = []
    for raw_path in raw_paths:
        if not isinstance(raw_path, str) or not raw_path.strip():
            continue

        file_path = Path(raw_path)
        normalized = str(file_path).lower()
        if normalized in seen_paths:
            continue
        seen_paths.add(normalized)

        if file_path.exists() and file_path.is_file():
            resolved_files.append(file_path)
        else:
            missing_paths.append(str(file_path))

    if missing_paths:
        LOGGER.warning(
            "Some recorded downloadedFiles are unavailable and will be skipped: %s",
            "; ".join(missing_paths),
        )

    return tuple(resolved_files)


def has_recorded_downloaded_file_entries(payload: dict[str, Any]) -> bool:
    raw_paths = payload.get("downloadedFiles")
    if not isinstance(raw_paths, list):
        return False

    return any(isinstance(raw_path, str) and raw_path.strip() for raw_path in raw_paths)


def load_job_file(job_file_path: Path) -> UploadJob:
    with job_file_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)

    required_keys = [
        "torrentHash",
        "torrentName",
        "torrentFilePath",
        "parentPath",
        "downloadedFiles",
    ]
    for key in required_keys:
        if key not in payload:
            raise RuntimeError(f"Job file {job_file_path} is missing {key}")

    torrent_file_path = resolve_torrent_file_path(job_file_path, payload)
    if not torrent_file_path.exists():
        raise RuntimeError(f"Torrent file not found: {torrent_file_path}")

    parent_path = Path(payload["parentPath"])
    if payload.get("destinationPath"):
        LOGGER.info(
            "Ignoring destinationPath for %s. Recorded downloadedFiles will be used when available; parentPath %s is the fallback.",
            job_file_path.name,
            parent_path,
        )

    has_recorded_entries = has_recorded_downloaded_file_entries(payload)
    downloaded_files = list(resolve_recorded_downloaded_files(payload))
    if downloaded_files:
        LOGGER.info(
            "Using %s recorded downloaded file(s) for %s",
            len(downloaded_files),
            job_file_path.name,
        )
    elif has_recorded_entries:
        raise RuntimeError(
            "No usable recorded downloadedFiles remain; refusing to fall back to parentPath discovery."
        )
    else:
        LOGGER.warning(
            "No usable recorded downloadedFiles found for %s. Falling back to live discovery under %s",
            job_file_path.name,
            parent_path,
        )
        downloaded_files = list(discover_live_content_files(parent_path))

    total_size_bytes = 0
    for file_path in downloaded_files:
        total_size_bytes += file_path.stat().st_size

    return UploadJob(
        job_file_path=job_file_path,
        torrent_hash=str(payload["torrentHash"]),
        torrent_name=str(payload["torrentName"]),
        torrent_file_path=torrent_file_path,
        parent_path=parent_path,
        downloaded_files=tuple(downloaded_files),
        total_size_bytes=total_size_bytes,
    )


def discover_jobs(
    config: UploaderConfig,
    notifier: Notifier | None = None,
) -> tuple[list[UploadJob], list[str]]:
    if not config.jobs_dir.exists():
        raise RuntimeError(f"Jobs directory does not exist: {config.jobs_dir}")

    jobs: list[UploadJob] = []
    skipped_messages: list[str] = []
    for job_file_path in sorted(config.jobs_dir.glob("*.json")):
        try:
            active_pid = get_active_running_marker_pid(job_file_path)
            if active_pid is not None:
                message = (
                    f"Skipping {job_file_path.name} because it is already running in PID {active_pid}"
                )
                LOGGER.info(message)
                print_status(message)
                skipped_messages.append(message)
                continue

            job = load_job_file(job_file_path)
            if job.total_size_bytes > config.max_job_size_bytes:
                message = (
                    f"Skipping {job.torrent_name} because total size {format_bytes(job.total_size_bytes)} "
                    f"exceeds limit {format_bytes(config.max_job_size_bytes)}"
                )
                LOGGER.warning(message)
                print_status(message)
                if notifier is not None and should_alert_for_oversized_job(job.job_file_path):
                    notifier.send(
                        f"SKIP {job.torrent_name[:60]} {format_bytes(job.total_size_bytes)} > {format_bytes(config.max_job_size_bytes)}"
                    )
                skipped_messages.append(message)
                continue
            jobs.append(job)
        except Exception as exc:
            message = f"Skipping job file {job_file_path.name}: {exc}"
            LOGGER.exception(message)
            print_status(message)
            if notifier is not None:
                notifier.send(f"JOB ERROR {job_file_path.name[:80]}")
            skipped_messages.append(message)
    return jobs, skipped_messages


def ensure_remote_dir(sftp: paramiko.SFTPClient, remote_path: str) -> None:
    parts = [part for part in remote_path.split("/") if part]
    current = "/" if remote_path.startswith("/") else ""
    for part in parts:
        current = posixpath.join(current, part) if current else part
        try:
            sftp.stat(current)
        except IOError:
            sftp.mkdir(current)


def get_remote_free_bytes(session: SSHSession, remote_path: str) -> int | None:
    assert session.sftp is not None
    try:
        stats = session.sftp.statvfs(remote_path)
        return stats.f_frsize * stats.f_bavail
    except Exception:
        stdin, stdout, stderr = session.client.exec_command(f"df -Pk {remote_path}")
        _ = stdin
        error_text = stderr.read().decode("utf-8", errors="replace").strip()
        output = stdout.read().decode("utf-8", errors="replace").strip().splitlines()
        if error_text or len(output) < 2:
            return None
        columns = output[-1].split()
        if len(columns) < 4:
            return None
        return int(columns[3]) * 1024


def check_remote_paths(session: SSHSession, config: UploaderConfig) -> tuple[int | None, int | None]:
    assert session.sftp is not None
    ensure_remote_dir(session.sftp, config.remote_content_path)
    ensure_remote_dir(session.sftp, config.remote_torrent_path)
    content_free = get_remote_free_bytes(session, config.remote_content_path)
    torrent_free = get_remote_free_bytes(session, config.remote_torrent_path)
    return content_free, torrent_free


def run_connection_test(config: UploaderConfig) -> int:
    print_status(
        f"Connecting to {config.ssh.host}:{config.ssh.port} as {config.ssh.username}..."
    )
    LOGGER.info(
        "Running connection test to %s:%s as %s",
        config.ssh.host,
        config.ssh.port,
        config.ssh.username,
    )
    with SSHSession(config.ssh) as session:
        content_free, torrent_free = check_remote_paths(session, config)
        print_status("SSH connection succeeded.")
        print_status(f"Remote content path ready: {config.remote_content_path}")
        print_status(f"Remote torrent path ready: {config.remote_torrent_path}")
        if content_free is not None:
            print_status(f"Remote content free space: {format_bytes(content_free)}")
        else:
            print_status("Remote content free space: unavailable, will rely on upload errors.")
        if torrent_free is not None:
            print_status(f"Remote torrent free space: {format_bytes(torrent_free)}")
        else:
            print_status("Remote torrent free space: unavailable, will rely on upload errors.")
        LOGGER.info(
            "Connection test succeeded. content_free=%s torrent_free=%s",
            content_free,
            torrent_free,
        )
    return 0


def execute_job_with_tracking(
    job: UploadJob,
    config: UploaderConfig,
    job_runner: Any,
    runtime_control: JobRuntimeControl,
) -> None:
    marker_path = acquire_job_running_marker(job.job_file_path)
    status_tracker = JobStatusTracker(marker_path, job, config.mode)
    try:
        job_runner(
            job,
            config,
            JobRuntimeControl(
                cancel_event=runtime_control.cancel_event,
                status_tracker=status_tracker,
            ),
        )
        status_tracker.mark_completed()
    except UploadCancelledError as exc:
        status_tracker.mark_cancelled(str(exc))
        raise
    except Exception as exc:
        status_tracker.mark_failed(str(exc))
        raise
    finally:
        release_job_running_marker(marker_path)


def all_skipped_jobs_are_running(skipped_messages: list[str]) -> bool:
    return bool(skipped_messages) and all(
        "already running in PID" in message for message in skipped_messages
    )


def submit_jobs_until_deadline(
    config: UploaderConfig,
    jobs: list[UploadJob],
    job_runner: Any,
) -> tuple[list[str], list[UploadJob]]:
    deadline = time.monotonic() + (config.max_run_hours * 3600)
    failures: list[str] = []
    deferred_jobs: list[UploadJob] = []
    deferred_job_paths: set[Path] = set()
    pending_jobs = list(jobs)
    deadline_enforced = False

    def defer_job(job: UploadJob) -> None:
        if job.job_file_path not in deferred_job_paths:
            deferred_jobs.append(job)
            deferred_job_paths.add(job.job_file_path)

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=config.max_concurrent_jobs,
        thread_name_prefix="upload",
    ) as executor:
        active_futures: dict[
            concurrent.futures.Future[None], tuple[UploadJob, JobRuntimeControl]
        ] = {}

        while pending_jobs and len(active_futures) < config.max_concurrent_jobs:
            if time.monotonic() >= deadline:
                for job in pending_jobs:
                    defer_job(job)
                pending_jobs.clear()
                break
            job = pending_jobs.pop(0)
            runtime_control = JobRuntimeControl(cancel_event=threading.Event())
            future = executor.submit(
                execute_job_with_tracking,
                job,
                config,
                job_runner,
                runtime_control,
            )
            active_futures[future] = (job, runtime_control)

        while active_futures:
            if not deadline_enforced:
                remaining = deadline - time.monotonic()
                wait_timeout = max(0, remaining)
            else:
                wait_timeout = None

            done, _ = concurrent.futures.wait(
                active_futures,
                timeout=wait_timeout,
                return_when=concurrent.futures.FIRST_COMPLETED,
            )

            if not done and not deadline_enforced and time.monotonic() >= deadline:
                deadline_enforced = True
                allowed_count = min(
                    max(0, config.continue_past_timeout_job_count),
                    len(active_futures),
                )
                active_items = list(active_futures.items())

                for index, (_future, (job, runtime_control)) in enumerate(active_items):
                    if index < allowed_count:
                        LOGGER.info(
                            "Allowing %s to continue past runtime limit until completion",
                            job.torrent_hash,
                        )
                        print_status(
                            f"[{job.torrent_name}] Runtime limit reached. Allowing this job to continue until completion."
                        )
                        continue

                    runtime_control.cancel_event.set()
                    defer_job(job)
                    LOGGER.info(
                        "Cancelling %s because runtime limit reached",
                        job.torrent_hash,
                    )
                    print_status(
                        f"[{job.torrent_name}] Runtime limit reached. Cancelling active upload so it can resume next cycle."
                    )

                for job in pending_jobs:
                    defer_job(job)
                pending_jobs.clear()
                continue

            for future in done:
                job, _runtime_control = active_futures.pop(future)
                try:
                    future.result()
                except UploadCancelledError:
                    defer_job(job)
                    LOGGER.info(
                        "Deferred %s after cooperative cancellation at runtime limit",
                        job.torrent_hash,
                    )
                except JobAlreadyRunningError as exc:
                    LOGGER.info("%s", exc)
                    print_status(f"[{job.torrent_name}] {exc}")
                except Exception as exc:
                    failures.append(f"[{job.torrent_name}] {exc}")

            while pending_jobs and len(active_futures) < config.max_concurrent_jobs:
                if time.monotonic() >= deadline:
                    for job in pending_jobs:
                        defer_job(job)
                    pending_jobs.clear()
                    break
                job = pending_jobs.pop(0)
                runtime_control = JobRuntimeControl(cancel_event=threading.Event())
                future = executor.submit(
                    execute_job_with_tracking,
                    job,
                    config,
                    job_runner,
                    runtime_control,
                )
                active_futures[future] = (job, runtime_control)

    return failures, deferred_jobs


def job_remote_root(job: UploadJob, config: UploaderConfig) -> str:
    local_download_root = PureWindowsPath(str(config.local_download_root_path))
    parent_path = PureWindowsPath(str(job.parent_path))
    try:
        relative_parent = parent_path.relative_to(local_download_root)
    except Exception:
        relative_parent = None

    if relative_parent is None:
        return posixpath.join(config.remote_content_path, job.remote_content_root_name)

    relative_parent_posix = relative_parent.as_posix().strip("/")
    if not relative_parent_posix or relative_parent_posix == ".":
        return config.remote_content_path

    return posixpath.join(config.remote_content_path, relative_parent_posix)


def local_to_remote_relative_path(config: UploaderConfig, local_path: Path) -> str | None:
    local_download_root = PureWindowsPath(str(config.local_download_root_path))
    try:
        relative = PureWindowsPath(str(local_path)).relative_to(local_download_root)
    except Exception:
        return None
    return relative.as_posix()


def fallback_relative_path(job: UploadJob, local_path: Path) -> str:
    try:
        relative = PureWindowsPath(str(local_path)).relative_to(PureWindowsPath(str(job.parent_path)))
        return relative.as_posix()
    except Exception:
        return local_path.name


def local_to_remote_file(job: UploadJob, config: UploaderConfig, local_path: Path) -> str:
    relative_to_download_root = local_to_remote_relative_path(config, local_path)
    if relative_to_download_root is not None:
        return posixpath.join(config.remote_content_path, relative_to_download_root)

    relative_path = fallback_relative_path(job, local_path)
    return posixpath.join(job_remote_root(job, config), relative_path)


def get_remote_file_size(sftp: paramiko.SFTPClient, remote_path: str) -> int | None:
    try:
        return sftp.stat(remote_path).st_size
    except IOError:
        return None


def remove_remote_file_if_exists(sftp: paramiko.SFTPClient, remote_path: str) -> None:
    try:
        sftp.remove(remote_path)
    except IOError:
        return


def write_remote_file(
    session: SSHSession,
    local_path: Path,
    remote_path: str,
    file_size: int,
    start_offset: int,
    progress: UploadProgress,
    cancel_event: threading.Event,
) -> None:
    assert session.sftp is not None
    mode = "ab" if start_offset > 0 else "wb"
    with local_path.open("rb") as local_handle:
        if start_offset > 0:
            local_handle.seek(start_offset)
            progress.callback(start_offset, file_size)
        with session.sftp.open(remote_path, mode) as remote_handle:
            remote_handle.set_pipelined(True)
            transferred = start_offset
            while True:
                if cancel_event.is_set():
                    remote_handle.flush()
                    raise UploadCancelledError(
                        f"Runtime limit reached while uploading {local_path.name}. Partial upload will resume next cycle."
                    )
                chunk = local_handle.read(UPLOAD_CHUNK_SIZE_BYTES)
                if not chunk:
                    break
                remote_handle.write(chunk)
                transferred += len(chunk)
                progress.callback(transferred, file_size)
            remote_handle.flush()


def upload_file(
    session: SSHSession,
    job: UploadJob,
    local_path: Path,
    remote_path: str,
    overwrite_remote_existing: bool,
    resume_partial_uploads: bool,
    retry_attempts: int,
    retry_delay_sec: int,
    verify_uploads: bool,
    cancel_event: threading.Event,
    status_tracker: JobStatusTracker | None = None,
    track_in_job_progress: bool = True,
) -> None:
    if not local_path.exists() or not local_path.is_file():
        raise MissingLocalFileError(f"Local file is unavailable: {local_path}")

    file_size = local_path.stat().st_size
    total_attempts = retry_attempts + 1

    for attempt_index in range(total_attempts):
        attempt_number = attempt_index + 1
        try:
            if cancel_event.is_set():
                raise UploadCancelledError(
                    f"Runtime limit reached before uploading {local_path.name}."
                )
            session.ensure_connected()
            assert session.sftp is not None
            ensure_remote_dir(session.sftp, posixpath.dirname(remote_path))
            remote_size = get_remote_file_size(session.sftp, remote_path)
            start_offset = 0

            if remote_size is not None:
                if remote_size == file_size:
                    if not overwrite_remote_existing:
                        print_status(
                            f"[{job.torrent_name}] Skipping existing remote file {remote_path} ({format_bytes(file_size)})"
                        )
                        LOGGER.info(
                            "Skipping existing remote file %s for %s because overwrite is disabled",
                            remote_path,
                            job.torrent_hash,
                        )
                        if status_tracker is not None and track_in_job_progress:
                            status_tracker.complete_content_file(local_path, remote_path, file_size)
                        return
                    print_status(
                        f"[{job.torrent_name}] Replacing existing remote file {remote_path}"
                    )
                    LOGGER.info(
                        "Replacing existing remote file %s for %s because overwrite is enabled",
                        remote_path,
                        job.torrent_hash,
                    )
                    remove_remote_file_if_exists(session.sftp, remote_path)
                elif remote_size < file_size and resume_partial_uploads:
                    start_offset = remote_size
                    print_status(
                        f"[{job.torrent_name}] Resuming {local_path.name} at {format_bytes(start_offset)} of {format_bytes(file_size)}"
                    )
                    LOGGER.info(
                        "Resuming upload of %s to %s at offset %s for %s",
                        local_path,
                        remote_path,
                        start_offset,
                        job.torrent_hash,
                    )
                elif overwrite_remote_existing:
                    print_status(
                        f"[{job.torrent_name}] Replacing remote file with mismatched size {remote_path}"
                    )
                    LOGGER.info(
                        "Replacing mismatched remote file %s for %s because overwrite is enabled",
                        remote_path,
                        job.torrent_hash,
                    )
                    remove_remote_file_if_exists(session.sftp, remote_path)
                else:
                    raise RuntimeError(
                        f"Remote file already exists with different size and overwrite is disabled: {remote_path}"
                    )

            if status_tracker is not None:
                if track_in_job_progress:
                    status_tracker.start_content_file(local_path, remote_path, file_size, start_offset)
                else:
                    status_tracker.start_torrent_upload(local_path, remote_path)
            progress = UploadProgress(
                job.torrent_name,
                local_path.name,
                file_size,
                progress_callback=(
                    (lambda transferred, total: status_tracker.update_content_progress(transferred, total))
                    if status_tracker is not None and track_in_job_progress
                    else (
                        (lambda transferred, total: status_tracker.update_torrent_progress(transferred, total))
                        if status_tracker is not None
                        else None
                    )
                ),
            )
            if start_offset > 0:
                print_status(
                    f"[{job.torrent_name}] Continuing {local_path.name} to {remote_path} ({format_bytes(file_size)})"
                )
            else:
                print_status(
                    f"[{job.torrent_name}] Uploading {local_path.name} to {remote_path} ({format_bytes(file_size)})"
                )
            LOGGER.info(
                "Uploading %s to %s (attempt %s/%s, resume_offset=%s)",
                local_path,
                remote_path,
                attempt_number,
                total_attempts,
                start_offset,
            )
            write_remote_file(
                session,
                local_path,
                remote_path,
                file_size,
                start_offset,
                progress,
                cancel_event,
            )
            if verify_uploads:
                remote_size = session.sftp.stat(remote_path).st_size
                if remote_size != file_size:
                    raise RuntimeError(
                        f"Remote size mismatch for {remote_path}: expected {file_size}, got {remote_size}"
                    )
            if status_tracker is not None:
                if track_in_job_progress:
                    status_tracker.complete_content_file(local_path, remote_path, file_size)
                else:
                    status_tracker.complete_torrent_upload()
            print_status(f"[{job.torrent_name}] Uploaded {local_path.name}")
            return
        except UploadCancelledError:
            raise
        except Exception as exc:
            if attempt_number >= total_attempts:
                raise
            LOGGER.warning(
                "Upload attempt %s/%s failed for %s to %s: %s",
                attempt_number,
                total_attempts,
                local_path,
                remote_path,
                exc,
            )
            print_status(
                f"[{job.torrent_name}] Upload attempt {attempt_number}/{total_attempts} failed for {local_path.name}: {exc}. Retrying in {retry_delay_sec}s."
            )
            try:
                session.reconnect()
                LOGGER.info(
                    "Reconnected SSH session after failed upload attempt %s/%s for %s",
                    attempt_number,
                    total_attempts,
                    job.torrent_hash,
                )
            except Exception as reconnect_exc:
                LOGGER.warning(
                    "Failed to reconnect SSH session after upload attempt %s/%s for %s: %s",
                    attempt_number,
                    total_attempts,
                    job.torrent_hash,
                    reconnect_exc,
                )
            if retry_delay_sec > 0:
                time.sleep(retry_delay_sec)


def transfer_job(
    job: UploadJob,
    config: UploaderConfig,
    continue_torrent_on_content_errors: bool,
    runtime_control: JobRuntimeControl,
) -> list[str]:
    LOGGER.info("Starting %s upload for %s", config.mode, job.torrent_hash)
    if runtime_control.status_tracker is not None:
        runtime_control.status_tracker.update_phase(
            "connecting",
            f"Connecting for {config.mode} upload",
        )
    print_status(
        f"[{job.torrent_name}] Starting {config.mode} upload. Total content size: {format_bytes(job.total_size_bytes)}"
    )
    content_errors: list[str] = []
    with SSHSession(config.ssh) as session:
        content_free, torrent_free = check_remote_paths(session, config)
        if content_free is not None:
            print_status(
                f"[{job.torrent_name}] Remote content free space: {format_bytes(content_free)}"
            )
        else:
            print_status(
                f"[{job.torrent_name}] Remote content free space unavailable. Upload errors will be used instead."
            )
        if torrent_free is not None:
            print_status(
                f"[{job.torrent_name}] Remote torrent free space: {format_bytes(torrent_free)}"
            )
        else:
            print_status(
                f"[{job.torrent_name}] Remote torrent free space unavailable. Upload errors will be used instead."
            )

        remote_root = job_remote_root(job, config)
        print_status(f"[{job.torrent_name}] Remote content folder: {remote_root}")
        for local_path in job.downloaded_files:
            if runtime_control.cancel_event.is_set():
                raise UploadCancelledError(
                    f"Runtime limit reached during {job.torrent_name}. Partial upload will resume next cycle."
                )
            if not local_path.exists() or not local_path.is_file():
                message = f"Skipping missing local file {local_path}"
                LOGGER.warning("%s for %s", message, job.torrent_hash)
                print_status(f"[{job.torrent_name}] {message}")
                continue

            remote_path = local_to_remote_file(job, config, local_path)
            try:
                upload_file(
                    session,
                    job,
                    local_path,
                    remote_path,
                    config.overwrite_remote_existing,
                    config.resume_partial_uploads,
                    config.retry_attempts,
                    config.retry_delay_sec,
                    config.verify_uploads,
                    runtime_control.cancel_event,
                    runtime_control.status_tracker,
                    True,
                )
            except UploadCancelledError:
                raise
            except MissingLocalFileError as exc:
                LOGGER.warning("Skipping missing local file for %s: %s", job.torrent_hash, exc)
                print_status(f"[{job.torrent_name}] Skipping missing local file {local_path}")
            except Exception as exc:
                message = f"Content upload issue for {local_path.name}: {exc}"
                LOGGER.warning("%s for %s", message, job.torrent_hash)
                print_status(f"[{job.torrent_name}] {message}")
                content_errors.append(message)

        remote_torrent_path = posixpath.join(
            config.remote_torrent_path, job.torrent_file_path.name
        )
        if content_errors and not continue_torrent_on_content_errors:
            raise RuntimeError("; ".join(content_errors))
        if runtime_control.cancel_event.is_set():
            raise UploadCancelledError(
                f"Runtime limit reached before uploading torrent metadata for {job.torrent_name}."
            )

        print_status(
            f"[{job.torrent_name}] Content upload complete. Uploading torrent file to {remote_torrent_path}"
        )
        LOGGER.info(
            "Content upload complete for %s. Uploading torrent file to %s",
            job.torrent_hash,
            remote_torrent_path,
        )
        upload_file(
            session,
            job,
            job.torrent_file_path,
            remote_torrent_path,
            config.overwrite_remote_existing,
            config.resume_partial_uploads,
            config.retry_attempts,
            config.retry_delay_sec,
            config.verify_uploads,
            runtime_control.cancel_event,
            runtime_control.status_tracker,
            False,
        )

    return content_errors


def remove_empty_parent_dirs(parent_path: Path) -> None:
    if not parent_path.exists() or parent_path.is_file():
        return

    directories = sorted(
        (path for path in parent_path.rglob("*") if path.is_dir()),
        key=lambda path: len(path.parts),
        reverse=True,
    )
    for directory in directories:
        try:
            if not any(directory.iterdir()):
                directory.rmdir()
        except OSError:
            continue

    try:
        if parent_path.exists() and not any(parent_path.iterdir()):
            parent_path.rmdir()
    except OSError:
        pass


def cleanup_local_files(job: UploadJob) -> None:
    for file_path in job.downloaded_files:
        if file_path.exists():
            file_path.unlink()
    remove_empty_parent_dirs(job.parent_path)
    if job.torrent_file_path.exists():
        job.torrent_file_path.unlink()
    if job.job_file_path.exists():
        job.job_file_path.unlink()
    release_job_running_marker(oversized_alert_marker_path(job.job_file_path))


def cleanup_qbittorrent(job: UploadJob, config: UploaderConfig) -> None:
    client = QBittorrentClient(
        config.qbittorrent.host,
        config.qbittorrent.username,
        config.qbittorrent.password,
    )
    client.login()
    print_status(f"[{job.torrent_name}] Stopping torrent in qBittorrent")
    client.stop_torrent(job.torrent_hash)
    print_status(f"[{job.torrent_name}] Removing torrent from qBittorrent")
    client.delete_torrent(job.torrent_hash, delete_files=False)


def stage_job(job: UploadJob, config: UploaderConfig, runtime_control: JobRuntimeControl) -> None:
    content_errors = transfer_job(
        job,
        config,
        continue_torrent_on_content_errors=True,
        runtime_control=runtime_control,
    )
    print_status(f"[{job.torrent_name}] Staged upload complete. No local cleanup performed.")
    LOGGER.info("Completed staged upload for %s", job.torrent_hash)
    if content_errors:
        raise RuntimeError("; ".join(content_errors))


def full_job(job: UploadJob, config: UploaderConfig, runtime_control: JobRuntimeControl) -> None:
    transfer_job(
        job,
        config,
        continue_torrent_on_content_errors=False,
        runtime_control=runtime_control,
    )
    if runtime_control.status_tracker is not None:
        runtime_control.status_tracker.mark_local_cleanup()
    print_status(
        f"[{job.torrent_name}] Upload complete. Removing local files and job artifacts."
    )
    cleanup_local_files(job)
    if runtime_control.status_tracker is not None:
        runtime_control.status_tracker.mark_qb_cleanup()
    print_status(f"[{job.torrent_name}] Local cleanup complete. Starting qBittorrent cleanup.")
    cleanup_qbittorrent(job, config)
    print_status(f"[{job.torrent_name}] Full upload and cleanup complete.")
    LOGGER.info("Completed full upload and cleanup for %s", job.torrent_hash)


def run_stage_mode(config: UploaderConfig, notifier: Notifier) -> int:
    jobs, skipped_messages = discover_jobs(config, notifier)
    print_status(
        f"Discovered {len(jobs)} upload job(s) in {config.jobs_dir}. Skipped {len(skipped_messages)}. Max runtime: {config.max_run_hours:g} hour(s)."
    )
    LOGGER.info(
        "Discovered %s jobs in %s and skipped %s. max_run_hours=%s",
        len(jobs),
        config.jobs_dir,
        len(skipped_messages),
        config.max_run_hours,
    )
    if not jobs:
        print_status("No eligible jobs found. No uploads were attempted.")
        if skipped_messages:
            if all_skipped_jobs_are_running(skipped_messages):
                LOGGER.info("Stage mode found only jobs already running in another uploader process.")
                return 0
            LOGGER.warning("Stage mode found no eligible jobs and skipped %s job(s)", len(skipped_messages))
            return 1
        return 0

    failures, deferred_jobs = submit_jobs_until_deadline(config, jobs, stage_job)
    for failure in failures:
        message = failure.replace("] ", "] Staged upload failed: ", 1)
        LOGGER.error(message)
        print_status(message)

    if deferred_jobs:
        deferred_names = ", ".join(job.job_file_path.name for job in deferred_jobs)
        message = (
            f"Runtime limit reached after {config.max_run_hours:g} hour(s). "
            f"Deferred {len(deferred_jobs)} job(s) for the next cycle: {deferred_names}"
        )
        LOGGER.info(message)
        print_status(message)

    if failures:
        print_status(f"Staged upload completed with {len(failures)} failure(s).")
        return 1

    print_status("Staged upload cycle completed.")
    return 0


def run_full_mode(config: UploaderConfig, notifier: Notifier) -> int:
    jobs, skipped_messages = discover_jobs(config, notifier)
    print_status(
        f"Discovered {len(jobs)} upload job(s) in {config.jobs_dir}. Skipped {len(skipped_messages)}. Max runtime: {config.max_run_hours:g} hour(s)."
    )
    LOGGER.info(
        "Discovered %s jobs in %s and skipped %s for full mode. max_run_hours=%s",
        len(jobs),
        config.jobs_dir,
        len(skipped_messages),
        config.max_run_hours,
    )
    if not jobs:
        print_status("No eligible jobs found. No uploads were attempted.")
        if skipped_messages:
            if all_skipped_jobs_are_running(skipped_messages):
                LOGGER.info("Full mode found only jobs already running in another uploader process.")
                return 0
            LOGGER.warning("Full mode found no eligible jobs and skipped %s job(s)", len(skipped_messages))
            return 1
        return 0

    failures, deferred_jobs = submit_jobs_until_deadline(config, jobs, full_job)
    for failure in failures:
        message = failure.replace("] ", "] Full upload failed: ", 1)
        LOGGER.error(message)
        print_status(message)
        job_name = message.split("]", 1)[0].lstrip("[")
        notifier.send(f"FAIL {job_name[:80]} upload/cleanup")

    if deferred_jobs:
        deferred_names = ", ".join(job.job_file_path.name for job in deferred_jobs)
        message = (
            f"Runtime limit reached after {config.max_run_hours:g} hour(s). "
            f"Deferred {len(deferred_jobs)} job(s) for the next cycle: {deferred_names}"
        )
        LOGGER.info(message)
        print_status(message)

    if failures:
        print_status(f"Full upload completed with {len(failures)} failure(s).")
        return 1

    print_status("Full upload cycle completed.")
    return 0


def main() -> int:
    try:
        settings = load_settings(SETTINGS_FILE)
        configure_logging(settings)
        args = parse_args(settings)
        config = build_config(args, settings)
    except Exception as exc:
        print(str(exc), flush=True)
        return 1

    notifier = Notifier(config.smtp)
    LOGGER.info("Uploader starting in %s mode", config.mode)
    try:
        if config.mode == "connect-test":
            return run_connection_test(config)

        update_monitor_pause_flag(SETTINGS_FILE, True)
        if config.mode == "stage":
            return run_stage_mode(config, notifier)
        if config.mode == "full":
            return run_full_mode(config, notifier)

        print_status(f"Unsupported uploader mode: {config.mode}")
        return 1
    finally:
        if config.mode in {"stage", "full"}:
            try:
                update_monitor_pause_flag(SETTINGS_FILE, False)
            except Exception:
                LOGGER.exception("Failed to clear monitor pause flag.")
                print_status("Failed to clear monitor pause flag.")
                raise


if __name__ == "__main__":
    raise SystemExit(main())