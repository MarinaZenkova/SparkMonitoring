from __future__ import annotations

import base64
import json
import os
import re
import shutil
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_SHS_URL = os.environ.get("SPARK_HISTORY_URL", "http://127.0.0.1:18080")
DEFAULT_RUN_GAP_MS = 30 * 60 * 1000


@dataclass(frozen=True, slots=True)
class HistoryApplication:
    application_id: str
    application_name: str
    attempt_id: str | None
    start_time: str | None
    end_time: str | None
    start_time_ms: int | None
    end_time_ms: int | None
    duration_ms: int | None
    completed: bool
    spark_user: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def validate_dag_name(value: str) -> str:
    dag_name = value.strip()
    if not dag_name or len(dag_name) > 200:
        raise ValueError("Введите имя DAG длиной до 200 символов")
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", dag_name):
        raise ValueError("Имя DAG может содержать буквы, цифры, точку, дефис и подчёркивание")
    return dag_name


def _timestamp_ms(value: Any) -> int | None:
    if value in (None, ""):
        return None
    text = str(value).strip()
    if text.endswith("GMT"):
        text = text[:-3] + "+00:00"
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp() * 1000)


class SparkHistoryClient:
    def __init__(self, base_url: str = DEFAULT_SHS_URL, timeout_seconds: int = 120):
        parsed = urllib.parse.urlparse(base_url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("Некорректный URL Spark History Server")
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.username = os.environ.get("SPARK_HISTORY_USER")
        self.password = os.environ.get("SPARK_HISTORY_PASSWORD")

    def _request(self, path: str, accept: str = "application/json") -> urllib.request.Request:
        request = urllib.request.Request(f"{self.base_url}{path}")
        request.add_header("Accept", accept)
        if self.username and self.password:
            token = base64.b64encode(f"{self.username}:{self.password}".encode()).decode()
            request.add_header("Authorization", f"Basic {token}")
        return request

    def applications(self, limit: int | None = None) -> list[dict[str, Any]]:
        query = f"?{urllib.parse.urlencode({'limit': limit})}" if limit is not None else ""
        try:
            with urllib.request.urlopen(self._request(f"/api/v1/applications{query}"), timeout=self.timeout_seconds) as response:
                payload = json.load(response)
        except urllib.error.HTTPError as exc:
            raise RuntimeError(f"SHS вернул HTTP {exc.code} при чтении списка приложений") from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"Не удалось подключиться к Spark History Server: {exc.reason}") from exc
        if not isinstance(payload, list):
            raise RuntimeError("Spark History Server вернул неожиданный формат списка приложений")
        return payload

    def latest_dag_run(self, dag_name: str, run_gap_ms: int = DEFAULT_RUN_GAP_MS) -> list[HistoryApplication]:
        dag_name = validate_dag_name(dag_name)
        prefix = f"{dag_name}__"
        matches: list[HistoryApplication] = []
        for raw in self.applications():
            name = str(raw.get("name") or "")
            if not name.startswith(prefix):
                continue
            attempts = raw.get("attempts") or []
            if not attempts:
                continue
            attempt = max(attempts, key=lambda item: _timestamp_ms(item.get("startTime")) or -1)
            start_ms = _timestamp_ms(attempt.get("startTime"))
            end_ms = _timestamp_ms(attempt.get("endTime"))
            duration = attempt.get("duration")
            matches.append(
                HistoryApplication(
                    application_id=str(raw.get("id")),
                    application_name=name,
                    attempt_id=str(attempt.get("attemptId")) if attempt.get("attemptId") is not None else None,
                    start_time=attempt.get("startTime"),
                    end_time=attempt.get("endTime"),
                    start_time_ms=start_ms,
                    end_time_ms=end_ms,
                    duration_ms=int(duration) if duration is not None else None,
                    completed=bool(attempt.get("completed")),
                    spark_user=attempt.get("sparkUser"),
                )
            )
        matches.sort(key=lambda item: (item.start_time_ms or -1, item.application_id))
        if not matches:
            return []

        groups: list[list[HistoryApplication]] = []
        current: list[HistoryApplication] = []
        current_end = -1
        for application in matches:
            start = application.start_time_ms or -1
            end = application.end_time_ms or start
            if current and start > current_end + run_gap_ms:
                groups.append(current)
                current = []
                current_end = -1
            current.append(application)
            current_end = max(current_end, end)
        if current:
            groups.append(current)
        return groups[-1]

    def download_event_log(self, application_id: str, destination: str | Path) -> Path:
        safe_id = urllib.parse.quote(application_id, safe="")
        target = Path(destination)
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_suffix(target.suffix + ".part")
        try:
            with urllib.request.urlopen(
                self._request(f"/api/v1/applications/{safe_id}/logs", "application/octet-stream"),
                timeout=self.timeout_seconds,
            ) as response, temporary.open("wb") as output:
                shutil.copyfileobj(response, output, length=1024 * 1024)
            temporary.replace(target)
        except urllib.error.HTTPError as exc:
            temporary.unlink(missing_ok=True)
            raise RuntimeError(f"SHS вернул HTTP {exc.code} при скачивании {application_id}") from exc
        except urllib.error.URLError as exc:
            temporary.unlink(missing_ok=True)
            raise RuntimeError(f"Не удалось скачать {application_id}: {exc.reason}") from exc
        return target
