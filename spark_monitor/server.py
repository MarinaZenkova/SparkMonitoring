from __future__ import annotations

import json
import mimetypes
import os
import shutil
import stat
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

from .config import load_rules
from .report import build_report, write_report
from .shs import DEFAULT_SHS_URL, SparkHistoryClient, validate_dag_name
from .storage import persist_report


def _summary(report: dict[str, Any]) -> dict[str, Any]:
    return {
        key: report.get(key)
        for key in (
            "schema_version",
            "parser_version",
            "rules_version",
            "application",
            "coverage",
            "summary_metrics",
            "top_stages",
            "diagnostics",
            "primary_evidence",
            "problem_count",
        )
    }


class AnalysisJobs:
    def __init__(self, report_root: Path, database_path: Path):
        self.report_root = report_root
        self.data_root = report_root.parent
        self.raw_root = self.data_root / "raw"
        self.manifest_path = self.data_root / "latest_run_manifest.json"
        self.work_root = self.data_root / ".analysis-work"
        self.database_path = database_path
        self._jobs: dict[str, dict[str, Any]] = {}
        self._lock = threading.Lock()

    def start(self, dag_name: str, shs_url: str) -> str:
        dag_name = validate_dag_name(dag_name)
        with self._lock:
            if any(job["status"] in {"QUEUED", "RUNNING"} for job in self._jobs.values()):
                raise RuntimeError("Расчёт уже выполняется")
            job_id = uuid.uuid4().hex
            self._jobs[job_id] = {
                "job_id": job_id,
                "dag_name": dag_name,
                "status": "QUEUED",
                "phase": "DISCOVERY",
                "message": "Ищу приложения последнего запуска",
                "total": 0,
                "completed": 0,
                "failed": 0,
                "applications": [],
                "report_ids": [],
                "errors": [],
            }
        threading.Thread(target=self._run, args=(job_id, dag_name, shs_url), daemon=True).start()
        return job_id

    def get(self, job_id: str) -> dict[str, Any] | None:
        with self._lock:
            job = self._jobs.get(job_id)
            return json.loads(json.dumps(job, ensure_ascii=False)) if job else None

    def active(self) -> dict[str, Any] | None:
        with self._lock:
            for job in reversed(self._jobs.values()):
                if job["status"] in {"QUEUED", "RUNNING"}:
                    return json.loads(json.dumps(job, ensure_ascii=False))
        return None

    def _update(self, job_id: str, **values: Any) -> None:
        with self._lock:
            self._jobs[job_id].update(values)

    def _increment(self, job_id: str, *, failed: bool, report_id: str | None = None, error: str | None = None) -> None:
        with self._lock:
            job = self._jobs[job_id]
            job["completed"] += 1
            if failed:
                job["failed"] += 1
            if report_id:
                job["report_ids"].append(report_id)
            if error:
                job["errors"].append(error)
            job["message"] = f"Обработано {job['completed']} из {job['total']} приложений"

    def _process_application(
        self,
        client: SparkHistoryClient,
        dag_name: str,
        application: dict[str, Any],
        raw_root: Path,
        report_root: Path,
    ) -> tuple[str, dict[str, Any]]:
        application_id = application["application_id"]
        raw_path = raw_root / dag_name / f"{application_id}.zip"
        client.download_event_log(application_id, raw_path)
        report = build_report(raw_path)
        write_report(report, report_root / f"{application_id}.json")
        return application_id, report

    @staticmethod
    def _remove_path(path: Path) -> None:
        if path.is_dir():
            def force_writable(function: Any, target: str, _: object) -> None:
                os.chmod(target, stat.S_IWRITE)
                function(target)

            shutil.rmtree(path, onerror=force_writable)
        else:
            if path.exists():
                path.chmod(stat.S_IWRITE)
            path.unlink(missing_ok=True)

    def _replace_current_dataset(
        self,
        job_id: str,
        staged_raw: Path,
        staged_reports: Path,
        staged_database: Path,
        staged_manifest: Path,
    ) -> None:
        managed_root = self.data_root.parent.resolve()
        targets = (
            (self.raw_root, staged_raw, "raw"),
            (self.report_root, staged_reports, "reports"),
            (self.database_path, staged_database, "spark_monitor.db"),
            (self.manifest_path, staged_manifest, "latest_run_manifest.json"),
        )
        for final, staged, _ in targets:
            resolved = final.resolve()
            if resolved == managed_root or managed_root not in resolved.parents:
                raise RuntimeError(f"Unsafe dataset target: {resolved}")
            if not staged.exists():
                raise RuntimeError(f"Staged dataset component is missing: {staged.name}")

        # A previous process may have stopped after promotion but before deleting
        # its backup. When every live target exists, those backups are obsolete.
        if all(final.exists() for final, _, _ in targets):
            for obsolete in self.data_root.glob(".analysis-backup-*"):
                self._remove_path(obsolete)

        backup_root = self.data_root / f".analysis-backup-{job_id}"
        if backup_root.exists():
            self._remove_path(backup_root)
        backup_root.mkdir(parents=True)
        moved_old: list[tuple[Path, Path]] = []
        promoted: list[Path] = []
        try:
            for final, _, backup_name in targets:
                if final.exists():
                    backup = backup_root / backup_name
                    final.rename(backup)
                    moved_old.append((backup, final))
            for final, staged, _ in targets:
                final.parent.mkdir(parents=True, exist_ok=True)
                staged.rename(final)
                promoted.append(final)
        except Exception:
            for final in reversed(promoted):
                self._remove_path(final)
            for backup, final in reversed(moved_old):
                if backup.exists():
                    backup.rename(final)
            raise
        else:
            self._remove_path(backup_root)

    def _run(self, job_id: str, dag_name: str, shs_url: str) -> None:
        job_root = self.work_root / job_id
        try:
            self._update(job_id, status="RUNNING", phase="DISCOVERY")
            client = SparkHistoryClient(shs_url)
            discovered = client.latest_dag_run(dag_name)
            if not discovered:
                raise RuntimeError(f"Для DAG {dag_name} приложения не найдены")
            applications = [application.to_dict() for application in discovered]
            staged_raw = job_root / "raw"
            staged_reports = job_root / "reports"
            staged_database = job_root / "spark_monitor.db"
            staged_manifest = job_root / "latest_run_manifest.json"
            staged_raw.mkdir(parents=True, exist_ok=True)
            staged_reports.mkdir(parents=True, exist_ok=True)
            self._update(
                job_id,
                phase="ANALYSIS",
                message=f"Найдено приложений: {len(applications)}",
                total=len(applications),
                applications=applications,
            )
            reports: dict[str, dict[str, Any]] = {}
            worker_count = min(4, len(applications))
            with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="spark-analysis") as pool:
                future_map = {
                    pool.submit(
                        self._process_application,
                        client,
                        dag_name,
                        application,
                        staged_raw,
                        staged_reports,
                    ): application
                    for application in applications
                }
                for future in as_completed(future_map):
                    application = future_map[future]
                    try:
                        report_id, report = future.result()
                        reports[report_id] = report
                        self._increment(job_id, failed=False, report_id=report_id)
                    except Exception as exc:  # keep other applications analyzable
                        self._increment(
                            job_id,
                            failed=True,
                            error=f"{application['application_name']}: {exc}",
                        )

            ordered_ids = [application["application_id"] for application in applications if application["application_id"] in reports]
            failed = len(applications) - len(ordered_ids)
            if failed:
                raise RuntimeError(
                    f"Новый набор не применён: не удалось обработать {failed} из {len(applications)} приложений"
                )
            for report_id in ordered_ids:
                persist_report(reports[report_id], staged_database)
            run_start = min((item.get("start_time_ms") for item in applications if item.get("start_time_ms") is not None), default=None)
            run_end = max((item.get("end_time_ms") for item in applications if item.get("end_time_ms") is not None), default=None)
            index = {
                "dag_name": dag_name,
                "shs_url": shs_url,
                "run_start": run_start,
                "run_end": run_end,
                "application_count": len(applications),
                "successful_report_count": len(ordered_ids),
                "report_ids": ordered_ids,
            }
            (staged_reports / "latest_dashboard.json").write_text(
                json.dumps(index, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            manifest = {
                "dag_name": dag_name,
                "selection": "latest deterministic run",
                "run_start_ms": run_start,
                "run_end_ms": run_end,
                "application_count": len(applications),
                "applications": applications,
            }
            staged_manifest.write_text(
                json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            self._update(job_id, phase="COMMIT", message="Заменяю предыдущий DAG новым набором")
            self._replace_current_dataset(
                job_id,
                staged_raw,
                staged_reports,
                staged_database,
                staged_manifest,
            )
            self._update(
                job_id,
                status="COMPLETED",
                phase="DONE",
                message=f"Готово: {len(ordered_ids)} приложений",
                report_ids=ordered_ids,
            )
        except Exception as exc:
            current = self.get(job_id) or {}
            errors = list(current.get("errors") or [])
            if str(exc) not in errors:
                errors.append(str(exc))
            self._update(job_id, status="FAILED", phase="DONE", message=str(exc), errors=errors)
        finally:
            if job_root.exists():
                try:
                    self._remove_path(job_root)
                except OSError:
                    # Cleanup must not overwrite the actual analysis result.
                    pass


def serve(reports_dir: str | Path, host: str = "127.0.0.1", port: int = 8765) -> None:
    project_root = Path(__file__).resolve().parent.parent
    requested_root = Path(reports_dir)
    report_root = requested_root.resolve()
    demo_root = project_root / "demo" / "reports"
    if (
        requested_root.as_posix().rstrip("/") == "data/reports"
        and not any(report_root.glob("*.json"))
        and demo_root.is_dir()
    ):
        report_root = demo_root.resolve()
    static_root = Path(__file__).resolve().parent / "static"
    report_root.mkdir(parents=True, exist_ok=True)
    database_path = report_root.parent.parent / "spark_monitor.db"
    jobs = AnalysisJobs(report_root, database_path)

    class Handler(BaseHTTPRequestHandler):
        def _send_json(self, payload: object, status: HTTPStatus = HTTPStatus.OK) -> None:
            data = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(data)

        def _send_file(self, path: Path) -> None:
            if not path.is_file():
                self.send_error(HTTPStatus.NOT_FOUND)
                return
            data = path.read_bytes()
            content_type = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", f"{content_type}; charset=utf-8" if content_type.startswith("text/") else content_type)
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(data)

        def _dashboard_index(self) -> dict[str, Any]:
            path = report_root / "latest_dashboard.json"
            if path.is_file():
                try:
                    return json.loads(path.read_text(encoding="utf-8"))
                except (OSError, json.JSONDecodeError):
                    pass
            return {"report_ids": []}

        def do_POST(self) -> None:  # noqa: N802
            request_path = unquote(urlparse(self.path).path)
            if request_path != "/api/analyze":
                self._send_json({"error": "Not found"}, HTTPStatus.NOT_FOUND)
                return
            try:
                content_length = int(self.headers.get("Content-Length", "0"))
                if content_length <= 0 or content_length > 16_384:
                    raise ValueError("Некорректный размер запроса")
                payload = json.loads(self.rfile.read(content_length))
                dag_name = validate_dag_name(str(payload.get("dag_name") or ""))
                shs_url = str(payload.get("shs_url") or DEFAULT_SHS_URL)
                job_id = jobs.start(dag_name, shs_url)
            except RuntimeError as exc:
                self._send_json({"error": str(exc)}, HTTPStatus.CONFLICT)
                return
            except (ValueError, json.JSONDecodeError) as exc:
                self._send_json({"error": str(exc)}, HTTPStatus.BAD_REQUEST)
                return
            self._send_json({"job_id": job_id}, HTTPStatus.ACCEPTED)

        def do_GET(self) -> None:  # noqa: N802
            request_path = unquote(urlparse(self.path).path)
            if request_path == "/api/rules":
                self._send_json(
                    {
                        "rules": load_rules(),
                        "source": "config/rules.v1.json",
                    }
                )
                return
            if request_path == "/api/reports":
                active_job = jobs.active()
                if active_job:
                    self._send_json(
                        {
                            "reports": [],
                            "context": {
                                "dag_name": active_job["dag_name"],
                                "state": active_job["status"],
                                "application_count": active_job["total"],
                            },
                        }
                    )
                    return
                index = self._dashboard_index()
                requested_ids = index.get("report_ids") or []
                paths = [report_root / f"{report_id}.json" for report_id in requested_ids]
                if not paths:
                    paths = [path for path in sorted(report_root.glob("*.json")) if path.name != "latest_dashboard.json"]
                reports = []
                for path in paths:
                    try:
                        report = json.loads(path.read_text(encoding="utf-8"))
                        if not isinstance(report.get("application"), dict):
                            continue
                    except (OSError, json.JSONDecodeError):
                        continue
                    item = _summary(report)
                    item["report_id"] = path.stem
                    reports.append(item)
                reports.sort(key=lambda item: item.get("application", {}).get("start_time") or 0)
                self._send_json({"reports": reports, "context": index})
                return
            if request_path.startswith("/api/jobs/"):
                job_id = request_path.removeprefix("/api/jobs/")
                job = jobs.get(job_id)
                if not job:
                    self._send_json({"error": "Задача не найдена"}, HTTPStatus.NOT_FOUND)
                    return
                self._send_json(job)
                return
            if request_path.startswith("/api/reports/"):
                report_id = request_path.removeprefix("/api/reports/")
                if not report_id or any(char not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_" for char in report_id):
                    self._send_json({"error": "Invalid report id"}, HTTPStatus.BAD_REQUEST)
                    return
                path = report_root / f"{report_id}.json"
                if not path.is_file():
                    self._send_json({"error": "Report not found"}, HTTPStatus.NOT_FOUND)
                    return
                self._send_file(path)
                return
            static_name = "index.html" if request_path in {"", "/"} else request_path.lstrip("/")
            path = (static_root / static_name).resolve()
            if static_root not in path.parents and path != static_root:
                self.send_error(HTTPStatus.NOT_FOUND)
                return
            self._send_file(path)

        def log_message(self, format: str, *args: object) -> None:
            print(f"[{self.log_date_time_string()}] {format % args}")

    server = ThreadingHTTPServer((host, port), Handler)
    print(f"Spark Monitor: http://{host}:{port}")
    print(f"Reports: {report_root}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
