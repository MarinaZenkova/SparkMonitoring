from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any


SCHEMA = """
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS applications (
    application_id TEXT NOT NULL,
    application_attempt_id TEXT NOT NULL DEFAULT '',
    application_name TEXT,
    start_time INTEGER,
    end_time INTEGER,
    status TEXT,
    schema_version TEXT NOT NULL,
    parser_version TEXT NOT NULL,
    rules_version TEXT NOT NULL,
    report_json TEXT NOT NULL,
    PRIMARY KEY (application_id, application_attempt_id)
);
CREATE TABLE IF NOT EXISTS application_configs (
    application_id TEXT NOT NULL,
    config_key TEXT NOT NULL,
    configured_value TEXT,
    effective_value TEXT,
    source TEXT,
    PRIMARY KEY (application_id, config_key)
);
CREATE TABLE IF NOT EXISTS resource_profiles (
    application_id TEXT NOT NULL,
    resource_profile_id INTEGER NOT NULL,
    data_json TEXT NOT NULL,
    PRIMARY KEY (application_id, resource_profile_id)
);
CREATE TABLE IF NOT EXISTS executors (
    application_id TEXT NOT NULL,
    executor_id TEXT NOT NULL,
    data_json TEXT NOT NULL,
    PRIMARY KEY (application_id, executor_id)
);
CREATE TABLE IF NOT EXISTS executor_intervals (
    application_id TEXT NOT NULL,
    executor_id TEXT NOT NULL,
    start_time INTEGER NOT NULL,
    end_time INTEGER NOT NULL,
    data_json TEXT NOT NULL,
    PRIMARY KEY (application_id, executor_id, start_time)
);
CREATE TABLE IF NOT EXISTS jobs (
    application_id TEXT NOT NULL,
    job_id INTEGER NOT NULL,
    data_json TEXT NOT NULL,
    PRIMARY KEY (application_id, job_id)
);
CREATE TABLE IF NOT EXISTS stages (
    application_id TEXT NOT NULL,
    stage_id INTEGER NOT NULL,
    stage_attempt_id INTEGER NOT NULL,
    data_json TEXT NOT NULL,
    PRIMARY KEY (application_id, stage_id, stage_attempt_id)
);
CREATE TABLE IF NOT EXISTS stage_metrics (
    application_id TEXT NOT NULL,
    stage_id INTEGER NOT NULL,
    stage_attempt_id INTEGER NOT NULL,
    metrics_json TEXT NOT NULL,
    PRIMARY KEY (application_id, stage_id, stage_attempt_id)
);
CREATE TABLE IF NOT EXISTS tasks (
    application_id TEXT NOT NULL,
    task_id INTEGER NOT NULL,
    stage_id INTEGER NOT NULL,
    stage_attempt_id INTEGER NOT NULL,
    data_json TEXT NOT NULL,
    PRIMARY KEY (application_id, task_id)
);
CREATE TABLE IF NOT EXISTS task_metrics (
    application_id TEXT NOT NULL,
    task_id INTEGER NOT NULL,
    metrics_json TEXT NOT NULL,
    PRIMARY KEY (application_id, task_id)
);
CREATE TABLE IF NOT EXISTS sql_executions (
    application_id TEXT NOT NULL,
    execution_id INTEGER NOT NULL,
    data_json TEXT NOT NULL,
    PRIMARY KEY (application_id, execution_id)
);
CREATE TABLE IF NOT EXISTS sql_plan_nodes (
    application_id TEXT NOT NULL,
    execution_id INTEGER NOT NULL,
    node_id INTEGER NOT NULL,
    data_json TEXT NOT NULL,
    PRIMARY KEY (application_id, execution_id, node_id)
);
CREATE TABLE IF NOT EXISTS sql_plan_edges (
    application_id TEXT NOT NULL,
    execution_id INTEGER NOT NULL,
    parent_node_id INTEGER NOT NULL,
    child_node_id INTEGER NOT NULL,
    PRIMARY KEY (application_id, execution_id, parent_node_id, child_node_id)
);
CREATE TABLE IF NOT EXISTS sql_plan_metrics (
    application_id TEXT NOT NULL,
    execution_id INTEGER NOT NULL,
    node_id INTEGER NOT NULL,
    accumulator_id INTEGER NOT NULL,
    metric_json TEXT NOT NULL,
    PRIMARY KEY (application_id, execution_id, node_id, accumulator_id)
);
CREATE TABLE IF NOT EXISTS diagnostics (
    application_id TEXT NOT NULL,
    diagnostic_block TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    problem_code TEXT NOT NULL,
    severity TEXT NOT NULL,
    data_json TEXT NOT NULL,
    PRIMARY KEY (application_id, diagnostic_block, ordinal)
);
"""


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def persist_report(report: dict[str, Any], database_path: str | Path) -> Path:
    path = Path(database_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    application = report["application"]
    application_id = application.get("application_id") or "unknown"
    attempt_id = application.get("application_attempt_id") or ""
    connection = sqlite3.connect(path)
    try:
        connection.executescript(SCHEMA)
        tables = (
            "application_configs",
            "resource_profiles",
            "executors",
            "executor_intervals",
            "jobs",
            "stages",
            "stage_metrics",
            "tasks",
            "task_metrics",
            "sql_executions",
            "sql_plan_nodes",
            "sql_plan_edges",
            "sql_plan_metrics",
            "diagnostics",
        )
        for table in tables:
            connection.execute(f"DELETE FROM {table} WHERE application_id = ?", (application_id,))
        connection.execute(
            "DELETE FROM applications WHERE application_id = ? AND application_attempt_id = ?",
            (application_id, attempt_id),
        )
        connection.execute(
            "INSERT INTO applications VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                application_id,
                attempt_id,
                application.get("application_name"),
                application.get("start_time"),
                application.get("end_time"),
                application.get("status"),
                report["schema_version"],
                report["parser_version"],
                report["rules_version"],
                _json(report),
            ),
        )
        for key, value in report["configuration"].items():
            connection.execute(
                "INSERT INTO application_configs VALUES (?, ?, ?, ?, ?)",
                (application_id, key, value.get("configured_value"), value.get("effective_value"), value.get("source")),
            )
        for profile in report["resources"]["resource_profiles"]:
            connection.execute(
                "INSERT INTO resource_profiles VALUES (?, ?, ?)",
                (application_id, profile["resource_profile_id"], _json(profile)),
            )
        for executor in report["resources"]["executors"]:
            connection.execute(
                "INSERT INTO executors VALUES (?, ?, ?)",
                (application_id, executor["executor_id"], _json(executor)),
            )
        for interval in report["resources"]["executor_intervals"]:
            connection.execute(
                "INSERT INTO executor_intervals VALUES (?, ?, ?, ?, ?)",
                (application_id, interval["executor_id"], interval["start_time"], interval["end_time"], _json(interval)),
            )
        for job in report["normalized"]["jobs"]:
            connection.execute("INSERT INTO jobs VALUES (?, ?, ?)", (application_id, job["job_id"], _json(job)))
        for stage in report["stages"]:
            key = (application_id, stage["stage_id"], stage["stage_attempt_id"])
            connection.execute("INSERT INTO stages VALUES (?, ?, ?, ?)", (*key, _json(stage)))
            connection.execute("INSERT INTO stage_metrics VALUES (?, ?, ?, ?)", (*key, _json(stage)))
        for task in report["normalized"]["tasks"]:
            connection.execute(
                "INSERT INTO tasks VALUES (?, ?, ?, ?, ?)",
                (application_id, task["task_id"], task["stage_id"], task["stage_attempt_id"], _json(task)),
            )
            connection.execute(
                "INSERT INTO task_metrics VALUES (?, ?, ?)",
                (application_id, task["task_id"], _json(task["metrics"])),
            )
        for execution in report["sql_executions"]:
            compact = {key: value for key, value in execution.items() if key != "plan_nodes"}
            connection.execute(
                "INSERT INTO sql_executions VALUES (?, ?, ?)",
                (application_id, execution["execution_id"], _json(compact)),
            )
            for node in execution["plan_nodes"]:
                node_without_metrics = {key: value for key, value in node.items() if key != "metrics"}
                connection.execute(
                    "INSERT INTO sql_plan_nodes VALUES (?, ?, ?, ?)",
                    (application_id, execution["execution_id"], node["node_id"], _json(node_without_metrics)),
                )
                for child_id in node["children"]:
                    connection.execute(
                        "INSERT INTO sql_plan_edges VALUES (?, ?, ?, ?)",
                        (application_id, execution["execution_id"], node["node_id"], child_id),
                    )
                for index, metric in enumerate(node["metrics"]):
                    accumulator_id = metric.get("accumulator_id")
                    if accumulator_id is None:
                        accumulator_id = -(index + 1)
                    connection.execute(
                        "INSERT INTO sql_plan_metrics VALUES (?, ?, ?, ?, ?)",
                        (application_id, execution["execution_id"], node["node_id"], accumulator_id, _json(metric)),
                    )
        for block_name, block in report["diagnostics"].items():
            for ordinal, problem in enumerate(block["problems"]):
                connection.execute(
                    "INSERT INTO diagnostics VALUES (?, ?, ?, ?, ?, ?)",
                    (application_id, block_name, ordinal, problem["problem_code"], problem["severity"], _json(problem)),
                )
        connection.commit()
    finally:
        connection.close()
    return path
