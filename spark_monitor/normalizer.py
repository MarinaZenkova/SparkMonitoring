from __future__ import annotations

import json
from typing import Any, Iterable

from .model import (
    Executor,
    Job,
    NormalizedApplication,
    ResourceProfile,
    SQLExecution,
    StageAttempt,
    TaskAttempt,
)
from .reader import EventRecord


SQL_START = "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart"
SQL_END = "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd"
SQL_AQE = "org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate"
SQL_DRIVER_ACCUM = "org.apache.spark.sql.execution.ui.SparkListenerDriverAccumUpdates"
SQL_METRIC_UPDATE = "org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveSQLMetricUpdates"

KNOWN_EVENTS = {
    "SparkListenerLogStart",
    "SparkListenerApplicationStart",
    "SparkListenerApplicationEnd",
    "SparkListenerEnvironmentUpdate",
    "SparkListenerResourceProfileAdded",
    "SparkListenerExecutorAdded",
    "SparkListenerExecutorRemoved",
    "SparkListenerBlockManagerAdded",
    "SparkListenerBlockManagerRemoved",
    "SparkListenerJobStart",
    "SparkListenerJobEnd",
    "SparkListenerStageSubmitted",
    "SparkListenerStageCompleted",
    "SparkListenerTaskStart",
    "SparkListenerTaskGettingResult",
    "SparkListenerTaskEnd",
    "SparkListenerStageExecutorMetrics",
    "SparkListenerExecutorMetricsUpdate",
    "SparkListenerSpeculativeTaskSubmitted",
    SQL_START,
    SQL_END,
    SQL_AQE,
    SQL_DRIVER_ACCUM,
    SQL_METRIC_UPDATE,
}


def _pairs(value: Any) -> dict[str, str]:
    if isinstance(value, dict):
        return {str(key): str(item) for key, item in value.items()}
    if isinstance(value, list):
        return {str(pair[0]): str(pair[1]) for pair in value if isinstance(pair, list) and len(pair) >= 2}
    return {}


def _number(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace(",", "")
    try:
        return float(text)
    except ValueError:
        return None


def _int(value: Any) -> int | None:
    number = _number(value)
    return int(number) if number is not None else None


def _reason_name(reason: Any) -> str:
    if isinstance(reason, str):
        return reason
    if isinstance(reason, dict):
        return str(reason.get("Reason") or reason.get("Class Name") or reason.get("Message") or json.dumps(reason, sort_keys=True))
    return str(reason or "")


def _stage_from_info(info: dict[str, Any]) -> StageAttempt:
    return StageAttempt(
        stage_id=int(info.get("Stage ID", -1)),
        attempt_id=int(info.get("Stage Attempt ID", 0)),
        name=info.get("Stage Name"),
        num_tasks=_int(info.get("Number of Tasks")),
        submission_time=_int(info.get("Submission Time")),
        completion_time=_int(info.get("Completion Time")),
        failure_reason=info.get("Failure Reason"),
        resource_profile_id=int(info.get("Resource Profile Id", 0)),
        details=info.get("Details"),
    )


def _task_metrics(event: dict[str, Any]) -> dict[str, int | float | None]:
    metrics = event.get("Task Metrics") or {}
    shuffle_read = metrics.get("Shuffle Read Metrics") or {}
    shuffle_write = metrics.get("Shuffle Write Metrics") or {}
    input_metrics = metrics.get("Input Metrics") or {}
    output_metrics = metrics.get("Output Metrics") or {}
    cpu_ns = _number(metrics.get("Executor CPU Time"))
    deserialize_cpu_ns = _number(metrics.get("Executor Deserialize CPU Time"))
    return {
        "executor_run_time_ms": _number(metrics.get("Executor Run Time")),
        "executor_cpu_time_ms": cpu_ns / 1_000_000 if cpu_ns is not None else None,
        "executor_deserialize_time_ms": _number(metrics.get("Executor Deserialize Time")),
        "executor_deserialize_cpu_time_ms": deserialize_cpu_ns / 1_000_000 if deserialize_cpu_ns is not None else None,
        "result_serialization_time_ms": _number(metrics.get("Result Serialization Time")),
        "result_size_bytes": _number(metrics.get("Result Size")),
        "jvm_gc_time_ms": _number(metrics.get("JVM GC Time")),
        "memory_bytes_spilled": _number(metrics.get("Memory Bytes Spilled")),
        "disk_bytes_spilled": _number(metrics.get("Disk Bytes Spilled")),
        "peak_execution_memory_bytes": _number(metrics.get("Peak Execution Memory")),
        "input_bytes": _number(input_metrics.get("Bytes Read")),
        "input_records": _number(input_metrics.get("Records Read")),
        "output_bytes": _number(output_metrics.get("Bytes Written")),
        "output_records": _number(output_metrics.get("Records Written")),
        "shuffle_remote_bytes_read": _number(shuffle_read.get("Remote Bytes Read")),
        "shuffle_local_bytes_read": _number(shuffle_read.get("Local Bytes Read")),
        "shuffle_remote_blocks_fetched": _number(shuffle_read.get("Remote Blocks Fetched")),
        "shuffle_local_blocks_fetched": _number(shuffle_read.get("Local Blocks Fetched")),
        "shuffle_fetch_wait_ms": _number(shuffle_read.get("Fetch Wait Time")),
        "shuffle_read_records": _number(shuffle_read.get("Total Records Read")),
        "shuffle_write_bytes": _number(shuffle_write.get("Shuffle Bytes Written")),
        "shuffle_write_records": _number(shuffle_write.get("Shuffle Records Written")),
        "shuffle_write_time_ms": (
            _number(shuffle_write.get("Shuffle Write Time")) / 1_000_000
            if _number(shuffle_write.get("Shuffle Write Time")) is not None
            else None
        ),
    }


def _accumulators(task_info: dict[str, Any]) -> dict[int, float]:
    result: dict[int, float] = {}
    for item in task_info.get("Accumulables", []) or []:
        accumulator_id = _int(item.get("ID"))
        update = _number(item.get("Update"))
        if accumulator_id is not None and update is not None:
            result[accumulator_id] = result.get(accumulator_id, 0.0) + update
    return result


def normalize(records: Iterable[EventRecord]) -> NormalizedApplication:
    model = NormalizedApplication()
    for record in records:
        event = record.event
        name = str(event.get("Event", ""))
        model.event_count += 1
        if name not in KNOWN_EVENTS:
            model.unknown_event_count += 1

        if name == "SparkListenerLogStart":
            model.spark_version = event.get("Spark Version")
        elif name == "SparkListenerApplicationStart":
            model.application_id = event.get("App ID") or model.application_id
            model.application_attempt_id = event.get("App Attempt ID")
            model.application_name = event.get("App Name")
            model.spark_user = event.get("User")
            model.start_time = _int(event.get("Timestamp"))
        elif name == "SparkListenerApplicationEnd":
            model.end_time = _int(event.get("Timestamp"))
            model.completed = True
        elif name == "SparkListenerEnvironmentUpdate":
            model.properties.update(_pairs(event.get("Spark Properties")))
            if not model.application_id:
                model.application_id = model.properties.get("spark.app.id")
            if not model.application_name:
                model.application_name = model.properties.get("spark.app.name")
            streaming_keys = ("spark.sql.streaming", "spark.streaming")
            model.structured_streaming_detected = any(
                key.startswith(streaming_keys) for key in model.properties
            )
        elif name == "SparkListenerResourceProfileAdded":
            profile_id = int(event.get("Resource Profile Id", 0))
            model.resource_profiles[profile_id] = ResourceProfile(
                profile_id,
                event.get("Executor Resource Requests") or {},
                event.get("Task Resource Requests") or {},
            )
        elif name == "SparkListenerExecutorAdded":
            info = event.get("Executor Info") or {}
            executor_id = str(event.get("Executor ID"))
            model.executors[executor_id] = Executor(
                executor_id=executor_id,
                host=info.get("Host"),
                resource_profile_id=int(info.get("Resource Profile Id", 0)),
                added_at=int(event.get("Timestamp", 0)),
                removed_at=None,
                total_cores=_number(info.get("Total Cores")),
                resources=info.get("Resources") or {},
            )
        elif name == "SparkListenerExecutorRemoved":
            executor_id = str(event.get("Executor ID"))
            executor = model.executors.get(executor_id)
            if executor:
                executor.removed_at = _int(event.get("Timestamp"))
                executor.removal_reason = event.get("Removed Reason")
        elif name == "SparkListenerJobStart":
            job_id = int(event.get("Job ID", -1))
            properties = _pairs(event.get("Properties"))
            job = Job(
                job_id=job_id,
                submission_time=_int(event.get("Submission Time")),
                stage_ids=[int(value) for value in event.get("Stage IDs", [])],
                properties=properties,
            )
            model.jobs[job_id] = job
            # JobStart contains skipped/planned stages that never emit
            # StageSubmitted/StageCompleted. Preserve them in the normalized
            # graph without treating their planned task count as executed work.
            for stage_info in event.get("Stage Infos", []) or []:
                stage = _stage_from_info(stage_info)
                model.stages.setdefault(stage.key, stage)
            execution_id = _int(properties.get("spark.sql.execution.id"))
            if execution_id is not None:
                sql = model.sql_executions.get(execution_id)
                if sql is None:
                    sql = SQLExecution(execution_id, None, None, None, None)
                    model.sql_executions[execution_id] = sql
                if job_id not in sql.job_ids:
                    sql.job_ids.append(job_id)
        elif name == "SparkListenerJobEnd":
            job_id = int(event.get("Job ID", -1))
            job = model.jobs.get(job_id)
            if job:
                job.completion_time = _int(event.get("Completion Time"))
                job.result = event.get("Job Result")
                job.status = "SUCCEEDED" if "JobSucceeded" in _reason_name(job.result) else "FAILED"
        elif name in {"SparkListenerStageSubmitted", "SparkListenerStageCompleted"}:
            stage = _stage_from_info(event.get("Stage Info") or {})
            existing = model.stages.get(stage.key)
            if existing and name == "SparkListenerStageCompleted":
                existing.name = stage.name or existing.name
                existing.num_tasks = stage.num_tasks if stage.num_tasks is not None else existing.num_tasks
                existing.submission_time = stage.submission_time or existing.submission_time
                existing.completion_time = stage.completion_time
                existing.failure_reason = stage.failure_reason
                existing.resource_profile_id = stage.resource_profile_id
                existing.details = stage.details or existing.details
            else:
                model.stages[stage.key] = stage
        elif name == "SparkListenerTaskEnd":
            info = event.get("Task Info") or {}
            reason = event.get("Task End Reason")
            failed = bool(info.get("Failed")) or "Success" not in _reason_name(reason)
            killed = bool(info.get("Killed")) or "Killed" in _reason_name(reason)
            task = TaskAttempt(
                task_id=int(info.get("Task ID", -1)),
                stage_id=int(event.get("Stage ID", -1)),
                stage_attempt_id=int(event.get("Stage Attempt ID", 0)),
                partition_id=int(info.get("Partition ID", info.get("Index", -1))),
                attempt=int(info.get("Attempt", 0)),
                executor_id=str(info.get("Executor ID")) if info.get("Executor ID") is not None else None,
                host=info.get("Host"),
                launch_time=_int(info.get("Launch Time")),
                finish_time=_int(info.get("Finish Time")),
                successful=not failed and not killed,
                failed=failed and not killed,
                killed=killed,
                speculative=bool(info.get("Speculative")),
                end_reason=reason,
                metrics=_task_metrics(event),
                accum_updates=_accumulators(info),
            )
            model.tasks.append(task)
            executor_metrics = event.get("Task Executor Metrics") or {}
            if any((_number(value) or 0) > 0 for value in executor_metrics.values()):
                model.executor_memory_metrics_available = True
        elif name == "SparkListenerStageExecutorMetrics":
            model.executor_memory_metrics_available = True
        elif name == SQL_START:
            execution_id = int(event.get("executionId", -1))
            current = model.sql_executions.get(execution_id)
            execution = SQLExecution(
                execution_id=execution_id,
                root_execution_id=_int(event.get("rootExecutionId")),
                description=event.get("description"),
                details=event.get("details"),
                start_time=_int(event.get("time")),
                physical_plan=event.get("physicalPlanDescription"),
                initial_plan=event.get("sparkPlanInfo"),
                job_ids=current.job_ids if current else [],
            )
            model.sql_executions[execution_id] = execution
        elif name == SQL_END:
            execution_id = int(event.get("executionId", -1))
            execution = model.sql_executions.setdefault(execution_id, SQLExecution(execution_id, None, None, None, None))
            execution.end_time = _int(event.get("time"))
            execution.error_message = event.get("errorMessage")
            execution.status = "FAILED" if execution.error_message else "SUCCEEDED"
        elif name == SQL_AQE:
            execution_id = int(event.get("executionId", -1))
            execution = model.sql_executions.setdefault(execution_id, SQLExecution(execution_id, None, None, None, None))
            execution.final_physical_plan = event.get("physicalPlanDescription")
            execution.final_plan = event.get("sparkPlanInfo")
            execution.aqe_updated = True
        elif name == SQL_DRIVER_ACCUM:
            execution_id = int(event.get("executionId", -1))
            execution = model.sql_executions.setdefault(execution_id, SQLExecution(execution_id, None, None, None, None))
            for pair in event.get("accumUpdates", []) or []:
                if isinstance(pair, list) and len(pair) >= 2:
                    accumulator_id = _int(pair[0])
                    value = _number(pair[1])
                    if accumulator_id is not None and value is not None:
                        execution.driver_accum_updates[accumulator_id] = value

    return model


def normalize_reader(reader: Any) -> NormalizedApplication:
    model = normalize(reader)
    model.log_complete = bool(reader.complete and model.completed)
    model.source_issues.extend(reader.issues)
    return model
