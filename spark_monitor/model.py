from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class SourceIssue:
    source: str
    line_number: int
    message: str


@dataclass(slots=True)
class ResourceProfile:
    profile_id: int
    executor_resources: dict[str, dict[str, Any]] = field(default_factory=dict)
    task_resources: dict[str, dict[str, Any]] = field(default_factory=dict)

    @property
    def executor_cores(self) -> float | None:
        request = self.executor_resources.get("cores", {})
        value = request.get("Amount")
        return float(value) if value is not None else None

    @property
    def task_cpus(self) -> float | None:
        request = self.task_resources.get("cpus", {})
        value = request.get("Amount")
        return float(value) if value is not None else None


@dataclass(slots=True)
class Executor:
    executor_id: str
    host: str | None
    resource_profile_id: int
    added_at: int
    removed_at: int | None
    total_cores: float | None
    removal_reason: str | None = None
    resources: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class Job:
    job_id: int
    submission_time: int | None
    completion_time: int | None = None
    stage_ids: list[int] = field(default_factory=list)
    properties: dict[str, str] = field(default_factory=dict)
    status: str = "RUNNING"
    result: Any = None


@dataclass(slots=True)
class StageAttempt:
    stage_id: int
    attempt_id: int
    name: str | None = None
    num_tasks: int | None = None
    submission_time: int | None = None
    completion_time: int | None = None
    failure_reason: str | None = None
    resource_profile_id: int = 0
    details: str | None = None

    @property
    def key(self) -> tuple[int, int]:
        return self.stage_id, self.attempt_id


@dataclass(slots=True)
class TaskAttempt:
    task_id: int
    stage_id: int
    stage_attempt_id: int
    partition_id: int
    attempt: int
    executor_id: str | None
    host: str | None
    launch_time: int | None
    finish_time: int | None
    successful: bool
    failed: bool
    killed: bool
    speculative: bool
    end_reason: Any
    metrics: dict[str, int | float | None]
    accum_updates: dict[int, float] = field(default_factory=dict)

    @property
    def duration_ms(self) -> int | None:
        if self.launch_time is None or self.finish_time is None:
            return None
        return max(0, self.finish_time - self.launch_time)


@dataclass(slots=True)
class SQLExecution:
    execution_id: int
    root_execution_id: int | None
    description: str | None
    details: str | None
    start_time: int | None
    end_time: int | None = None
    status: str = "RUNNING"
    error_message: str | None = None
    physical_plan: str | None = None
    initial_plan: dict[str, Any] | None = None
    final_physical_plan: str | None = None
    final_plan: dict[str, Any] | None = None
    aqe_updated: bool = False
    job_ids: list[int] = field(default_factory=list)
    driver_accum_updates: dict[int, float] = field(default_factory=dict)


@dataclass(slots=True)
class NormalizedApplication:
    application_id: str | None = None
    application_attempt_id: str | None = None
    application_name: str | None = None
    spark_version: str | None = None
    spark_user: str | None = None
    start_time: int | None = None
    end_time: int | None = None
    completed: bool = False
    log_complete: bool = True
    properties: dict[str, str] = field(default_factory=dict)
    resource_profiles: dict[int, ResourceProfile] = field(default_factory=dict)
    executors: dict[str, Executor] = field(default_factory=dict)
    jobs: dict[int, Job] = field(default_factory=dict)
    stages: dict[tuple[int, int], StageAttempt] = field(default_factory=dict)
    tasks: list[TaskAttempt] = field(default_factory=list)
    sql_executions: dict[int, SQLExecution] = field(default_factory=dict)
    executor_memory_metrics_available: bool = False
    structured_streaming_detected: bool = False
    unknown_event_count: int = 0
    event_count: int = 0
    source_issues: list[SourceIssue] = field(default_factory=list)

