from __future__ import annotations

import math
from collections import defaultdict
from typing import Any, Iterable

from .config import configuration, parse_bool, sanitized_properties
from .model import Executor, NormalizedApplication, StageAttempt, TaskAttempt
from .sql_analysis import analyze_sql


def _sum(tasks: Iterable[TaskAttempt], key: str) -> float | None:
    values = [task.metrics.get(key) for task in tasks]
    available = [float(value) for value in values if value is not None]
    return sum(available) if available else None


def _percentile(values: list[float], percentile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * percentile
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    fraction = position - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def _distribution(tasks: list[TaskAttempt], metric: str) -> dict[str, float | None]:
    if metric == "task_duration_ms":
        values = [float(task.duration_ms) for task in tasks if task.duration_ms is not None]
    else:
        values = [float(value) for task in tasks if (value := task.metrics.get(metric)) is not None]
    return {
        "p50": _percentile(values, 0.50),
        "p75": _percentile(values, 0.75),
        "p95": _percentile(values, 0.95),
        "p99": _percentile(values, 0.99),
        "max": max(values) if values else None,
    }


def _ratio(numerator: float | None, denominator: float | None, multiplier: float = 1.0) -> float | None:
    if numerator is None or denominator is None or denominator <= 0:
        return None
    return numerator / denominator * multiplier


def logical_tasks(model: NormalizedApplication) -> list[TaskAttempt]:
    successful_stage_attempts: dict[int, int] = {}
    for stage in model.stages.values():
        if stage.completion_time is not None and not stage.failure_reason:
            successful_stage_attempts[stage.stage_id] = max(
                stage.attempt_id, successful_stage_attempts.get(stage.stage_id, -1)
            )
    if not successful_stage_attempts:
        for task in model.tasks:
            if task.successful:
                successful_stage_attempts[task.stage_id] = max(
                    task.stage_attempt_id, successful_stage_attempts.get(task.stage_id, -1)
                )
    candidates = [
        task
        for task in model.tasks
        if task.successful and successful_stage_attempts.get(task.stage_id) == task.stage_attempt_id
    ]
    chosen: dict[tuple[int, int, int], TaskAttempt] = {}
    for task in candidates:
        key = (task.stage_id, task.stage_attempt_id, task.partition_id)
        current = chosen.get(key)
        order = (task.finish_time or 2**63, task.task_id)
        current_order = (current.finish_time or 2**63, current.task_id) if current else None
        if current is None or order < current_order:
            chosen[key] = task
    return sorted(chosen.values(), key=lambda task: (task.stage_id, task.stage_attempt_id, task.partition_id))


def _application_bounds(model: NormalizedApplication) -> tuple[int | None, int | None]:
    start = model.start_time
    end = model.end_time
    if start is None:
        times = [task.launch_time for task in model.tasks if task.launch_time is not None]
        start = min(times) if times else None
    if end is None:
        times = [task.finish_time for task in model.tasks if task.finish_time is not None]
        end = max(times) if times else None
    return start, end


def _executor_end(executor: Executor, application_end: int | None) -> int:
    end = executor.removed_at or application_end or executor.added_at
    return max(executor.added_at, end)


def _timeline_stats(model: NormalizedApplication, start: int | None, end: int | None) -> dict[str, Any]:
    if start is None or end is None or end <= start:
        return {
            "executor_count_min": None,
            "executor_count_avg": None,
            "executor_count_max": None,
            "allocated_cores_min": None,
            "allocated_cores_avg": None,
            "allocated_cores_max": None,
            "executor_core_ms_capacity": None,
            "task_slot_capacity_ms": None,
            "peak_available_slots": None,
            "average_available_slots": None,
            "intervals": [],
        }
    events: list[tuple[int, int, float, float, str]] = []
    intervals: list[dict[str, Any]] = []
    for executor in model.executors.values():
        added = min(max(executor.added_at, start), end)
        removed = min(max(_executor_end(executor, end), start), end)
        cores = float(executor.total_cores or 0)
        profile = model.resource_profiles.get(executor.resource_profile_id)
        task_cpus = profile.task_cpus if profile and profile.task_cpus else None
        slots = cores / task_cpus if task_cpus and task_cpus > 0 else 0.0
        if removed > added:
            events.append((added, 1, cores, slots, executor.executor_id))
            events.append((removed, -1, -cores, -slots, executor.executor_id))
            intervals.append(
                {
                    "executor_id": executor.executor_id,
                    "start_time": added,
                    "end_time": removed,
                    "lifetime_ms": removed - added,
                    "cores": cores,
                    "task_slots": slots,
                }
            )
    events.sort(key=lambda value: (value[0], value[1]))
    cursor = start
    count = 0
    cores = 0.0
    slots = 0.0
    weighted_count = weighted_cores = weighted_slots = 0.0
    mins: list[float | None] = [None, None, None]
    maxs = [0.0, 0.0, 0.0]
    for timestamp, count_delta, core_delta, slot_delta, _ in events:
        duration = max(0, timestamp - cursor)
        weighted_count += count * duration
        weighted_cores += cores * duration
        weighted_slots += slots * duration
        if duration > 0 and count > 0:
            mins[0] = count if mins[0] is None else min(mins[0], count)
            mins[1] = cores if mins[1] is None else min(mins[1], cores)
            mins[2] = slots if mins[2] is None else min(mins[2], slots)
            maxs[0] = max(maxs[0], count)
            maxs[1] = max(maxs[1], cores)
            maxs[2] = max(maxs[2], slots)
        count += count_delta
        cores += core_delta
        slots += slot_delta
        cursor = timestamp
    duration = max(0, end - cursor)
    weighted_count += count * duration
    weighted_cores += cores * duration
    weighted_slots += slots * duration
    if duration > 0 and count > 0:
        mins[0] = count if mins[0] is None else min(mins[0], count)
        mins[1] = cores if mins[1] is None else min(mins[1], cores)
        mins[2] = slots if mins[2] is None else min(mins[2], slots)
    maxs[0] = max(maxs[0], count)
    maxs[1] = max(maxs[1], cores)
    maxs[2] = max(maxs[2], slots)
    wall = end - start
    return {
        "executor_count_min": mins[0],
        "executor_count_avg": weighted_count / wall,
        "executor_count_max": maxs[0],
        "allocated_cores_min": mins[1],
        "allocated_cores_avg": weighted_cores / wall,
        "allocated_cores_max": maxs[1],
        "executor_core_ms_capacity": weighted_cores,
        "task_slot_capacity_ms": weighted_slots,
        "peak_available_slots": maxs[2],
        "average_available_slots": weighted_slots / wall,
        "intervals": sorted(intervals, key=lambda item: (item["start_time"], item["executor_id"])),
    }


def _running_task_stats(tasks: list[TaskAttempt], start: int | None, end: int | None) -> tuple[float | None, int | None]:
    if start is None or end is None or end <= start:
        return None, None
    events: list[tuple[int, int]] = []
    total_duration = 0
    for task in tasks:
        if task.launch_time is not None and task.finish_time is not None:
            events.append((task.launch_time, 1))
            events.append((task.finish_time, -1))
            total_duration += max(0, task.finish_time - task.launch_time)
    active = peak = 0
    for _, delta in sorted(events, key=lambda value: (value[0], value[1])):
        active += delta
        peak = max(peak, active)
    return total_duration / (end - start), peak


def _capacity_for_window(model: NormalizedApplication, start: int | None, end: int | None) -> float | None:
    if start is None or end is None or end <= start:
        return None
    capacity = 0.0
    for executor in model.executors.values():
        overlap = max(0, min(end, _executor_end(executor, end)) - max(start, executor.added_at))
        profile = model.resource_profiles.get(executor.resource_profile_id)
        task_cpus = profile.task_cpus if profile and profile.task_cpus else None
        if task_cpus and task_cpus > 0 and executor.total_cores is not None:
            capacity += overlap * executor.total_cores / task_cpus
    return capacity or None


def _stage_metrics(
    model: NormalizedApplication,
    logical: list[TaskAttempt],
    app_duration: int | None,
    skewed_task_multiplier: float,
) -> list[dict[str, Any]]:
    logical_by_stage: dict[tuple[int, int], list[TaskAttempt]] = defaultdict(list)
    actual_by_stage: dict[tuple[int, int], list[TaskAttempt]] = defaultdict(list)
    for task in logical:
        logical_by_stage[(task.stage_id, task.stage_attempt_id)].append(task)
    for task in model.tasks:
        actual_by_stage[(task.stage_id, task.stage_attempt_id)].append(task)
    result: list[dict[str, Any]] = []
    keys = sorted(set(model.stages) | set(actual_by_stage))
    for key in keys:
        stage = model.stages.get(key) or StageAttempt(key[0], key[1])
        actual = actual_by_stage.get(key, [])
        final = logical_by_stage.get(key, [])
        wall = (
            stage.completion_time - stage.submission_time
            if stage.submission_time is not None and stage.completion_time is not None
            else None
        )
        duration_dist = _distribution(final, "task_duration_ms")
        duration_median = duration_dist["p50"]
        duration_values = [float(task.duration_ms) for task in final if task.duration_ms is not None]
        skewed_tasks_pct = (
            sum(value > skewed_task_multiplier * duration_median for value in duration_values) / len(duration_values) * 100
            if duration_values and duration_median is not None and duration_median > 0
            else None
        )
        input_dist = _distribution(final, "input_bytes")
        shuffle_read_dist = _distribution(final, "shuffle_remote_bytes_read")
        local_shuffle_dist = _distribution(final, "shuffle_local_bytes_read")
        if shuffle_read_dist["p50"] is not None:
            combined_values = [
                float(task.metrics.get("shuffle_remote_bytes_read") or 0)
                + float(task.metrics.get("shuffle_local_bytes_read") or 0)
                for task in final
                if task.metrics.get("shuffle_remote_bytes_read") is not None
                or task.metrics.get("shuffle_local_bytes_read") is not None
            ]
            shuffle_read_dist = {
                "p50": _percentile(combined_values, 0.5),
                "p75": _percentile(combined_values, 0.75),
                "p95": _percentile(combined_values, 0.95),
                "p99": _percentile(combined_values, 0.99),
                "max": max(combined_values) if combined_values else None,
            }
        shuffle_write_dist = _distribution(final, "shuffle_write_bytes")
        peak_memory_dist = _distribution(final, "peak_execution_memory_bytes")
        capacity = _capacity_for_window(model, stage.submission_time, stage.completion_time)
        run_time = _sum(actual, "executor_run_time_ms")
        stage_metric = {
            "stage_id": stage.stage_id,
            "stage_attempt_id": stage.attempt_id,
            "name": stage.name,
            "status": (
                "FAILED"
                if stage.failure_reason
                else (
                    "SUCCEEDED"
                    if stage.completion_time is not None
                    else ("SKIPPED" if stage.submission_time is None and not actual else "RUNNING")
                )
            ),
            "failure_reason": stage.failure_reason,
            "resource_profile_id": stage.resource_profile_id,
            "submission_time": stage.submission_time,
            "completion_time": stage.completion_time,
            "wall_duration_ms": wall,
            "stage_wall_share_pct": _ratio(wall, app_duration, 100),
            "task_count": len(final),
            "task_attempt_count": len(actual),
            "executor_run_time_ms": run_time,
            "executor_cpu_time_ms": _sum(actual, "executor_cpu_time_ms"),
            "gc_time_ms": _sum(actual, "jvm_gc_time_ms"),
            "input_bytes": _sum(final, "input_bytes"),
            "input_records": _sum(final, "input_records"),
            "output_bytes": _sum(final, "output_bytes"),
            "output_records": _sum(final, "output_records"),
            "shuffle_read_bytes": (_sum(final, "shuffle_remote_bytes_read") or 0) + (_sum(final, "shuffle_local_bytes_read") or 0),
            "shuffle_read_records": _sum(final, "shuffle_read_records"),
            "shuffle_write_bytes": _sum(final, "shuffle_write_bytes"),
            "shuffle_write_records": _sum(final, "shuffle_write_records"),
            "shuffle_fetch_wait_ms": _sum(actual, "shuffle_fetch_wait_ms"),
            "memory_spill_bytes": _sum(actual, "memory_bytes_spilled"),
            "disk_spill_bytes": _sum(actual, "disk_bytes_spilled"),
            "task_duration": duration_dist,
            "input_bytes_distribution": input_dist,
            "shuffle_read_distribution": shuffle_read_dist,
            "shuffle_write_distribution": shuffle_write_dist,
            "peak_execution_memory_distribution": peak_memory_dist,
            "duration_skew_p95": _ratio(duration_dist["p95"], duration_dist["p50"]),
            "duration_skew_max": _ratio(duration_dist["max"], duration_dist["p50"]),
            "skewed_tasks_pct": skewed_tasks_pct,
            "skewed_task_multiplier": skewed_task_multiplier,
            "input_skew_p95": _ratio(input_dist["p95"], input_dist["p50"]),
            "shuffle_read_skew_p95": _ratio(shuffle_read_dist["p95"], shuffle_read_dist["p50"]),
            "shuffle_write_skew_p95": _ratio(shuffle_write_dist["p95"], shuffle_write_dist["p50"]),
            "stage_parallelism_utilization_pct": _ratio(run_time, capacity, 100),
        }
        result.append(stage_metric)
    return result


def _top_stages(stages: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    def top(key: str) -> list[dict[str, Any]]:
        available = [stage for stage in stages if stage.get(key) is not None]
        return sorted(available, key=lambda stage: (-float(stage[key]), stage["stage_id"], stage["stage_attempt_id"]))[:5]

    return {
        "by_wall_duration": top("wall_duration_ms"),
        "by_executor_runtime": top("executor_run_time_ms"),
        "by_cpu_time": top("executor_cpu_time_ms"),
        "by_shuffle": sorted(
            stages,
            key=lambda stage: -float((stage.get("shuffle_read_bytes") or 0) + (stage.get("shuffle_write_bytes") or 0)),
        )[:5],
        "by_spill": sorted(
            stages,
            key=lambda stage: -float((stage.get("memory_spill_bytes") or 0) + (stage.get("disk_spill_bytes") or 0)),
        )[:5],
    }


def calculate_metrics(model: NormalizedApplication, rules: dict[str, Any] | None = None) -> dict[str, Any]:
    actual = model.tasks
    logical = logical_tasks(model)
    start, end = _application_bounds(model)
    app_duration = end - start if start is not None and end is not None else None
    config = configuration(model.properties, model.spark_version)
    timeline = _timeline_stats(model, start, end)
    average_running, peak_running = _running_task_stats(actual, start, end)
    skewed_task_multiplier = float((rules or {}).get("skew", {}).get("skewed_task_multiplier", 3))
    stages = _stage_metrics(model, logical, app_duration, skewed_task_multiplier)

    executor_run = _sum(actual, "executor_run_time_ms")
    executor_cpu = _sum(actual, "executor_cpu_time_ms")
    gc_time = _sum(actual, "jvm_gc_time_ms")
    input_bytes = _sum(logical, "input_bytes")
    output_bytes = _sum(logical, "output_bytes")
    shuffle_remote = _sum(logical, "shuffle_remote_bytes_read")
    shuffle_local = _sum(logical, "shuffle_local_bytes_read")
    shuffle_read = (shuffle_remote or 0) + (shuffle_local or 0) if shuffle_remote is not None or shuffle_local is not None else None
    shuffle_write = _sum(logical, "shuffle_write_bytes")
    fetch_wait = _sum(actual, "shuffle_fetch_wait_ms")
    peak_memory = _distribution(logical, "peak_execution_memory_bytes")
    tasks_with_spill = sum(
        1
        for task in actual
        if (task.metrics.get("memory_bytes_spilled") or 0) > 0 or (task.metrics.get("disk_bytes_spilled") or 0) > 0
    )
    successful_jobs = sum(1 for job in model.jobs.values() if job.status == "SUCCEEDED")
    failed_jobs = sum(1 for job in model.jobs.values() if job.status == "FAILED")
    failed_stages = sum(1 for stage in model.stages.values() if stage.failure_reason)
    status = "FAILED" if failed_jobs or failed_stages else ("SUCCEEDED" if model.completed else "INCOMPLETE")

    sql = analyze_sql(model, logical, config)
    worst_duration = max((stage.get("duration_skew_p95") or 0 for stage in stages), default=0) or None
    worst_duration_max = max((stage.get("duration_skew_max") or 0 for stage in stages), default=0) or None
    worst_skewed_tasks_pct = max((stage.get("skewed_tasks_pct") or 0 for stage in stages), default=0) or None
    worst_input = max((stage.get("input_skew_p95") or 0 for stage in stages), default=0) or None
    worst_shuffle = max((stage.get("shuffle_read_skew_p95") or 0 for stage in stages), default=0) or None
    worst_stage = max(stages, key=lambda stage: stage.get("duration_skew_p95") or 0, default=None)
    total_shuffle = (shuffle_read or 0) + (shuffle_write or 0) if shuffle_read is not None or shuffle_write is not None else None
    io_denominator = sum(value or 0 for value in (input_bytes, output_bytes, shuffle_read, shuffle_write))
    wasted_core_ms = (
        max(0.0, timeline["executor_core_ms_capacity"] - executor_cpu)
        if timeline["executor_core_ms_capacity"] is not None and executor_cpu is not None
        else None
    )
    profile_task_cpus = {
        profile.task_cpus for profile in model.resource_profiles.values() if profile.task_cpus is not None
    }
    cpu_slot_identity_applicable = profile_task_cpus == {1.0}
    scan_result_file_values = [
        scan["result_data_files"] for scan in sql["scans"] if scan.get("result_data_files") is not None
    ]
    scan_skipped_file_values = [
        scan["skipped_data_files"] for scan in sql["scans"] if scan.get("skipped_data_files") is not None
    ]
    scan_result_files = sum(scan_result_file_values) if scan_result_file_values else None
    scan_skipped_files = sum(scan_skipped_file_values) if scan_skipped_file_values else None
    scan_considered_files = (
        scan_result_files + scan_skipped_files
        if scan_result_files is not None and scan_skipped_files is not None
        else None
    )

    executor_rows: list[dict[str, Any]] = []
    tasks_by_executor: dict[str, list[TaskAttempt]] = defaultdict(list)
    for task in actual:
        if task.executor_id:
            tasks_by_executor[task.executor_id].append(task)
    for executor_id in sorted(model.executors):
        executor = model.executors[executor_id]
        tasks = tasks_by_executor.get(executor_id, [])
        removed = _executor_end(executor, end)
        executor_rows.append(
            {
                "executor_id": executor_id,
                "host": executor.host,
                "resource_profile_id": executor.resource_profile_id,
                "added_at": executor.added_at,
                "removed_at": executor.removed_at,
                "lifetime_ms": removed - executor.added_at,
                "total_cores": executor.total_cores,
                "removal_reason": executor.removal_reason,
                "completed_tasks": sum(task.successful for task in tasks),
                "failed_tasks": sum(task.failed for task in tasks),
                "executor_run_time_ms": _sum(tasks, "executor_run_time_ms"),
                "executor_cpu_time_ms": _sum(tasks, "executor_cpu_time_ms"),
                "gc_time_ms": _sum(tasks, "jvm_gc_time_ms"),
                "input_bytes": _sum(tasks, "input_bytes"),
                "shuffle_read_bytes": ((_sum(tasks, "shuffle_remote_bytes_read") or 0) + (_sum(tasks, "shuffle_local_bytes_read") or 0)),
                "shuffle_write_bytes": _sum(tasks, "shuffle_write_bytes"),
                "memory_bytes_spilled": _sum(tasks, "memory_bytes_spilled"),
                "disk_bytes_spilled": _sum(tasks, "disk_bytes_spilled"),
            }
        )

    summary = {
        "duration_ms": app_duration,
        "jobs": len(model.jobs),
        "stages": len({stage_id for stage_id, _ in model.stages}),
        "stage_attempts": len(model.stages),
        "tasks": len(actual),
        "failed_tasks": sum(task.failed for task in actual),
        "killed_tasks": sum(task.killed for task in actual),
        "speculative_tasks": sum(task.speculative for task in actual),
        "failed_stages": failed_stages,
        "driver_memory_config": model.properties.get("spark.driver.memory"),
        "driver_cores_config": model.properties.get("spark.driver.cores"),
        "executor_memory_config": model.properties.get("spark.executor.memory"),
        "executor_memory_overhead_config": model.properties.get("spark.executor.memoryOverhead"),
        "executor_cores_config": model.properties.get("spark.executor.cores"),
        "dynamic_allocation": parse_bool(model.properties.get("spark.dynamicAllocation.enabled")),
        "min_executors": model.properties.get("spark.dynamicAllocation.minExecutors"),
        "initial_executors": model.properties.get("spark.dynamicAllocation.initialExecutors"),
        "max_executors": model.properties.get("spark.dynamicAllocation.maxExecutors"),
        "actual_peak_executors": timeline["executor_count_max"],
        "actual_average_executors": timeline["executor_count_avg"],
        "actual_peak_cores": timeline["allocated_cores_max"],
        "actual_average_cores": timeline["allocated_cores_avg"],
        "executor_run_time_ms": executor_run,
        "executor_cpu_time_ms": executor_cpu,
        "compute_window_start_time": start,
        "compute_window_end_time": end,
        "compute_window_duration_ms": app_duration,
        "task_cpu_efficiency_pct": _ratio(executor_cpu, executor_run, 100),
        "allocated_cpu_utilization_pct": _ratio(executor_cpu, timeline["executor_core_ms_capacity"], 100),
        "task_slot_utilization_pct": _ratio(executor_run, timeline["task_slot_capacity_ms"], 100),
        "wasted_core_seconds": wasted_core_ms / 1000 if wasted_core_ms is not None else None,
        "cpu_slot_identity_applicable": cpu_slot_identity_applicable,
        "cpu_slot_identity_delta_pct_points": (
            abs(
                _ratio(executor_cpu, timeline["executor_core_ms_capacity"], 100)
                - (
                    _ratio(executor_run, timeline["task_slot_capacity_ms"], 100)
                    * _ratio(executor_cpu, executor_run, 100)
                    / 100
                )
            )
            if cpu_slot_identity_applicable
            and _ratio(executor_cpu, timeline["executor_core_ms_capacity"], 100) is not None
            and _ratio(executor_run, timeline["task_slot_capacity_ms"], 100) is not None
            and _ratio(executor_cpu, executor_run, 100) is not None
            else None
        ),
        "peak_running_tasks": peak_running,
        "average_running_tasks": average_running,
        "peak_available_slots": timeline["peak_available_slots"],
        "average_available_slots": timeline["average_available_slots"],
        "gc_time_ms": gc_time,
        "gc_ratio_pct": _ratio(gc_time, executor_run, 100),
        "memory_spill_bytes": _sum(actual, "memory_bytes_spilled"),
        "disk_spill_bytes": _sum(actual, "disk_bytes_spilled"),
        "tasks_with_spill_pct": _ratio(tasks_with_spill, len(actual), 100),
        "disk_spill_to_processed_bytes": _ratio(
            _sum(actual, "disk_bytes_spilled"), (input_bytes or 0) + (shuffle_read or 0)
        ),
        "peak_execution_memory_p95": peak_memory["p95"],
        "peak_execution_memory_max": peak_memory["max"],
        "input_bytes": input_bytes,
        "input_records": _sum(logical, "input_records"),
        "output_bytes": output_bytes,
        "output_records": _sum(logical, "output_records"),
        "shuffle_read_bytes": shuffle_read,
        "shuffle_write_bytes": shuffle_write,
        "total_shuffle_io_bytes": total_shuffle,
        "shuffle_to_source_input_ratio": _ratio(total_shuffle, input_bytes),
        "shuffle_io_share_pct": _ratio(total_shuffle, io_denominator, 100),
        "shuffle_fetch_wait_ms": fetch_wait,
        "shuffle_fetch_wait_pct": _ratio(fetch_wait, executor_run, 100),
        "remote_shuffle_share_pct": _ratio(shuffle_remote, (shuffle_remote or 0) + (shuffle_local or 0), 100),
        "worst_duration_skew_factor": worst_duration,
        "worst_duration_max_factor": worst_duration_max,
        "worst_skewed_tasks_pct": worst_skewed_tasks_pct,
        "worst_input_skew_factor": worst_input,
        "worst_shuffle_skew_factor": worst_shuffle,
        "worst_stage_id": worst_stage["stage_id"] if worst_stage else None,
        "scan_bytes": sum((scan.get("scan_bytes") or 0) for scan in sql["scans"]) if sql["scans"] else None,
        "scan_rows": sum((scan.get("scan_output_rows") or 0) for scan in sql["scans"]) if sql["scans"] else None,
        "scan_files_read": (
            scan_result_files
            if scan_result_files is not None
            else sum((scan.get("files_read") or 0) for scan in sql["scans"])
            if sql["scans"]
            else None
        ),
        "scan_files_skipped": scan_skipped_files,
        "scan_files_considered": scan_considered_files,
        "scan_file_pruning_pct": (
            scan_skipped_files / scan_considered_files * 100 if scan_considered_files else None
        ),
        "sort_merge_joins": sql["operator_counts"].get("SortMergeJoin", 0),
        "broadcast_joins": sql["operator_counts"].get("BroadcastHashJoin", 0),
        "shuffled_hash_joins": sql["operator_counts"].get("ShuffledHashJoin", 0),
        "broadcast_nested_loop_joins": sql["operator_counts"].get("BroadcastNestedLoopJoin", 0),
        "cartesian_products": sql["operator_counts"].get("CartesianProduct", 0),
        "max_join_amplification": max((join.get("join_row_amplification") or 0 for join in sql["joins"]), default=0) or None,
        "window_count": sql["operator_counts"].get("Window", 0),
        "heavy_window_count": 0,
        "largest_window_input_rows": max((item.get("window_input_rows") or 0 for item in sql["windows"]), default=0) or None,
        "largest_window_upstream_shuffle_bytes": max((item.get("upstream_shuffle_bytes") or 0 for item in sql["windows"]), default=0) or None,
        "largest_sort_input_rows": max((item.get("input_rows") or 0 for item in sql["sorts"]), default=0) or None,
        "largest_exchange_bytes": max((item.get("shuffle_bytes_written") or 0 for item in sql["exchanges"]), default=0) or None,
        "single_partition_exchange_count": sum(bool(item.get("single_partition")) for item in sql["exchanges"]),
        "max_row_expansion": max((item.get("row_amplification") or 0 for item in sql["row_expansion_operators"]), default=0) or None,
    }

    coverage = {
        "task_metrics_available": any(task.metrics.get("executor_run_time_ms") is not None for task in actual),
        "executor_cpu_time_available": any(task.metrics.get("executor_cpu_time_ms") is not None for task in actual),
        "sql_events_available": bool(model.sql_executions),
        "sql_plan_available": any(execution.get("plan_nodes") for execution in sql["executions"]),
        "sql_metrics_available": sql["sql_metrics_available"],
        "aqe_events_available": any(execution.aqe_updated for execution in model.sql_executions.values()),
        "executor_memory_metrics_available": model.executor_memory_metrics_available,
        "application_completed": model.completed,
        "log_complete": model.log_complete,
    }
    application = {
        "application_id": model.application_id,
        "application_attempt_id": model.application_attempt_id,
        "application_name": model.application_name,
        "spark_version": model.spark_version,
        "spark_user": model.spark_user,
        "start_time": start,
        "end_time": end,
        "duration_ms": app_duration,
        "status": status,
        "exit_code": None,
        "number_of_jobs": len(model.jobs),
        "successful_jobs": successful_jobs,
        "failed_jobs": failed_jobs,
        "number_of_stages": len({stage_id for stage_id, _ in model.stages}),
        "number_of_stage_attempts": len(model.stages),
        "number_of_tasks": len(actual),
        "successful_tasks": sum(task.successful for task in actual),
        "failed_tasks": sum(task.failed for task in actual),
        "killed_tasks": sum(task.killed for task in actual),
        "speculative_tasks": sum(task.speculative for task in actual),
        "sql_execution_count": len(model.sql_executions),
        "sql_available": bool(model.sql_executions),
        "aqe_detected": any(execution.aqe_updated for execution in model.sql_executions.values()),
        "structured_streaming_detected": model.structured_streaming_detected,
        "analysis_status": "COMPLETE" if model.log_complete else "PARTIAL",
    }
    resources = {
        **{key: value for key, value in timeline.items() if key != "intervals"},
        "resource_profiles": [
            {
                "resource_profile_id": profile.profile_id,
                "executor_resources": profile.executor_resources,
                "task_resources": profile.task_resources,
                "executor_cores": profile.executor_cores,
                "task_cpus": profile.task_cpus,
            }
            for _, profile in sorted(model.resource_profiles.items())
        ],
        "executors": executor_rows,
        "executor_intervals": timeline["intervals"],
    }
    return {
        "application": application,
        "coverage": coverage,
        "configuration": config,
        "spark_properties": sanitized_properties(model.properties),
        "resources": resources,
        "summary_metrics": summary,
        "stages": stages,
        "top_stages": _top_stages(stages),
        "sql_executions": sql["executions"],
        "sql_facts": {
            key: sql[key]
            for key in (
                "joins",
                "scans",
                "windows",
                "exchanges",
                "sorts",
                "row_expansion_operators",
                "operator_counts",
            )
        },
        "source": {
            "event_count": model.event_count,
            "unknown_event_count": model.unknown_event_count,
            "issues": [
                {"source": issue.source, "line_number": issue.line_number, "message": issue.message}
                for issue in model.source_issues
            ],
        },
        "_normalized": {
            "jobs": [
                {
                    "job_id": job.job_id,
                    "submission_time": job.submission_time,
                    "completion_time": job.completion_time,
                    "stage_ids": job.stage_ids,
                    "status": job.status,
                    "sql_execution_id": job.properties.get("spark.sql.execution.id"),
                }
                for _, job in sorted(model.jobs.items())
            ],
            "tasks": [
                {
                    "task_id": task.task_id,
                    "stage_id": task.stage_id,
                    "stage_attempt_id": task.stage_attempt_id,
                    "partition_id": task.partition_id,
                    "attempt": task.attempt,
                    "executor_id": task.executor_id,
                    "launch_time": task.launch_time,
                    "finish_time": task.finish_time,
                    "successful": task.successful,
                    "failed": task.failed,
                    "killed": task.killed,
                    "speculative": task.speculative,
                    "metrics": task.metrics,
                }
                for task in actual
            ],
        },
    }
