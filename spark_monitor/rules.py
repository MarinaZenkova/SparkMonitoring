from __future__ import annotations

from typing import Any


SEVERITY_RANK = {"NOT_APPLICABLE": -2, "NOT_AVAILABLE": -1, "OK": 0, "WARNING": 1, "CRITICAL": 2}


def _severity_high(value: float | None, warning: float, critical: float) -> str | None:
    if value is None:
        return None
    if value >= critical:
        return "CRITICAL"
    if value >= warning:
        return "WARNING"
    return None


def _severity_low(value: float | None, warning: float, critical: float) -> str | None:
    if value is None:
        return None
    if value <= critical:
        return "CRITICAL"
    if value <= warning:
        return "WARNING"
    return None


def _diagnostic(
    code: str,
    severity: str,
    metric_values: dict[str, Any],
    thresholds: dict[str, Any],
    *,
    stages: list[int] | None = None,
    executions: list[int] | None = None,
    plan_node_id: int | None = None,
    evidence: list[str] | None = None,
    kind: str = "DIAGNOSTIC",
) -> dict[str, Any]:
    result = {
        "problem_code": code,
        "severity": severity,
        "metric_values": metric_values,
        "thresholds_used": thresholds,
        "affected_stages": sorted(set(stages or [])),
        "affected_sql_executions": sorted(set(executions or [])),
        "evidence": evidence or [],
        "availability": "AVAILABLE",
        "kind": kind,
    }
    if plan_node_id is not None:
        result["plan_node_id"] = plan_node_id
    return result


def _block(metrics: dict[str, Any], problems: list[dict[str, Any]], empty_status: str = "OK") -> dict[str, Any]:
    status = max((problem["severity"] for problem in problems), key=SEVERITY_RANK.get, default=empty_status)
    return {"status": status, "metrics": metrics, "problems": problems}


def apply_rules(facts: dict[str, Any], rules: dict[str, Any]) -> dict[str, Any]:
    summary = facts["summary_metrics"]
    coverage = facts["coverage"]
    stages = facts["stages"]
    sql = facts["sql_facts"]

    compute_problems: list[dict[str, Any]] = []
    cpu_cfg = rules["cpu"]
    cpu = summary.get("task_cpu_efficiency_pct")
    cpu_severity = _severity_low(cpu, cpu_cfg["low_efficiency_warning_pct"], cpu_cfg["low_efficiency_critical_pct"])
    if cpu_severity:
        compute_problems.append(
            _diagnostic(
                "LOW_CPU_EFFICIENCY",
                cpu_severity,
                {"task_cpu_efficiency_pct": cpu},
                cpu_cfg,
                evidence=["executor_cpu_time_ms / executor_run_time_ms × 100"],
            )
        )
    util_cfg = rules["utilization"]
    slot_util = summary.get("task_slot_utilization_pct")
    slot_severity = _severity_low(slot_util, util_cfg["low_slot_warning_pct"], util_cfg["low_slot_critical_pct"])
    if (
        slot_severity == "CRITICAL"
        and (summary.get("duration_ms") or 0) < util_cfg["min_duration_for_critical_ms"]
        and (summary.get("wasted_core_seconds") or 0) < util_cfg["wasted_core_seconds_critical"]
    ):
        slot_severity = "WARNING"
    if slot_severity:
        compute_problems.append(
            _diagnostic(
                "RESOURCE_UNDERUTILIZATION",
                slot_severity,
                {
                    "task_slot_utilization_pct": slot_util,
                    "average_running_tasks": summary.get("average_running_tasks"),
                    "average_available_slots": summary.get("average_available_slots"),
                    "wasted_core_seconds": summary.get("wasted_core_seconds"),
                    "application_duration_ms": summary.get("duration_ms"),
                },
                util_cfg,
                evidence=[
                    "executor_run_time_ms / task_slot_capacity_ms × 100",
                    "CRITICAL requires configured duration or wasted-core impact",
                ],
            )
        )
    parallel_cfg = rules["parallelism"]
    peak_running = summary.get("peak_running_tasks")
    peak_slots = summary.get("peak_available_slots")
    peak_ratio = peak_running / peak_slots if peak_running is not None and peak_slots else None
    parallel_severity = _severity_low(
        peak_ratio,
        parallel_cfg["peak_slot_ratio_warning"],
        parallel_cfg["peak_slot_ratio_critical"],
    )
    if parallel_severity:
        compute_problems.append(
            _diagnostic(
                "INSUFFICIENT_PARALLELISM",
                parallel_severity,
                {"peak_running_tasks": peak_running, "peak_available_slots": peak_slots, "peak_slot_ratio": peak_ratio},
                parallel_cfg,
                evidence=["peak_running_tasks / peak_available_slots"],
            )
        )
    compute_available = coverage["task_metrics_available"] or facts["resources"].get("task_slot_capacity_ms") is not None
    compute = _block(
        {
            key: summary.get(key)
            for key in (
                "task_cpu_efficiency_pct",
                "allocated_cpu_utilization_pct",
                "task_slot_utilization_pct",
                "peak_running_tasks",
                "average_running_tasks",
                "peak_available_slots",
                "average_available_slots",
                "wasted_core_seconds",
                "cpu_slot_identity_delta_pct_points",
            )
        },
        compute_problems,
        "OK" if compute_available else "NOT_AVAILABLE",
    )

    memory_problems: list[dict[str, Any]] = []
    memory_cfg = rules["memory"]
    gc_ratio = summary.get("gc_ratio_pct")
    gc_severity = _severity_high(gc_ratio, memory_cfg["gc_warning_pct"], memory_cfg["gc_critical_pct"])
    if gc_severity:
        memory_problems.append(
            _diagnostic(
                "HIGH_GC",
                gc_severity,
                {"gc_ratio_pct": gc_ratio, "gc_time_ms": summary.get("gc_time_ms")},
                {key: memory_cfg[key] for key in ("gc_warning_pct", "gc_critical_pct")},
                evidence=["jvm_gc_time_ms / executor_run_time_ms × 100"],
            )
        )
    spill = max(summary.get("memory_spill_bytes") or 0, summary.get("disk_spill_bytes") or 0)
    spill_severity = _severity_high(spill, memory_cfg["spill_warning_bytes"], memory_cfg["spill_critical_bytes"])
    if spill_severity:
        memory_problems.append(
            _diagnostic(
                "HEAVY_SPILL",
                spill_severity,
                {
                    "memory_spill_bytes": summary.get("memory_spill_bytes"),
                    "disk_spill_bytes": summary.get("disk_spill_bytes"),
                    "tasks_with_spill_pct": summary.get("tasks_with_spill_pct"),
                },
                {key: memory_cfg[key] for key in ("spill_warning_bytes", "spill_critical_bytes")},
                evidence=["Memory and disk spill are evaluated independently; maximum controls severity"],
            )
        )
    memory = _block(
        {
            key: summary.get(key)
            for key in (
                "gc_time_ms",
                "gc_ratio_pct",
                "memory_spill_bytes",
                "disk_spill_bytes",
                "tasks_with_spill_pct",
                "peak_execution_memory_p95",
                "peak_execution_memory_max",
            )
        },
        memory_problems,
        "OK" if coverage["task_metrics_available"] else "NOT_AVAILABLE",
    )

    skew_problems: list[dict[str, Any]] = []
    skew_cfg = rules["skew"]
    analyzed_stages = 0
    for stage in stages:
        if stage.get("task_count", 0) < skew_cfg["min_tasks"]:
            continue
        duration_factor = stage.get("duration_skew_p95")
        duration_max = stage.get("duration_skew_max")
        data_factors = [
            stage.get("input_skew_p95"),
            stage.get("shuffle_read_skew_p95"),
            stage.get("shuffle_write_skew_p95"),
        ]
        if duration_factor is None:
            continue
        analyzed_stages += 1
        duration_severity = _severity_high(
            duration_factor,
            skew_cfg["p95_median_warning"],
            skew_cfg["p95_median_critical"],
        )
        if duration_max is not None and duration_max >= skew_cfg["max_median_critical"]:
            duration_severity = "CRITICAL"
        if not duration_severity:
            continue
        available_data = [value for value in data_factors if value is not None]
        data_high = any(value >= skew_cfg["p95_median_warning"] for value in available_data)
        code = "DATA_SKEW" if data_high else "TASK_STRAGGLERS"
        skew_problems.append(
            _diagnostic(
                code,
                duration_severity,
                {
                    "duration_p50_ms": stage["task_duration"]["p50"],
                    "duration_p95_ms": stage["task_duration"]["p95"],
                    "duration_skew_factor": duration_factor,
                    "duration_max_factor": duration_max,
                    "skewed_tasks_pct": stage.get("skewed_tasks_pct"),
                    "skewed_task_multiplier": stage.get("skewed_task_multiplier"),
                    "input_skew_factor": stage.get("input_skew_p95"),
                    "shuffle_read_skew_factor": stage.get("shuffle_read_skew_p95"),
                    "shuffle_write_skew_factor": stage.get("shuffle_write_skew_p95"),
                },
                skew_cfg,
                stages=[stage["stage_id"]],
                evidence=[
                    "Duration p95 / median exceeds threshold",
                    "Data-volume skew also exceeds threshold" if data_high else "Available data-volume distributions remain below threshold",
                ],
            )
        )
    skew = _block(
        {
            "analyzed_stage_count": analyzed_stages,
            "worst_stage_id": summary.get("worst_stage_id"),
            "worst_duration_skew_factor": summary.get("worst_duration_skew_factor"),
            "worst_duration_max_factor": summary.get("worst_duration_max_factor"),
            "worst_skewed_tasks_pct": summary.get("worst_skewed_tasks_pct"),
            "worst_input_skew_factor": summary.get("worst_input_skew_factor"),
            "worst_shuffle_skew_factor": summary.get("worst_shuffle_skew_factor"),
        },
        skew_problems,
        "OK" if analyzed_stages else "NOT_AVAILABLE",
    )

    shuffle_problems: list[dict[str, Any]] = []
    shuffle_cfg = rules["shuffle"]
    fetch_wait_pct = summary.get("shuffle_fetch_wait_pct")
    fetch_severity = _severity_high(
        fetch_wait_pct,
        shuffle_cfg["fetch_wait_warning_pct"],
        shuffle_cfg["fetch_wait_critical_pct"],
    )
    if fetch_severity:
        shuffle_problems.append(
            _diagnostic(
                "SHUFFLE_FETCH_WAIT",
                fetch_severity,
                {"shuffle_fetch_wait_pct": fetch_wait_pct, "shuffle_fetch_wait_ms": summary.get("shuffle_fetch_wait_ms")},
                {key: shuffle_cfg[key] for key in ("fetch_wait_warning_pct", "fetch_wait_critical_pct")},
                evidence=["shuffle_fetch_wait_ms / executor_run_time_ms × 100"],
            )
        )
    shuffle_ratio = summary.get("shuffle_to_source_input_ratio")
    shuffle_severity = _severity_high(
        shuffle_ratio,
        shuffle_cfg["io_to_input_warning_ratio"],
        shuffle_cfg["io_to_input_critical_ratio"],
    )
    if shuffle_severity:
        shuffle_problems.append(
            _diagnostic(
                "HEAVY_SHUFFLE",
                shuffle_severity,
                {
                    "shuffle_to_source_input_ratio": shuffle_ratio,
                    "shuffle_read_bytes": summary.get("shuffle_read_bytes"),
                    "shuffle_write_bytes": summary.get("shuffle_write_bytes"),
                },
                {key: shuffle_cfg[key] for key in ("io_to_input_warning_ratio", "io_to_input_critical_ratio")},
                evidence=["(shuffle read + shuffle write) / source input"],
            )
        )
    shuffle = _block(
        {
            key: summary.get(key)
            for key in (
                "shuffle_read_bytes",
                "shuffle_write_bytes",
                "total_shuffle_io_bytes",
                "shuffle_to_source_input_ratio",
                "shuffle_io_share_pct",
                "shuffle_fetch_wait_ms",
                "shuffle_fetch_wait_pct",
                "remote_shuffle_share_pct",
            )
        },
        shuffle_problems,
        "OK" if summary.get("shuffle_read_bytes") is not None else "NOT_AVAILABLE",
    )

    scan_problems: list[dict[str, Any]] = []
    scan_cfg = rules["scan"]
    unpruned_scan_items = [
        scan_item
        for scan_item in sql["scans"]
        if scan_item.get("partition_pruning_status") in {
            "NO_PARTITION_FILTER",
            "NO_PRUNING_DETECTED",
        }
    ]
    if unpruned_scan_items:
        largest_unpruned_scan = max(unpruned_scan_items, key=lambda item: item.get("scan_bytes") or 0)
        large_unpruned_items = [
            item for item in unpruned_scan_items if (item.get("scan_bytes") or 0) >= scan_cfg["large_scan_bytes"]
        ]
        scan_problems.append(
            _diagnostic(
                "LARGE_SCAN_WITHOUT_PARTITION_PRUNING" if large_unpruned_items else "NO_PARTITION_PRUNING",
                "CRITICAL" if large_unpruned_items else "WARNING",
                {
                    "unpruned_scan_count": len(unpruned_scan_items),
                    "large_unpruned_scan_count": len(large_unpruned_items),
                    "unpruned_scan_bytes": sum((item.get("scan_bytes") or 0) for item in unpruned_scan_items),
                    "largest_unpruned_scan_bytes": largest_unpruned_scan.get("scan_bytes"),
                },
                scan_cfg,
                stages=sorted({stage for item in unpruned_scan_items for stage in (item.get("stage_ids") or [])}),
                executions=sorted({item["execution_id"] for item in unpruned_scan_items}),
                plan_node_id=largest_unpruned_scan["plan_node_id"],
                evidence=[
                    f"{len(unpruned_scan_items)} scan(s) reported zero skipped data files",
                    "Per-source evidence is available in Scan details",
                ],
            )
        )
    pruning_detected = sum(item.get("partition_pruning_status") == "PRUNING_DETECTED" for item in sql["scans"])
    source_not_partitioned = sum(item.get("partition_pruning_status") == "SOURCE_NOT_PARTITIONED" for item in sql["scans"])
    unknown_pruning = sum(item.get("partition_pruning_status") == "UNKNOWN" for item in sql["scans"])
    scans_without_filter = sum(
        item.get("partition_pruning_status") in {"NO_PARTITION_FILTER", "NO_PRUNING_DETECTED"}
        for item in sql["scans"]
    )
    large_without_filter = sum(
        item.get("partition_pruning_status") in {"NO_PARTITION_FILTER", "NO_PRUNING_DETECTED"}
        and (item.get("scan_bytes") or 0) >= scan_cfg["large_scan_bytes"]
        for item in sql["scans"]
    )
    skipped_data_files_values = [
        item["skipped_data_files"] for item in sql["scans"] if item.get("skipped_data_files") is not None
    ]
    result_data_files_values = [
        item["result_data_files"] for item in sql["scans"] if item.get("result_data_files") is not None
    ]
    skipped_data_files = sum(skipped_data_files_values) if skipped_data_files_values else None
    result_data_files = sum(result_data_files_values) if result_data_files_values else None
    considered_data_files = (
        skipped_data_files + result_data_files
        if skipped_data_files is not None and result_data_files is not None
        else None
    )
    file_pruning_pct = (
        skipped_data_files / considered_data_files * 100 if considered_data_files else None
    )
    if not coverage["sql_plan_available"] or not sql["scans"]:
        scan_empty_status = "NOT_APPLICABLE"
    elif source_not_partitioned == len(sql["scans"]):
        scan_empty_status = "NOT_APPLICABLE"
    elif unknown_pruning > 0:
        scan_empty_status = "NOT_AVAILABLE"
    else:
        scan_empty_status = "OK"
    scan = _block(
        {
            "scan_count": len(sql["scans"]),
            "scan_bytes": summary.get("scan_bytes"),
            "scan_rows": summary.get("scan_rows"),
            "files_read": summary.get("scan_files_read"),
            "partition_filters_present": sum(bool(item.get("partition_filters_present")) for item in sql["scans"]),
            "pushed_filters_present": sum(bool(item.get("pushed_filters_present")) for item in sql["scans"]),
            "data_filters_present": sum(bool(item.get("data_filters_present")) for item in sql["scans"]),
            "scan_filters_present": sum(bool(item.get("scan_filters_present")) for item in sql["scans"]),
            "dynamic_partition_pruning_detected": sum(bool(item.get("dynamic_partition_pruning_detected")) for item in sql["scans"]),
            "pruning_detected_scans": pruning_detected,
            "confirmed_unpruned_scans": scans_without_filter,
            "source_not_partitioned_scans": source_not_partitioned,
            "unknown_pruning_scans": unknown_pruning,
            "result_data_files": result_data_files,
            "skipped_data_files": skipped_data_files,
            "considered_data_files": considered_data_files,
            "file_pruning_pct": file_pruning_pct,
            "scans_without_partition_filter": scans_without_filter,
            "large_scans_without_partition_filter": large_without_filter,
            "scans": sql["scans"],
        },
        scan_problems,
        scan_empty_status,
    )

    join_problems: list[dict[str, Any]] = []
    join_cfg = rules["join"]
    for join in sql["joins"]:
        amplification = join.get("join_row_amplification")
        severity = _severity_high(amplification, join_cfg["amplification_warning"], join_cfg["amplification_critical"])
        if severity:
            join_problems.append(
                _diagnostic(
                    "JOIN_ROW_EXPLOSION",
                    severity,
                    {
                        "join_strategy": join["join_strategy"],
                        "join_type": join["join_type"],
                        "left_rows": join["left_rows"],
                        "right_rows": join["right_rows"],
                        "join_output_rows": join["join_output_rows"],
                        "join_row_amplification": amplification,
                    },
                    join_cfg,
                    stages=join.get("stage_ids"),
                    executions=[join["execution_id"]],
                    plan_node_id=join["plan_node_id"],
                    evidence=["join output rows / max(left input rows, right input rows)"],
                )
            )
        if join.get("potential_broadcast_candidate"):
            join_problems.append(
                _diagnostic(
                    "POTENTIAL_BROADCAST_CANDIDATE",
                    "WARNING",
                    {
                        "join_strategy": join["join_strategy"],
                        "left_bytes": join["left_bytes_if_available"],
                        "right_bytes": join["right_bytes_if_available"],
                        "effective_auto_broadcast_join_threshold_bytes": join[
                            "effective_auto_broadcast_join_threshold_bytes"
                        ],
                    },
                    {"effective_auto_broadcast_join_threshold_bytes": join["effective_auto_broadcast_join_threshold_bytes"]},
                    stages=join.get("stage_ids"),
                    executions=[join["execution_id"]],
                    plan_node_id=join["plan_node_id"],
                    evidence=["A known join side is below the effective broadcast threshold"],
                    kind="POTENTIAL_OPTIMIZATION",
                )
            )
    join = _block(
        {
            "join_count": len(sql["joins"]),
            "sort_merge_joins": summary.get("sort_merge_joins"),
            "broadcast_joins": summary.get("broadcast_joins"),
            "shuffled_hash_joins": summary.get("shuffled_hash_joins"),
            "broadcast_nested_loop_joins": summary.get("broadcast_nested_loop_joins"),
            "cartesian_products": summary.get("cartesian_products"),
            "max_join_amplification": summary.get("max_join_amplification"),
            "joins": sql["joins"],
        },
        join_problems,
        "NOT_APPLICABLE" if not coverage["sql_plan_available"] or not sql["joins"] else "OK",
    )

    operator_problems: list[dict[str, Any]] = []
    window_cfg = rules["window"]
    operator_cfg = rules["sql_operators"]
    heavy_window_count = 0
    for window in sql["windows"]:
        rows = window.get("window_input_rows")
        if rows is not None and rows >= window_cfg["heavy_window_rows"]:
            heavy_window_count += 1
            operator_problems.append(
                _diagnostic(
                    "HEAVY_WINDOW_AFTER_SHUFFLE" if window.get("requires_shuffle_and_sort") else "HEAVY_WINDOW",
                    "WARNING",
                    {
                        "window_input_rows": rows,
                        "window_output_rows": window.get("window_output_rows"),
                        "upstream_shuffle_bytes": window.get("upstream_shuffle_bytes"),
                        "requires_shuffle_and_sort": window.get("requires_shuffle_and_sort"),
                    },
                    window_cfg,
                    stages=window.get("stage_ids"),
                    executions=[window["execution_id"]],
                    plan_node_id=window["plan_node_id"],
                    evidence=["Window input rows exceed configured threshold"],
                )
            )
    for sort_item in sql["sorts"]:
        rows = sort_item.get("input_rows")
        if rows is not None and rows >= operator_cfg["heavy_sort_rows"]:
            operator_problems.append(
                _diagnostic(
                    "HEAVY_SORT",
                    "WARNING",
                    {"sort_input_rows": rows, "spill_bytes": sort_item.get("spill_bytes")},
                    {"heavy_sort_rows": operator_cfg["heavy_sort_rows"]},
                    stages=sort_item.get("stage_ids"),
                    executions=[sort_item["execution_id"]],
                    plan_node_id=sort_item["plan_node_id"],
                    evidence=["Sort input rows exceed configured threshold"],
                )
            )
    for exchange in sql["exchanges"]:
        if exchange.get("single_partition") and (exchange.get("shuffle_bytes_written") or 0) >= window_cfg["heavy_window_shuffle_bytes"]:
            operator_problems.append(
                _diagnostic(
                    "SINGLE_PARTITION_BOTTLENECK",
                    "CRITICAL",
                    {"shuffle_bytes_written": exchange.get("shuffle_bytes_written"), "single_partition": True},
                    {"heavy_window_shuffle_bytes": window_cfg["heavy_window_shuffle_bytes"]},
                    stages=exchange.get("stage_ids"),
                    executions=[exchange["execution_id"]],
                    plan_node_id=exchange["plan_node_id"],
                    evidence=["Large Exchange uses SinglePartition"],
                )
            )
    for row_operator in sql["row_expansion_operators"]:
        amplification = row_operator.get("row_amplification")
        severity = _severity_high(
            amplification,
            operator_cfg["row_amplification_warning"],
            operator_cfg["row_amplification_critical"],
        )
        if severity:
            operator_problems.append(
                _diagnostic(
                    "ROW_EXPANSION_OPERATOR",
                    severity,
                    {
                        "operator_name": row_operator.get("operator_name"),
                        "input_rows": row_operator.get("input_rows"),
                        "output_rows": row_operator.get("output_rows"),
                        "row_amplification": amplification,
                    },
                    operator_cfg,
                    stages=row_operator.get("stage_ids"),
                    executions=[row_operator["execution_id"]],
                    plan_node_id=row_operator["plan_node_id"],
                    evidence=["operator output rows / child output rows"],
                )
            )
    summary["heavy_window_count"] = heavy_window_count
    operators = _block(
        {
            "operator_counts": sql["operator_counts"],
            "window_count": len(sql["windows"]),
            "heavy_window_count": heavy_window_count,
            "sort_count": sql["operator_counts"].get("Sort", 0),
            "exchange_count": len(sql["exchanges"]),
            "largest_window_input_rows": summary.get("largest_window_input_rows"),
            "largest_window_upstream_shuffle_bytes": summary.get("largest_window_upstream_shuffle_bytes"),
            "largest_sort_input_rows": summary.get("largest_sort_input_rows"),
            "largest_exchange_bytes": summary.get("largest_exchange_bytes"),
            "single_partition_exchange_count": summary.get("single_partition_exchange_count"),
            "max_row_expansion": summary.get("max_row_expansion"),
            "windows": sql["windows"],
            "sorts": sql["sorts"],
            "exchanges": sql["exchanges"],
            "row_expansion_operators": sql["row_expansion_operators"],
        },
        operator_problems,
        "NOT_APPLICABLE" if not coverage["sql_plan_available"] else "OK",
    )

    diagnostics = {
        "compute": compute,
        "memory": memory,
        "skew": skew,
        "shuffle": shuffle,
        "scan": scan,
        "joins": join,
        "sql_operators": operators,
    }
    all_problems = [problem for block in diagnostics.values() for problem in block["problems"]]
    primary = max(
        all_problems,
        key=lambda problem: (SEVERITY_RANK[problem["severity"]], problem["problem_code"]),
        default=None,
    )
    return {
        "diagnostics": diagnostics,
        "primary_evidence": primary["problem_code"] if primary else None,
        "problem_count": len(all_problems),
    }
