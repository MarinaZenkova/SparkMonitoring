from __future__ import annotations

import json
import re
from collections import deque
from typing import Any

from .config import parse_bytes
from .model import NormalizedApplication, SQLExecution, TaskAttempt


JOIN_NAMES = {
    "BroadcastHashJoin",
    "SortMergeJoin",
    "ShuffledHashJoin",
    "BroadcastNestedLoopJoin",
    "CartesianProduct",
}
SCAN_ALIASES = ("filescan", "filesourcescan", "batchscan", "columnarbatchescan", "scan")
EXPENSIVE_NAMES = {
    "Window",
    "Sort",
    "Exchange",
    "HashAggregate",
    "SortAggregate",
    "Expand",
    "Generate",
    "CartesianProduct",
    "BroadcastNestedLoopJoin",
}


def _as_number(value: Any) -> float | None:
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return None


def _task_accumulators(tasks: list[TaskAttempt]) -> dict[int, float]:
    values: dict[int, float] = {}
    for task in tasks:
        for accumulator_id, value in task.accum_updates.items():
            values[accumulator_id] = values.get(accumulator_id, 0.0) + value
    return values


def _metric(metrics: list[dict[str, Any]], *names: str) -> float | None:
    wanted = {name.casefold() for name in names}
    for metric in metrics:
        if str(metric.get("name", "")).casefold() in wanted:
            return _as_number(metric.get("value"))
    return None


def _flatten_plan(plan: dict[str, Any] | None, values: dict[int, float], execution_id: int) -> list[dict[str, Any]]:
    if not plan:
        return []
    nodes: list[dict[str, Any]] = []

    def visit(raw: dict[str, Any], parent_id: int | None) -> None:
        node_id = len(nodes)
        metrics: list[dict[str, Any]] = []
        for metric in raw.get("metrics", raw.get("Metrics", [])) or []:
            accumulator_id = metric.get("accumulatorId", metric.get("Accumulator ID"))
            accumulator_id = int(accumulator_id) if accumulator_id is not None else None
            metrics.append(
                {
                    "name": metric.get("name", metric.get("Name")),
                    "accumulator_id": accumulator_id,
                    "metric_type": metric.get("metricType", metric.get("Metric Type")),
                    "value": values.get(accumulator_id) if accumulator_id is not None else None,
                }
            )
        node = {
            "node_id": node_id,
            "parent_node_id": parent_id,
            "operator_name": raw.get("nodeName", raw.get("Node Name", "Unknown")),
            "operator_description": raw.get("simpleString", raw.get("Simple String", "")),
            "metadata": raw.get("metadata", raw.get("Metadata", {})) or {},
            "metrics": metrics,
            "children": [],
            "execution_id": execution_id,
        }
        nodes.append(node)
        for child in raw.get("children", raw.get("Children", [])) or []:
            child_id = len(nodes)
            node["children"].append(child_id)
            visit(child, node_id)

    visit(plan, None)
    return nodes


def _nearest_metric(nodes_by_id: dict[int, dict[str, Any]], start_id: int, names: tuple[str, ...]) -> float | None:
    queue: deque[int] = deque([start_id])
    seen: set[int] = set()
    while queue:
        node_id = queue.popleft()
        if node_id in seen:
            continue
        seen.add(node_id)
        node = nodes_by_id[node_id]
        value = _metric(node["metrics"], *names)
        if value is not None:
            return value
        queue.extend(node["children"])
    return None


def _nearest_node(
    nodes_by_id: dict[int, dict[str, Any]], start_id: int, operator_names: tuple[str, ...]
) -> dict[str, Any] | None:
    wanted = tuple(name.casefold() for name in operator_names)
    queue: deque[int] = deque([start_id])
    seen: set[int] = set()
    while queue:
        node_id = queue.popleft()
        if node_id in seen:
            continue
        seen.add(node_id)
        node = nodes_by_id[node_id]
        name = str(node["operator_name"]).casefold()
        if any(candidate in name for candidate in wanted):
            return node
        queue.extend(node["children"])
    return None


def _relation(node: dict[str, Any]) -> str | None:
    name = str(node["operator_name"])
    match = re.search(r"(?:BatchScan|FileScan|FileSourceScan|ColumnarBatchScan|Scan)\s+([^\[\s]+)", name)
    return match.group(1) if match else None


def _filters(description: str) -> tuple[str | None, str | None, str | None, str | None]:
    def extract(pattern: str) -> str | None:
        match = re.search(pattern, description, flags=re.IGNORECASE)
        return match.group(1).strip() if match else None

    partition_filters = extract(r"PartitionFilters\s*:\s*\[([^\]]*)\]")
    data_filters = extract(r"DataFilters\s*:\s*\[([^\]]*)\]")
    pushed_filters = extract(r"PushedFilters\s*:\s*\[([^\]]*)\]")
    scan_filters = extract(r"\[filters=(.*?),\s*groupedBy=")
    if data_filters is None:
        data_filters = scan_filters
    return partition_filters, data_filters, pushed_filters, scan_filters


def _pruning_status(
    partition_filters: str | None,
    dynamic_partition_pruning: bool,
    partitioned_source: bool | None,
    skipped_data_files: float | None,
    result_data_files: float | None,
) -> tuple[str, list[str]]:
    evidence: list[str] = []
    if dynamic_partition_pruning:
        evidence.append("dynamic_partition_pruning")
    if partition_filters:
        evidence.append("partition_filter_expression")
    if skipped_data_files is not None and skipped_data_files > 0:
        evidence.append("source_skipped_data_files")
    if evidence:
        return "PRUNING_DETECTED", evidence
    if skipped_data_files == 0 and result_data_files is not None and result_data_files > 0:
        return "NO_PRUNING_DETECTED", ["source_reported_zero_skipped_data_files"]
    if partitioned_source is True and partition_filters == "":
        return "NO_PARTITION_FILTER", ["partitioned_source_without_partition_filter"]
    if partitioned_source is False:
        return "SOURCE_NOT_PARTITIONED", ["source_not_partitioned"]
    return "UNKNOWN", []


def _join_type(description: str) -> str | None:
    known = (
        "Inner",
        "LeftOuter",
        "RightOuter",
        "FullOuter",
        "LeftSemi",
        "LeftAnti",
        "Cross",
    )
    for item in known:
        if re.search(rf"\b{item}\b", description):
            return item
    return None


def _execution_stage_ids(model: NormalizedApplication, execution: SQLExecution) -> list[int]:
    result: set[int] = set()
    for job_id in execution.job_ids:
        job = model.jobs.get(job_id)
        if job:
            result.update(job.stage_ids)
    return sorted(result)


def analyze_sql(
    model: NormalizedApplication,
    logical_tasks: list[TaskAttempt],
    configuration: dict[str, Any],
) -> dict[str, Any]:
    task_values = _task_accumulators(logical_tasks)
    executions: list[dict[str, Any]] = []
    all_joins: list[dict[str, Any]] = []
    all_scans: list[dict[str, Any]] = []
    all_windows: list[dict[str, Any]] = []
    all_exchanges: list[dict[str, Any]] = []
    all_sorts: list[dict[str, Any]] = []
    all_row_expansion_operators: list[dict[str, Any]] = []
    operator_counts: dict[str, int] = {name: 0 for name in sorted(EXPENSIVE_NAMES | JOIN_NAMES)}
    sql_metric_found = False

    threshold_raw = configuration.get("spark.sql.adaptive.autoBroadcastJoinThreshold", {}).get("effective_value")
    if threshold_raw is None:
        threshold_raw = configuration.get("spark.sql.autoBroadcastJoinThreshold", {}).get("effective_value")
    broadcast_threshold = parse_bytes(threshold_raw)

    for execution_id in sorted(model.sql_executions):
        execution = model.sql_executions[execution_id]
        values = dict(task_values)
        values.update(execution.driver_accum_updates)
        plan = execution.final_plan or execution.initial_plan
        nodes = _flatten_plan(plan, values, execution_id)
        by_id = {node["node_id"]: node for node in nodes}
        stage_ids = _execution_stage_ids(model, execution)
        joins: list[dict[str, Any]] = []
        scans: list[dict[str, Any]] = []
        windows: list[dict[str, Any]] = []
        exchanges: list[dict[str, Any]] = []
        sorts: list[dict[str, Any]] = []
        row_expansion_operators: list[dict[str, Any]] = []

        for node in nodes:
            name = str(node["operator_name"])
            folded = name.casefold()
            metrics = node["metrics"]
            if any(metric.get("value") is not None for metric in metrics):
                sql_metric_found = True
            canonical = next((candidate for candidate in JOIN_NAMES if candidate.casefold() in folded), None)
            if canonical:
                operator_counts[canonical] = operator_counts.get(canonical, 0) + 1
                children = node["children"]
                left_rows = _nearest_metric(by_id, children[0], ("number of output rows",)) if len(children) >= 1 else None
                right_rows = _nearest_metric(by_id, children[1], ("number of output rows",)) if len(children) >= 2 else None
                left_bytes = _nearest_metric(by_id, children[0], ("data size", "total data file size (bytes)")) if len(children) >= 1 else None
                right_bytes = _nearest_metric(by_id, children[1], ("data size", "total data file size (bytes)")) if len(children) >= 2 else None
                output_rows = _metric(metrics, "number of output rows")
                denominator = max(left_rows or 0, right_rows or 0)
                amplification = output_rows / denominator if output_rows is not None and denominator > 0 else None
                join = {
                    "execution_id": execution_id,
                    "plan_node_id": node["node_id"],
                    "stage_ids": stage_ids,
                    "join_type": _join_type(str(node["operator_description"])),
                    "join_strategy": canonical,
                    "left_rows": left_rows,
                    "right_rows": right_rows,
                    "left_bytes_if_available": left_bytes,
                    "right_bytes_if_available": right_bytes,
                    "join_output_rows": output_rows,
                    "join_row_amplification": amplification,
                    "effective_auto_broadcast_join_threshold_bytes": broadcast_threshold,
                    "aqe_enabled": execution.aqe_updated,
                    "potential_broadcast_candidate": bool(
                        canonical == "SortMergeJoin"
                        and broadcast_threshold is not None
                        and any(value is not None and value < broadcast_threshold for value in (left_bytes, right_bytes))
                    ),
                }
                joins.append(join)
                all_joins.append(join)

            is_scan = any(alias in folded for alias in SCAN_ALIASES)
            if is_scan:
                description = str(node["operator_description"])
                partition_filters, data_filters, pushed_filters, scan_filters = _filters(description)
                metadata = node.get("metadata") or {}
                partition_filters = partition_filters if partition_filters is not None else metadata.get("PartitionFilters")
                data_filters = data_filters if data_filters is not None else metadata.get("DataFilters")
                pushed_filters = pushed_filters if pushed_filters is not None else metadata.get("PushedFilters")
                scan_filters = scan_filters if scan_filters is not None else metadata.get("Filters")
                partition_filters = str(partition_filters).strip() if partition_filters is not None else None
                data_filters = str(data_filters).strip() if data_filters is not None else None
                pushed_filters = str(pushed_filters).strip() if pushed_filters is not None else None
                scan_filters = str(scan_filters).strip() if scan_filters is not None else None
                dynamic = "dynamicpruning" in description.casefold()
                partition_columns = metadata.get("PartitionColumns") or metadata.get("Partitioning")
                partitioned_flag = metadata.get("Partitioned")
                partitioned = True if partitioned_flag is True or bool(partition_columns) else (False if partitioned_flag is False else None)
                skipped_data_files = _metric(metrics, "number of skipped data files")
                result_data_files = _metric(metrics, "number of result data files")
                pruning_status, pruning_evidence = _pruning_status(
                    partition_filters,
                    dynamic,
                    partitioned,
                    skipped_data_files,
                    result_data_files,
                )
                considered_data_files = (
                    skipped_data_files + result_data_files
                    if skipped_data_files is not None and result_data_files is not None
                    else None
                )
                file_pruning_pct = (
                    skipped_data_files / considered_data_files * 100
                    if considered_data_files
                    else None
                )
                metric_types = " ".join(str(metric.get("metric_type") or "") for metric in metrics).casefold()
                description_folded = description.casefold()
                detected_format = next(
                    (candidate for candidate in ("iceberg", "parquet", "orc", "delta") if candidate in description_folded or candidate in metric_types),
                    None,
                )
                scan = {
                    "execution_id": execution_id,
                    "plan_node_id": node["node_id"],
                    "stage_ids": stage_ids,
                    "table_or_relation": _relation(node),
                    "format": detected_format,
                    "output_columns": None,
                    "scan_output_rows": _metric(metrics, "number of output rows"),
                    "scan_bytes": _metric(metrics, "total data file size (bytes)", "files size", "data size"),
                    "files_read": _metric(metrics, "number of result data files", "number of file splits read", "files read"),
                    "result_data_files": result_data_files,
                    "skipped_data_files": skipped_data_files,
                    "considered_data_files": considered_data_files,
                    "file_pruning_pct": file_pruning_pct,
                    "files_size": _metric(metrics, "total data file size (bytes)", "files size"),
                    "partitions_read": _metric(metrics, "number of partitions", "partitions read"),
                    "scan_time_ms": _metric(metrics, "scan time", "total planning duration (ms)"),
                    "metadata_time_ms": _metric(metrics, "metadata time"),
                    "partition_filters": partition_filters,
                    "data_filters": data_filters,
                    "pushed_filters": pushed_filters,
                    "scan_filters": scan_filters,
                    "partition_filters_present": bool(partition_filters),
                    "data_filters_present": bool(data_filters),
                    "pushed_filters_present": bool(pushed_filters),
                    "scan_filters_present": bool(scan_filters),
                    "partitioned_source": partitioned,
                    "dynamic_partition_pruning_detected": dynamic,
                    "partition_pruning_status": pruning_status,
                    "pruning_evidence": pruning_evidence,
                }
                scans.append(scan)
                all_scans.append(scan)

            if "window" in folded:
                operator_counts["Window"] += 1
                children = node["children"]
                input_rows = _nearest_metric(by_id, children[0], ("number of output rows",)) if children else None
                sort_node = _nearest_node(by_id, children[0], ("Sort",)) if children else None
                exchange_node = _nearest_node(by_id, children[0], ("Exchange",)) if children else None
                window = {
                    "execution_id": execution_id,
                    "plan_node_id": node["node_id"],
                    "stage_ids": stage_ids,
                    "window_output_rows": _metric(metrics, "number of output rows"),
                    "window_input_rows": input_rows,
                    "requires_shuffle_and_sort": bool(sort_node and exchange_node),
                    "upstream_shuffle_bytes": (
                        _metric(exchange_node["metrics"], "shuffle bytes written", "data size") if exchange_node else None
                    ),
                }
                windows.append(window)
                all_windows.append(window)
            if name == "Sort" or folded.startswith("sort "):
                operator_counts["Sort"] += 1
                children = node["children"]
                sort_item = {
                    "execution_id": execution_id,
                    "plan_node_id": node["node_id"],
                    "stage_ids": stage_ids,
                    "input_rows": _nearest_metric(by_id, children[0], ("number of output rows",)) if children else None,
                    "output_rows": _metric(metrics, "number of output rows"),
                    "peak_memory_bytes": _metric(metrics, "peak memory"),
                    "spill_bytes": _metric(metrics, "spill size"),
                    "global_sort": "true" in str(node["operator_description"]).casefold().split(",")[-2:-1],
                }
                sorts.append(sort_item)
                all_sorts.append(sort_item)
            if "exchange" in folded:
                operator_counts["Exchange"] += 1
                exchange = {
                    "execution_id": execution_id,
                    "plan_node_id": node["node_id"],
                    "stage_ids": stage_ids,
                    "shuffle_bytes_written": _metric(metrics, "shuffle bytes written", "data size"),
                    "shuffle_records_written": _metric(metrics, "shuffle records written"),
                    "single_partition": "singlepartition" in str(node["operator_description"]).casefold(),
                }
                exchanges.append(exchange)
                all_exchanges.append(exchange)
            for operator in ("HashAggregate", "SortAggregate", "Expand", "Generate"):
                if operator.casefold() in folded:
                    operator_counts[operator] += 1
                    if operator in {"Expand", "Generate"}:
                        children = node["children"]
                        input_rows = _nearest_metric(by_id, children[0], ("number of output rows",)) if children else None
                        output_rows = _metric(metrics, "number of output rows")
                        amplification = output_rows / input_rows if output_rows is not None and input_rows else None
                        row_operator = {
                            "execution_id": execution_id,
                            "plan_node_id": node["node_id"],
                            "stage_ids": stage_ids,
                            "operator_name": operator,
                            "input_rows": input_rows,
                            "output_rows": output_rows,
                            "row_amplification": amplification,
                        }
                        row_expansion_operators.append(row_operator)
                        all_row_expansion_operators.append(row_operator)

        execution_record = {
            "execution_id": execution_id,
            "root_execution_id": execution.root_execution_id,
            "description": execution.description,
            "start_time": execution.start_time,
            "end_time": execution.end_time,
            "duration_ms": (
                execution.end_time - execution.start_time
                if execution.start_time is not None and execution.end_time is not None
                else None
            ),
            "status": execution.status,
            "physical_plan": execution.physical_plan,
            "final_physical_plan": execution.final_physical_plan or execution.physical_plan,
            "aqe_updated": execution.aqe_updated,
            "job_ids": sorted(execution.job_ids),
            "stage_ids": stage_ids,
            "plan_nodes": nodes,
            "joins": joins,
            "scans": scans,
            "windows": windows,
            "exchanges": exchanges,
            "sorts": sorts,
            "row_expansion_operators": row_expansion_operators,
        }
        executions.append(execution_record)

    return {
        "executions": executions,
        "joins": all_joins,
        "scans": all_scans,
        "windows": all_windows,
        "exchanges": all_exchanges,
        "sorts": all_sorts,
        "row_expansion_operators": all_row_expansion_operators,
        "operator_counts": operator_counts,
        "sql_metrics_available": sql_metric_found,
    }
