from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any


CONFIG_KEYS = (
    "spark.driver.cores",
    "spark.driver.memory",
    "spark.driver.memoryOverhead",
    "spark.driver.maxResultSize",
    "spark.kubernetes.driver.request.cores",
    "spark.kubernetes.driver.limit.cores",
    "spark.kubernetes.driver.memoryOverheadFactor",
    "spark.executor.cores",
    "spark.executor.memory",
    "spark.executor.memoryOverhead",
    "spark.executor.instances",
    "spark.kubernetes.executor.request.cores",
    "spark.kubernetes.executor.limit.cores",
    "spark.kubernetes.executor.memoryOverheadFactor",
    "spark.dynamicAllocation.enabled",
    "spark.dynamicAllocation.minExecutors",
    "spark.dynamicAllocation.initialExecutors",
    "spark.dynamicAllocation.maxExecutors",
    "spark.sql.shuffle.partitions",
    "spark.sql.adaptive.enabled",
    "spark.sql.adaptive.coalescePartitions.enabled",
    "spark.sql.adaptive.advisoryPartitionSizeInBytes",
    "spark.sql.adaptive.coalescePartitions.initialPartitionNum",
    "spark.sql.adaptive.skewJoin.enabled",
    "spark.sql.adaptive.skewJoin.skewedPartitionFactor",
    "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes",
    "spark.sql.autoBroadcastJoinThreshold",
    "spark.sql.adaptive.autoBroadcastJoinThreshold",
    "spark.sql.broadcastTimeout",
)


SPARK_DEFAULTS: dict[str, dict[str, Any]] = {
    "3.5": {
        "spark.dynamicAllocation.enabled": "false",
        "spark.sql.shuffle.partitions": "200",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.advisoryPartitionSizeInBytes": "64MB",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.sql.adaptive.skewJoin.skewedPartitionFactor": "5.0",
        "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "256MB",
        "spark.sql.autoBroadcastJoinThreshold": "10MB",
        "spark.sql.broadcastTimeout": "300",
    }
}


SENSITIVE_KEY = re.compile(
    r"(?i)(password|passwd|secret|token|credential|access[._-]?key|private[._-]?key|client[._-]?secret)"
)


def load_rules(path: str | Path | None = None) -> dict[str, Any]:
    source = Path(path) if path else Path(__file__).resolve().parent.parent / "config" / "rules.v1.json"
    return json.loads(source.read_text(encoding="utf-8"))


def redact_property(key: str, value: Any) -> Any:
    return "[REDACTED]" if SENSITIVE_KEY.search(key) else value


def configuration(properties: dict[str, str], spark_version: str | None) -> dict[str, Any]:
    family = ".".join((spark_version or "").split(".")[:2])
    defaults = SPARK_DEFAULTS.get(family, {})
    result: dict[str, Any] = {}
    for key in CONFIG_KEYS:
        configured = properties.get(key)
        effective = configured if configured is not None else defaults.get(key)
        result[key] = {
            "configured_value": redact_property(key, configured),
            "effective_value": redact_property(key, effective),
            "source": "configured" if configured is not None else ("spark_default" if key in defaults else "unknown"),
        }
    return result


def sanitized_properties(properties: dict[str, str]) -> dict[str, str]:
    return {key: redact_property(key, value) for key, value in sorted(properties.items())}


def parse_bytes(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value).strip().lower()
    match = re.fullmatch(r"([+-]?[0-9]+(?:\.[0-9]+)?)\s*([kmgtpe]?)(?:i?b)?", text)
    if not match:
        return None
    number = float(match.group(1))
    exponent = "kmgtpe".find(match.group(2)) + 1 if match.group(2) else 0
    return int(number * (1024**exponent))


def parse_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if value is None:
        return None
    text = str(value).strip().lower()
    if text in {"true", "1", "yes"}:
        return True
    if text in {"false", "0", "no"}:
        return False
    return None

